#include <cassert>

#include <common/logging.hh>
#include <common/tracing.hh>

#include <garbage_collector/xid_ready.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

#include <pg_repl/pg_msg_stream.hh>

#include <storage/xid.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>

#include <write_cache/write_cache_func.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogReader::Batch::commit(uint64_t xid)
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // go through each subtxn and push it's outstanding batches to the WriteCache
        std::vector<uint64_t> pg_xids;
        std::map<uint64_t, ChangeListPtr> change_map;
        for (const auto &txn_entry : _txns) {
            // XXX we need to sort the data by primary key if we want to perform efficient hurry-ups

            for (const auto &entry : txn_entry.second->table_map) {
                if (entry.second.extent == nullptr) {
                    continue;
                }
                WriteCacheFuncImpl::add_extent(_db, entry.first, txn_entry.first,
                                               entry.second.start_lsn, entry.second.extent);
            }

            // collect the pgxids for the write cache commit
            pg_xids.push_back(txn_entry.first);

            // place the schema changes into a map ordered by the starting LSN
            if (txn_entry.second->changes != nullptr) {
                change_map.try_emplace(txn_entry.second->changes->front().second,
                                       txn_entry.second->changes);
            }
        }

        // apply any schema changes
        if (!change_map.empty()) {
            _apply_schema_changes(change_map, xid);
        }

        // assign an XID to the committed transaction and update the mappings in the write cache
        WriteCacheFuncImpl::commit(_db, xid, pg_xids);

        // stop timing for this transaction
        _span->SetAttribute("xid", static_cast<int64_t>(xid));
        _span->End();
    }

    void
    PgLogReader::Batch::abort()
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // drop any batches for all active txns
        for (auto &&entry : _txns) {
            auto txn = entry.second;
            WriteCacheFuncImpl::abort(_db, txn->pg_xid);
        }

        // stop timing for this transaction
        _span->End();
    }

    void
    PgLogReader::Batch::abort_subtxn(int32_t pg_xid)
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // find the txn to abort it
        auto itr = _txns.find(pg_xid);
        assert(itr != _txns.end());

        // abort any batches associated with this txn sent to the WriteCache
        WriteCacheFuncImpl::abort(_db, itr->second->pg_xid);

        // note: the schema changes will be lost when this txn is removed from the active set
        _txns.erase(itr);

        // note: we don't update the current pgxid since then we will have a guaranteed mis-match on
        //       the next operation and we can handle it accordingly
    }

    void
    PgLogReader::Batch::TableEntry::update_schema()
    {
        auto sort_keys = table_schema->get_sort_keys();
        bool has_primary = !sort_keys.empty();
        sort_keys.push_back("__springtail_lsn");

        auto columns = table_schema->column_order();

        SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
        SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);

        std::vector<SchemaColumn> new_columns{op, lsn};

        schema = table_schema->create_schema(columns, new_columns, sort_keys);

        fields = schema->get_mutable_fields(columns);
        if (has_primary) {
            pkey_fields = schema->get_mutable_fields(table_schema->get_sort_keys());
        } else {
            pkey_fields = fields;
        }
        op_f = schema->get_mutable_field("__springtail_op");
        lsn_f = schema->get_mutable_field("__springtail_lsn");

        pg_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            pg_fields->push_back(std::make_shared<PgLogField>(fields->at(i)->get_type(), i));
        }

        pg_pkey_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < pkey_fields->size(); i++) {
            pg_pkey_fields->push_back(std::make_shared<PgLogField>(pkey_fields->at(i)->get_type(), i));
        }
    }

    template <int T>
    void
    PgLogReader::Batch::add_mutation(uint64_t current_xid,
                                     int32_t pg_xid,
                                     int32_t tid,
                                     const PgMsgTupleData &data)
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        auto txn = _get_txn(pg_xid);

        // get the Extent containing mutations
        auto &entry = txn->table_map[tid];
        if (entry.extent == nullptr) {
            if (entry.schema == nullptr) {
                XidLsn current(current_xid);
                entry.table_schema = SchemaMgr::get_instance()->get_extent_schema(_db, tid, current);
                entry.update_schema();
            }

            entry.extent = std::make_shared<Extent>(ExtentType{}, 0, entry.schema->row_size());
            entry.start_lsn = _lsn;
        }
        
        // add the mutation to the batch
        auto &&row = entry.extent->append();
        if constexpr (T == PgMsgEnum::INSERT || T == PgMsgEnum::UPDATE) {
            MutableTuple(entry.fields, row).assign(FieldTuple(entry.pg_fields, &data));
        } else if constexpr (T == PgMsgEnum::DELETE) {
            MutableTuple(entry.pkey_fields, row).assign(FieldTuple(entry.pg_pkey_fields, &data));
        } else {
            static_assert(false, "Invalid template parameter: PgLogReader::Batch::add_mutation");
        }
        entry.op_f->set_int8(row, T);
        entry.lsn_f->set_uint64(row, _lsn++);

        // XXX we need some way to limit the total memory used by a batch across all extents

        // if the batch has grown to the maximum size, pass it to the WriteCache
        if (entry.extent->byte_count() > MAX_BATCH_SIZE) {
            WriteCacheFuncImpl::add_extent(_db, tid, pg_xid, entry.start_lsn, entry.extent);
            entry.extent = nullptr;
        }
    }

    void
    PgLogReader::Batch::truncate(uint64_t current_xid,
                                 const PgMsgTruncate &msg)
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // get the current txn
        auto txn = _get_txn(msg.xid);

        // note: there may be multiple truncates due to CASCADE
        for (auto tid : msg.rel_ids) {
            // if we sent any batches to the WriteCache, evict them
            WriteCacheFuncImpl::drop_table(_db, tid, _cur_pg_xid);

            // XXX if this is a subtxn, then once it's committed, we could also drop any earlier
            //     mutations to the table in it's parent pg xid, but it's not strictly necessary

            // create a new extent that begins with a truncate operation
            auto &entry = txn->table_map[tid];
            if (entry.schema == nullptr) {
                XidLsn current(current_xid);
                entry.table_schema = SchemaMgr::get_instance()->get_extent_schema(_db, tid, current);
                entry.update_schema();
            }

            entry.extent = std::make_shared<Extent>(ExtentType{}, 0, entry.schema->row_size());
            entry.start_lsn = _lsn;

            // add a TRUNCATE row
            auto &&row = entry.extent->append();
            entry.op_f->set_int8(row, PgMsgEnum::TRUNCATE);
            entry.lsn_f->set_uint64(row, _lsn++);
        }
    }

    void
    PgLogReader::Batch::schema_change(int32_t tid, int32_t pg_xid, PgMsgPtr msg)
    {
        tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // get the table entry
        auto txn = _get_txn(pg_xid);
        auto &entry = txn->table_map[tid];

        // push any data mutations into the write cache since the schema is going to change
        if (entry.extent) {
            WriteCacheFuncImpl::add_extent(_db, tid, pg_xid,
                                           entry.start_lsn, entry.extent);
            entry.extent = nullptr;
        }

        // for table creation / mutation create a new schema
        if (msg->msg_type == PgMsgEnum::CREATE_TABLE || msg->msg_type == PgMsgEnum::ALTER_TABLE) {
            auto &table = std::get<PgMsgTable>(msg->msg);
            std::vector<SchemaColumn> columns;
            for (auto column : table.columns) {
                columns.emplace_back(SchemaColumn{
                        column.column_name,
                        column.position,
                        static_cast<SchemaType>(column.type),
                        column.pg_type,
                        column.is_nullable
                    });
            }

            entry.table_schema = std::make_shared<ExtentSchema>(columns);
            entry.update_schema();
        } else if (msg->msg_type == PgMsgEnum::DROP_TABLE) {
            // XXX should we do a truncate here?  it could improve performance if this follows a set
            //     of mutations, but it's not clear we actually need it since the mutations would be
            //     dropped either way
        }

        // save the change to process once we see a commit
        if (txn->changes == nullptr) {
            txn->changes = std::make_shared<ChangeList>();
        }
        txn->changes->emplace_back(ChangeEntry{ msg, _lsn++ });
    }

    PgLogReader::Batch::TxnEntryPtr
    PgLogReader::Batch::_get_txn(int32_t pg_xid)
    {
        // check if a new subtxn started
        if (pg_xid == _cur_pg_xid) {
            return _cur_txn;
        }

        // see if we already know about this pg_xid
        auto itr = _txns.find(pg_xid);
        if (itr == _txns.end()) {
            return _start_subtxn(pg_xid);
        }

        return itr->second;
    }

    PgLogReader::Batch::TxnEntryPtr
    PgLogReader::Batch::_start_subtxn(int32_t pg_xid)
    {
        // construct a new txn entry for this subtxn
        auto sub_txn = std::make_shared<TxnEntry>(pg_xid);

        // send the current txn's extents to the write cache and pull the schema forward to the subtxn
        if (_cur_txn != nullptr) {
            for (auto &entry : _cur_txn->table_map) {
                auto &table = entry.second;

                // send the current extent to the WriteCache
                WriteCacheFuncImpl::add_extent(_db, entry.first, _cur_txn->pg_xid,
                                               table.start_lsn, table.extent);
                table.extent = nullptr;

                // use the schema information from the current txn in the subtxn
                auto &&p = sub_txn->table_map.try_emplace(entry.first, TableEntry{ table.table_schema });
                p.first->second.update_schema();
            }
        }

        // record the subtransaction into the map
        _txns.try_emplace(pg_xid, sub_txn);

        // mark the current pgxid we are operating within
        _cur_pg_xid = pg_xid;
        _cur_txn = sub_txn;

        return sub_txn;
    }

    void
    PgLogReader::Batch::_apply_schema_changes(const LsnChangeMap &change_map,
                                              uint64_t xid)
    {
        RedisDDL redis_ddl;

        // apply any schema changes in LSN order to the SysTblMgr
        auto next_i = change_map.begin();
        auto change_i = next_i++;
        while (change_i != change_map.end()) {
            // check if a subtxn has interleaved schema changes
            if (next_i != change_map.end()) {
                uint64_t cur_lsn = change_i->second->front().second;
                uint64_t next_lsn = next_i->second->front().second;
                if (cur_lsn > next_lsn) {
                    ++change_i;
                    ++next_i;
                    continue;
                }
            }

            // apply the schema change
            auto client = sys_tbl_mgr::Client::get_instance();
            auto change = change_i->second->front().first;
            XidLsn xidlsn(xid, change_i->second->front().second);

            std::string ddl_stmt;
            switch(change->msg_type) {
            case PgMsgEnum::CREATE_TABLE:
                {
                    auto &table_msg = std::get<PgMsgTable>(change->msg);
                    ddl_stmt = client->create_table(_db, xidlsn, table_msg);
                    redis_ddl.add_ddl(_db, xid, ddl_stmt);
                    break;
                }
            case PgMsgEnum::ALTER_TABLE:
                {
                    auto &table_msg = std::get<PgMsgTable>(change->msg);
                    ddl_stmt = client->alter_table(_db, xidlsn, table_msg);

                    // check for re-sync
                    nlohmann::json action = nlohmann::json::parse(ddl_stmt).at("action");
                    if (action.get<std::string>() == "resync") {
                        // XXXXXX if a re-sync is required for a table, drop the changes for that table
                        //        from the write cache and initiate a re-sync
                        SPDLOG_ERROR("Still need to implement resync");
                        throw Error();
                    } else if (action.get<std::string>() != "no_change") {
                        redis_ddl.add_ddl(_db, xid, ddl_stmt);
                    }
                    break;
                }
            case PgMsgEnum::DROP_TABLE:
                {
                    auto &drop_msg = std::get<PgMsgDropTable>(change->msg);
                    ddl_stmt = client->drop_table(_db, xidlsn, drop_msg);
                    redis_ddl.add_ddl(_db, xid, ddl_stmt);
                    break;
                }
            case PgMsgEnum::CREATE_INDEX:
                {
                    auto &index_msg = std::get<PgMsgIndex>(change->msg);
                    ddl_stmt = client->create_index(_db, xidlsn, index_msg);
                    break;
                }
            case PgMsgEnum::DROP_INDEX:
                {
                    auto &index_msg = std::get<PgMsgDropIndex>(change->msg);
                    ddl_stmt = client->drop_index(_db, xidlsn, index_msg);
                    break;
                }

            default:
                SPDLOG_ERROR("Message type {} not handled", static_cast<uint8_t>(change->msg_type));
                throw Error();
            }

            // remove the schema change we just applied
            change_i->second->pop_front();

            // check if the list of changes is empty for this txn
            if (change_i->second->empty()) {
                ++change_i;
                if (next_i != change_map.end()) {
                    ++next_i;
                }
            }
        }
    }

    void
    PgLogReader::process_log(const std::filesystem::path &path,
                             uint64_t start_offset,
                             int num_messages)
    {
        // init stream reader
        _reader.set_file(path, start_offset);

        std::vector<char> filter = {
            pg_msg::MSG_BEGIN,
            pg_msg::MSG_COMMIT,
            pg_msg::MSG_STREAM_START,
            pg_msg::MSG_STREAM_COMMIT,
            pg_msg::MSG_STREAM_ABORT,
            pg_msg::MSG_INSERT,
            pg_msg::MSG_UPDATE,
            pg_msg::MSG_DELETE,
            pg_msg::MSG_TRUNCATE,
            pg_msg::MSG_MESSAGE // this will capture create_table, drop_table, alter_table, create_index, drop_index
        };

        _current_path = path;

        // consume messages from log; num_messages of -1 means go until eos
        bool eos = false; // end of stream
        while (num_messages != 0 && !eos) {
            bool eob=false; // end of block

            // while not at end of message block (or stream) process
            while (!eob && !eos) {
                // read next message
                PgMsgPtr msg = _reader.read_message(filter, eos, eob);
                if (msg == nullptr) {
                    continue;
                }

                // handle the message
                switch(msg->msg_type) {
                case PgMsgEnum::BEGIN:
                    _process_begin(std::get<PgMsgBegin>(msg->msg));
                    break;

                case PgMsgEnum::COMMIT:
                    _process_commit(std::get<PgMsgCommit>(msg->msg));
                    break;

                case PgMsgEnum::STREAM_START:
                    _process_stream_start(std::get<PgMsgStreamStart>(msg->msg));
                    break;

                case PgMsgEnum::STREAM_COMMIT:
                    _process_stream_commit(std::get<PgMsgStreamCommit>(msg->msg));
                    break;

                case PgMsgEnum::STREAM_ABORT:
                    _process_stream_abort(std::get<PgMsgStreamAbort>(msg->msg));
                    break;

                case PgMsgEnum::INSERT:
                    {
                        auto &insert = std::get<PgMsgInsert>(msg->msg);
                        int32_t pg_xid = (msg->is_streaming) ? insert.xid : _current_xact->xid;
                        _current_batch->add_mutation<PgMsgEnum::INSERT>(this->get_current_xid(), pg_xid,
                                                                        insert.rel_id, insert.new_tuple);
                        break;
                    }
                case PgMsgEnum::DELETE:
                    {
                        auto &remove = std::get<PgMsgDelete>(msg->msg);
                        int32_t pg_xid = (msg->is_streaming) ? remove.xid : _current_xact->xid;
                        _current_batch->add_mutation<PgMsgEnum::DELETE>(this->get_current_xid(), pg_xid,
                                                                        remove.rel_id, remove.tuple);
                        break;
                    }
                case PgMsgEnum::UPDATE:
                    {
                        auto &update = std::get<PgMsgUpdate>(msg->msg);
                        int32_t pg_xid = (msg->is_streaming) ? update.xid : _current_xact->xid;
                        if (update.old_type == 0) {
                            _current_batch->add_mutation<PgMsgEnum::UPDATE>(this->get_current_xid(), pg_xid,
                                                                            update.rel_id, update.new_tuple);
                        } else {
                            _current_batch->add_mutation<PgMsgEnum::DELETE>(this->get_current_xid(), pg_xid,
                                                                            update.rel_id, update.old_tuple);
                            _current_batch->add_mutation<PgMsgEnum::INSERT>(this->get_current_xid(), pg_xid,
                                                                            update.rel_id, update.new_tuple);
                        }
                        break;
                    }
                    break;

                case PgMsgEnum::TRUNCATE:
                    _current_batch->truncate(this->get_current_xid(),
                                             std::get<PgMsgTruncate>(msg->msg));
                    break;

                case PgMsgEnum::CREATE_TABLE:
                case PgMsgEnum::ALTER_TABLE:
                    {
                        PgMsgTable &table_msg = std::get<PgMsgTable>(msg->msg);
                        _process_ddl(table_msg.oid, table_msg.xid, msg->is_streaming, msg);
                        break;
                    }
                case PgMsgEnum::DROP_TABLE:
                    {
                        PgMsgDropTable &drop_msg = std::get<PgMsgDropTable>(msg->msg);
                        _process_ddl(drop_msg.oid, drop_msg.xid, msg->is_streaming, msg);
                        break;
                    }
                case PgMsgEnum::CREATE_INDEX:
                    {
                        const auto &index_msg = std::get<PgMsgIndex>(msg->msg);
                        _process_ddl(index_msg.oid, index_msg.xid, msg->is_streaming, msg);
                        break;
                    }
                case PgMsgEnum::DROP_INDEX:
                    {
                        const auto &index_msg = std::get<PgMsgDropIndex>(msg->msg);
                        _process_ddl(index_msg.oid, index_msg.xid, msg->is_streaming, msg);
                        break;
                    }
                default:
                    SPDLOG_WARN("Unknown message type: {}", static_cast<uint8_t>(msg->msg_type));
                    break;
                }
            }

            if (num_messages > 0) {
                num_messages--;
            }
        }
    }

    void
    PgLogReader::_process_begin(const PgMsgBegin &begin_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Begin: xid={}, xact_lsn={}\n", begin_msg.xid, begin_msg.xact_lsn);

        PgTransactionPtr xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_COMMIT);
        xact->begin_path = _current_path;
        xact->begin_offset = _reader.header_offset();
        xact->xact_lsn = begin_msg.xact_lsn;
        xact->xid = begin_msg.xid;
        _current_xact = xact;

        // prepare a batch for processing
        _current_batch = std::make_shared<Batch>(_db_id, begin_msg.xid);
        _batch_map.try_emplace(begin_msg.xid, _current_batch);
    }

    void
    PgLogReader::_process_commit(const PgMsgCommit &commit_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Commit: commit_lsn={}, xact_lsn={}\n", commit_msg.commit_lsn, commit_msg.xact_lsn);

        PgTransactionPtr xact = _current_xact;
        if (_current_xact == nullptr || commit_msg.commit_lsn != _current_xact->xact_lsn) {
            // we don't have the start of the transaction...
            SPDLOG_WARN("No matching xact for commit: commit_lsn={}\n", commit_msg.commit_lsn);
            return;
        }

        assert (xact->type == PgTransaction::TYPE_COMMIT);

        // set transaction path and end offset
        xact->commit_path = _current_path;
        xact->commit_offset = _reader.offset();

        // update the write cache and system tables as needed
        uint64_t xid = this->get_next_xid();
        _current_batch->commit(xid);
        _batch_map.erase(xact->xid);
        _current_batch = nullptr;

        // message the Committer

        /** Queue for XID messages to the Committer. */
        RedisQueue<gc::XidReady> committer_queue(fmt::format(redis::QUEUE_GC_XID_READY,
                                                             Properties::get_db_instance_id()));
        committer_queue.push(gc::XidReady(_db_id, gc::XidReady::XactMsg(xid)));

        // pass the xact to the xact logging thread
        xact->springtail_xid = xid;
        _queue->push(xact);
        _current_xact = nullptr;
    }

    void
    PgLogReader::_process_stream_start(const PgMsgStreamStart &start_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Stream start: xid={}, first={}\n", start_msg.xid, start_msg.first);

        if (!start_msg.first) {
            auto itr = _batch_map.find(start_msg.xid);
            assert(itr != _batch_map.end());

            _current_batch = itr->second;
            return;
        }

        // new transaction
        PgTransactionPtr xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_COMMIT);
        xact->begin_path = _current_path;
        xact->begin_offset = _reader.header_offset();
        xact->xid = start_msg.xid;
        _xact_map.insert({xact->xid, xact});

        PgTransactionPtr stream_xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_STREAM_START);
        stream_xact->begin_path = _current_path;
        stream_xact->begin_offset = _reader.header_offset();
        stream_xact->xid = start_msg.xid;

        _queue->push(stream_xact);

        // prepare a batch for processing
        _current_batch = std::make_shared<Batch>(_db_id, start_msg.xid);
        _batch_map.try_emplace(start_msg.xid, _current_batch);
    }

    void
    PgLogReader::_process_stream_commit(const PgMsgStreamCommit &commit_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Stream commit: xid={}, xact_lsn={}\n", commit_msg.xid, commit_msg.xact_lsn);

        // commit only happens for the top level xid, subxacts under the xid
        // automatically commit unless they were previously aborted
        auto itr = _xact_map.find(commit_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("No matching xact for stream commit: xid={}, xact_lsn={}",
                        commit_msg.xid, commit_msg.xact_lsn);
            return;
        }

        PgTransactionPtr xact = itr->second;
        xact->commit_path = _current_path;
        xact->commit_offset = _reader.offset();
        xact->xact_lsn = commit_msg.xact_lsn;

        assert (xact->type == PgTransaction::TYPE_COMMIT);

        // commit the current batch
        uint64_t xid = this->get_next_xid();
        _current_batch->commit(xid);
        _batch_map.erase(commit_msg.xid);

        // message the Committer
        RedisQueue<gc::XidReady> committer_queue(fmt::format(redis::QUEUE_GC_XID_READY,
                                                             Properties::get_db_instance_id()));
        committer_queue.push(gc::XidReady(_db_id, gc::XidReady::XactMsg(xid)));

        // pass the xact to the xact logging thread
        xact->springtail_xid = xid;
        _xact_map.erase(itr);
        _queue->push(xact);
    }

    void
    PgLogReader::_process_stream_abort(const PgMsgStreamAbort &abort_msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Stream abort: xid={}, sub_xid={}\n", abort_msg.xid, abort_msg.sub_xid);

        auto itr = _xact_map.find(abort_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("No matching xact for stream abort: xid={}, xact_lsn={}",
                        abort_msg.xid, abort_msg.abort_lsn);
            return;
        }

        if (abort_msg.sub_xid == abort_msg.xid) {
            // if sub_xid == xid, then it's a top level xact that aborted
            // add it to the xact queue for logging
            PgTransactionPtr xact = itr->second;
            xact->type = PgTransaction::TYPE_STREAM_ABORT;
            _queue->push(xact);
            // remove it from the map
            _xact_map.erase(itr);

            // abort the txn
            _current_batch->abort();
            _batch_map.erase(abort_msg.xid);
        } else {
            // subtransaction aborted, add to parent xact aborted list
            itr->second->aborted_xids.insert(abort_msg.sub_xid);

            // abort the subtxn
            _current_batch->abort_subtxn(abort_msg.sub_xid);
            _batch_map.erase(abort_msg.sub_xid);
        }

        _current_batch = nullptr;
    }

    void
    PgLogReader::_process_ddl(uint32_t oid, int32_t xid, bool is_streaming, PgMsgPtr msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Process DDL: oid={} pg_xid={}\n", oid, xid);

        // record the schema change into the batch
        _current_batch->schema_change(oid, xid, msg);

        if (!is_streaming) {
            // not streaming, so xid is invalid; use current_xact
            _current_xact->oids.insert(oid);
            return;
        }

        // if streaming, then xid is valid
        auto itr = _xact_map.find(xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            SPDLOG_WARN("XID not found for message: xid={}\n", xid);
            return;
        }

        PgTransactionPtr xact = itr->second;
        xact->oids.insert(oid);
    }
} // namespace springtail::pg_log_mgr
