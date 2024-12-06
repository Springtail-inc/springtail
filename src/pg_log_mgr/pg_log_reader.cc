#include <cassert>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

#include <pg_repl/pg_msg_stream.hh>

namespace springtail::pg_log_mgr {

    void
    PgLogReader::Batch::commit()
    {
        // go through each subtxn and push it's outstanding batches to the WriteCache
        for (const auto &txn_entry : _txns) {
            for (const auto &entry : txn_entry.second.table_map) {
                WriteCacheFuncImpl::add_extent(_db, entry.first, txn_entry.first,
                                               entry.second.start_lsn, entry.second.extent);
            }
        }

        // XXX need some way to collect *all* of the valid subtxns together

        // XXX assign an XID to the committed transaction and update the mappings in the write cache
        uint64_t xid = get_xid();
        WriteCacheFuncImpl::commit(_db, _pg_xids, xid);

        // apply any schema changes to the SysTblMgr
        auto client = sys_tbl_mgr::Client::get_instance();
        for (auto &&change : _schema_changes) {
            nlohmann::json ddl_stmt;

            switch(change->msg_type) {
            case PgMsgEnum::CREATE_TABLE:
                {
                    auto &table_msg = std::get<PgMsgTable>(change->msg);
                    ddl_stmt = client->create_table(_db, xid, table_msg);
                    break;
                }
            case PgMsgEnum::ALTER_TABLE:
                {
                    auto &table_msg = std::get<PgMsgTable>(change->msg);
                    ddl_stmt = client->alter_table(_db, xid, table_msg);

                    // check for re-sync
                    nlohmann::json action = nlohmann::json::parse(ddl_stmt).at("action");
                    if (action.get<std::string>() == "resync") {
                        // XXX if a re-sync is required for a table, drop the changes for that table
                        //     from the write cache and initiate a re-sync
                        throw Error();
                    }
                    break;
                }
            case PgMsgEnum::DROP_TABLE:
                {
                    auto &drop_msg = std::get<PgMsgDropTable>(change->msg);
                    ddl_stmt = client->drop_table(_db, xid, drop_msg);
                    break;
                }
            case PgMsgEnum::CREATE_INDEX:
                {
                    auto &index_msg = std::get<PgMsgIndex>(change->msg);
                    ddl_stmt = client->create_index(_db, xid, index_msg);
                    break;
                }
            case PgMsgEnum::DROP_INDEX:
                {
                    auto &index_msg = std::get<PgMsgDropIndex>(change->msg);
                    ddl_stmt = client->drop_index(_db, xid, index_msg);
                    break;
                }
            }

            _redis_ddl.add_ddl(_db, xid, ddl_stmt);
        }

        // XXX message the Committer
    }

    void
    PgLogReader::Batch::abort()
    {
        // drop any batches for all active txns
        for (auto &&entry : _txns) {
            auto txn = entry.second;
            WriteCacheFuncImpl::abort(_db, txn->pg_xid);
        }
    }

    void
    PgLogReader::Batch::abort_subtxn(int32_t pg_xid)
    {
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
    PgLogReader::Batch::add_mutation(int32_t tid, int32_t pg_xid)
    {
        auto txn = _get_txn(pg_xid);

        // get the Extent containing mutations
        auto &entry = txn->table_map[tid];
        if (entry.extent == nullptr) {
            entry.extent = std::make_shared<Extent>(); // XXXXXX
            entry.start_lsn = _lsn;
        }

        // add the mutation to the batch
        auto &&row = entry.extent->append();
        MutableTuple(entry.fields, row).assign(FieldTuple(entry.pg_fields, &msg.new_tuple));
        _type_f->set_int8(row, Op::INSERT);
        _lsn_f->set_uint64(row, _lsn++);

        // if the batch has grown to the maximum size, pass it to the WriteCache
        if (entry.extent->byte_count() > MAX_BATCH_SIZE) {
            WriteCacheFuncImpl::add_extent(_db, tid, pg_xid, entry.start_lsn, entry.extent);
            entry.extent = nullptr;
        }
    }

    void
    PgLogReader::Batch::truncate(const PgMsgTruncate &msg)
    {
        // get the current txn
        auto txn = _get_txn(msg.xid);

        // note: there may be multiple truncates due to CASCADE
        for (auto tid : msg.rel_ids) {
            // if we sent any batches to the WriteCache, evict them
            WriteCacheFuncImpl::drop_table(_db, tid, _cur_pg_xid);

            // XXX if this is a subtxn, then once it's committed, we could also drop any earlier
            //     mutations to the table in it's parent pg xid

            // create a new extent that begins with a truncate operation
            auto &entry = txn->table_map[tid];
            entry.extent = std::make_shared<Extent>();
            entry.start_lsn = _lsn;

            // add a TRUNCATE row
            auto &&row = entry.extent->append();
            _type_f->set_int8(row, Op::TRUNCATE);
            _lsn_f->set_uint64(row, _lsn++);
        }
    }

    void
    PgLogReader::Batch::schema_change(int32_t tid, int32_t pg_xid, PgMsgPtr msg)
    {
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
            entry.schema = std::make_shared<ExtentSchema>(schema_change);
        } else if (msg->msg_type == PgMsgEnum::DROP_TABLE) {
            // XXX should we do a truncate here?  it could improve performance if this follows a set
            //     of mutations, but it's not clear we actually need it since the mutations would be
            //     dropped either way
        }

        // save the change to process once we see a commit
        entry.changes.emplace_back({ msg, _lsn++ });
    }

    TxnEntryPtr
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

    TxnEntryPtr
    PgLogReader::Batch::_start_subtxn(int32_t pg_xid)
    {
        // construct a new txn entry for this subtxn
        auto sub_txn = std::make_shared<TxnEntry>(pg_xid, _schema_changes.end());

        // send the current txn's extents to the write cache and pull the schema forward to the subtxn
        auto cur_txn = _txns.back();
        for (auto &entry : cur_txn->table_map) {
            auto &table = entry.second;

            // send the current extent to the WriteCache
            WriteCacheFuncImpl::add_extent(_db, entry.first, cur_txn->pg_xid,
                                           table.start_lsn, table.extent);
            table.extent = nullptr;

            // use the schema information from the current txn in the subtxn
            sub_txn->table_map.try_emplace(entry.first, { table.schema });
        }

        // record the subtransaction into the map
        _txns.push_back(sub_txn);

        // mark the current pgxid we are operating within
        _cur_pg_xid = pg_xid;

        return sub_txn;
    }

    void
    PgLogReader::process_log(const std::filesystem::path &path,
                             uint64_t start_offset,
                             int num_messages)
    {
        // init stream reader
        _reader.set_file(path, start_offset);

        std::vector<char> filter = { pg_msg::MSG_BEGIN,
            pg_msg::MSG_COMMIT,
            pg_msg::MSG_STREAM_START,
            pg_msg::MSG_STREAM_COMMIT,
            pg_msg::MSG_STREAM_ABORT,
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
                case PgMsgEnum::DELETE:
                case PgMsgEnum::UPDATE:
                    _process_mutation(msg);
                    break;

                case PgMsgEnum::TRUNCATE:
                    _process_truncate(std::get<PgMsgTruncate>(msg->msg));
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
                    SPDLOG_WARN("Unknown message type\n");
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

        // XXX prepare a batch for processing
        _current_batch = std::make_shared<Batch>();
        _batch_map.try_emplace({ begin_msg.xid, _current_batch });
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

        _queue->push(xact);
        _current_xact = nullptr;

        // update the write cache and system tables as needed
        _current_batch->commit();
        _batch_map.erase(commit_msg.xid);
        _current_batch = nullptr;
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

        // XXX prepare a batch for processing
        _current_batch = std::make_shared<Batch>();
        _batch_map.try_emplace({ start_msg.xid, _current_batch });
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

        _xact_map.erase(itr);
        _queue->push(xact);

        // commit the current batch
        _current_batch->commit();
        _batch_map.erase(commit_msg.xid);
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
            _current_batch->abort(abort_msg.xid);
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
    PgLogReader::_process_mutation(int32_t tid,
                                   const PgMsgTupleData &msg)
    {
        _current_batch->add_mutation(tid, msg);
    }

    void
    PgLogReader::_process_truncate(const PgMsgTruncate &msg)
    {
        _current_batch->truncate(msg);
    }

    void
    PgLogReader::_process_ddl(uint32_t oid, int32_t xid, bool is_streaming, PgMsgPtr msg)
    {
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
