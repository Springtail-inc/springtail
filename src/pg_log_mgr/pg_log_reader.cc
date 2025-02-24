#include <cassert>

#include <common/logging.hh>
#include <common/tracing.hh>
#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/sync_tracker.hh>
#include <pg_repl/pg_msg_stream.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_types.hh>
#include <storage/xid.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <write_cache/write_cache_func.hh>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

namespace springtail::pg_log_mgr {

    PgLogReader::PgLogReader(uint64_t db_id, const PgTransactionQueuePtr queue)
        : _db_id(db_id),
          _queue(queue)
    {
        auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter("pg_log_mgr");
        _postgres_log_reader_latencies = std::shared_ptr<opentelemetry::metrics::Histogram<double>>(
            meter
                ->CreateDoubleHistogram("postgres_log_reader_latencies",
                                        "Latency between when Postgres committed the transaction "
                                        "and when we process it in the log reader",
                                        "ms")
                .release());
    }

    void
    PgLogReader::Batch::commit(uint64_t xid, PostgresTimestamp commit_ts)
    {
        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);
        _span->AddEvent("commit", {{"pg_commit_time", commit_ts.to_unix_ns()}});

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
        WriteCacheFuncImpl::commit(_db, xid, pg_xids, commit_ts);

        // stop timing for this transaction
        if (_span->IsRecording()) {
            _span->SetAttribute("xid", static_cast<int64_t>(xid));
            _span->SetAttribute("pg_xids", fmt::format("{}", fmt::join(pg_xids, ",")));
        }
        _span->End();
    }

    void
    PgLogReader::Batch::abort(PostgresTimestamp abort_ts)
    {
        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);
        _span->AddEvent("aborted", {{"pg_abort_time", abort_ts.to_unix_ns()}});

        // drop any batches for all active txns
        for (auto &&entry : _txns) {
            auto txn = entry.second;
            WriteCacheFuncImpl::abort(_db, txn->pg_xid);
        }

        // stop timing for this transaction
        _span->End();
    }

    void
    PgLogReader::Batch::abort_subtxn(int32_t pg_xid, PostgresTimestamp abort_ts)
    {
        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // Add subtransaction abort event with the provided timestamp
        _span->AddEvent("subtransaction_abort", {
            {"sub_xid", pg_xid},
            {"pg_abort_time", abort_ts.to_unix_ns()}
        });

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
        // check if we should skip the mutation due to ongoing table sync
        if (SyncTracker::get_instance()->should_skip(_db, tid, _pg_xid)) {
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Skip mutation: oid={} pg_xid={}\n", tid, pg_xid);
            return;
        }

        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);
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
        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);

        // get the current txn
        auto txn = _get_txn(msg.xid);

        // note: there may be multiple truncates due to CASCADE
        for (auto tid : msg.rel_ids) {
            // check if we should skip this table due to ongoing table sync
            if (SyncTracker::get_instance()->should_skip(_db, tid, _pg_xid)) {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Skip truncate: oid={} pg_xid={}\n", tid, _pg_xid);
                continue;
            }

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
        auto scope = tracing::tracer("PgLogReader")->WithActiveSpan(_span);

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
            auto change = change_i->second->front().first;
            XidLsn xidlsn(xid, change_i->second->front().second);

            _apply_schema_change(change, xidlsn);

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
    PgLogReader::Batch::_apply_schema_change(PgMsgPtr change,
                                             const XidLsn &xidlsn)
    {
        RedisDDL redis_ddl;
        auto client = sys_tbl_mgr::Client::get_instance();

        switch(change->msg_type) {
        case PgMsgEnum::CREATE_TABLE:
            {
                auto &table_msg = std::get<PgMsgTable>(change->msg);
                std::string &&ddl_stmt = client->create_table(_db, xidlsn, table_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::ALTER_TABLE:
            {
                auto &table_msg = std::get<PgMsgTable>(change->msg);
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "ALTER TABLE: xid={}, pg_xid={}, tid={}",
                                    xidlsn.xid, table_msg.xid, table_msg.oid);

                std::string &&ddl_stmt = client->alter_table(_db, xidlsn, table_msg);

                // check for re-sync
                nlohmann::json action = nlohmann::json::parse(ddl_stmt).at("action");
                if (action.get<std::string>() == "resync") {
                    // mark the table as syncing to ensure we properly skip messages
                    bool is_first = SyncTracker::get_instance()->mark_resync(_db, table_msg.oid);

                    // notify the PgLogParser to resync the table
                    auto key = fmt::format(redis::QUEUE_SYNC_TABLES,
                                           Properties::get_db_instance_id(), _db);
                    RedisQueue<std::string> table_sync_queue(key);
                    table_sync_queue.push(std::to_string(table_msg.oid));

                    // notify the Committer to stop committing XIDs
                    if (is_first) {
                        _committer_queue.push(std::make_shared<XidReady>(_db));
                    }
                } else if (action.get<std::string>() != "no_change") {
                    redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                }
                break;
            }
        case PgMsgEnum::DROP_TABLE:
            {
                auto &drop_msg = std::get<PgMsgDropTable>(change->msg);
                std::string &&ddl_stmt = client->drop_table(_db, xidlsn, drop_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::CREATE_NAMESPACE:
            {
                auto &namespace_msg = std::get<PgMsgNamespace>(change->msg);
                std::string &&ddl_stmt = client->create_namespace(_db, xidlsn, namespace_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::ALTER_NAMESPACE:
            {
                auto &namespace_msg = std::get<PgMsgNamespace>(change->msg);
                std::string &&ddl_stmt = client->alter_namespace(_db, xidlsn, namespace_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::DROP_NAMESPACE:
            {
                auto &namespace_msg = std::get<PgMsgNamespace>(change->msg);
                std::string &&ddl_stmt = client->drop_namespace(_db, xidlsn, namespace_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::CREATE_INDEX:
            {
                auto &index_msg = std::get<PgMsgIndex>(change->msg);
                std::string &&ddl_stmt = client->create_index(_db, xidlsn, index_msg,
                                                              sys_tbl::IndexNames::State::READY);

                // Store the DDL statement for the Committer
                redis_ddl.add_index_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::DROP_INDEX:
            {
                auto &index_msg = std::get<PgMsgDropIndex>(change->msg);
                std::string &&ddl_stmt = client->drop_index(_db, xidlsn, index_msg);

                // Store the DDL statement for the Committer
                redis_ddl.add_index_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }

        default:
            SPDLOG_ERROR("Message type {} not handled", static_cast<uint8_t>(change->msg_type));
            throw Error();
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

                // process the message
                _process_msg(msg);
            }

            if (num_messages > 0) {
                num_messages--;
            }
        }
    }

    void
    PgLogReader::_process_msg(PgMsgPtr msg)
    {
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
            {
                _current_batch->truncate(this->get_current_xid(),
                                         std::get<PgMsgTruncate>(msg->msg));
                break;
            }
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
        case PgMsgEnum::CREATE_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::ALTER_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::DROP_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
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
        case PgMsgEnum::COPY_SYNC:
            {
                const auto &sync_msg = std::get<PgMsgCopySync>(msg->msg);
                _check_sync_commit(_db_id, sync_msg.pg_xid, sync_msg.target_xid);
                break;
            }

        default:
            SPDLOG_WARN("Unknown message type: {}", static_cast<uint8_t>(msg->msg_type));
            break;
        }
    }

    void
    PgLogReader::_check_sync_commit(uint64_t db_id,
                                    int32_t pg_xid,
                                    uint64_t xid)
    {
        // check if we need to perform a table swap / commit before proceeding
        auto &&xid_msg = SyncTracker::get_instance()->check_commit(db_id, pg_xid);
        if (xid_msg) {
            // synchronously issue the swap/commit at the GC-2 prior to processing this xid
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Issue TABLE_SYNC_COMMIT on {} @ {}", db_id, xid);
            _committer_queue.push(std::make_shared<XidReady>(xid_msg.value()));

            // once the swap/commit is complete, we can clear the entry from the sync
            // tracker and continue processing
            SyncTracker::get_instance()->clear_tables(db_id, *xid_msg);
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
        _current_batch->commit(xid, PostgresTimestamp(commit_msg.commit_ts));
        _batch_map.erase(xact->xid);
        _current_batch = nullptr;

        // check if we need to perform a table swap / commit before proceeding
        _check_sync_commit(_db_id, xact->xid, xid);

        // Record latency between postgres commit time and when we process it
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() -
            PostgresTimestamp(commit_msg.commit_ts).to_system_time());
        _postgres_log_reader_latencies->Record(duration.count(), _context);
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR,
                            "Commit processed {} milliseconds after postgres commit",
                            duration.count());

        // message the Committer
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Issue XID to committer on {} @ {}", _db_id, xid);
        _committer_queue.push(std::make_shared<XidReady>(_db_id, XidReady::XactMsg(xid)));

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
        _current_batch->commit(xid, PostgresTimestamp(commit_msg.commit_ts));
        _batch_map.erase(commit_msg.xid);

        // message the Committer
        _committer_queue.push(std::make_shared<XidReady>(_db_id, XidReady::XactMsg(xid)));

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
            _current_batch->abort(PostgresTimestamp(abort_msg.abort_ts));
            _batch_map.erase(abort_msg.xid);
        } else {
            // subtransaction aborted, add to parent xact aborted list
            itr->second->aborted_xids.insert(abort_msg.sub_xid);

            // abort the subtxn
            _current_batch->abort_subtxn(abort_msg.sub_xid, PostgresTimestamp(abort_msg.abort_ts));
            _batch_map.erase(abort_msg.sub_xid);
        }

        _current_batch = nullptr;
    }

    void
    PgLogReader::_process_ddl(uint32_t oid, int32_t pg_xid, bool is_streaming, PgMsgPtr msg)
    {
        int32_t pg_xid_txn;
        PgTransactionPtr xact;
        if (!is_streaming) {
            // not streaming, so pg_xid is invalid; use current_xact
            xact = _current_xact;
            pg_xid_txn = pg_xid = xact->xid;
        } else {
            // if streaming, then pg_xid is valid
            auto itr = _xact_map.find(pg_xid);
            if (itr == _xact_map.end()) {
                // no start streaming xact found...
                SPDLOG_WARN("PG_XID not found for message: pg_xid={}\n", pg_xid);
                return;
            }

            xact = itr->second;
            pg_xid_txn = xact->xid;
        }
        xact->oids.insert(oid);

        // check if we should ignore this message
        if (SyncTracker::get_instance()->should_skip(_db_id, oid, pg_xid_txn)) {
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Skip DDL: oid={} pg_xid={}\n", oid, pg_xid_txn);
            return;
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Process DDL: oid={} pg_xid={}\n", oid, pg_xid_txn);

        // record the schema change into the batch
        _current_batch->schema_change(oid, pg_xid, msg);
    }
} // namespace springtail::pg_log_mgr
