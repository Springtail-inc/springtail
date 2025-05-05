#include <common/filesystem.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_log_mgr/sync_tracker.hh>

#include <redis/redis_ddl.hh>
#include <sys_tbl_mgr/client.hh>
#include <write_cache/write_cache_func.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::pg_log_mgr {

    PgLogReader::PgLogReader(uint64_t db_id, uint32_t queue_size,
                             const std::filesystem::path &repl_log_path,
                             const CommitterQueuePtr committer_queue,
                             const bool archive_logs)
        : _db_id(db_id),
          // retrieve the most recently committed XID at startup
         _committed_xid(xid_mgr::XidMgrServer::get_instance()->get_committed_xid(db_id, 0)),
          _archive_logs(archive_logs),
          _repl_log_path(repl_log_path),
          _committer_queue(committer_queue),
          _msg_queue(queue_size)
    {
        _xid_ts_tracker = std::make_shared<WalProgressTracker>();

        // construct the table existence cache
        _exists_cache = std::make_shared<ExistsCache>(8192); // XXX make this size a property?

        // start the message processing thread
        _msg_thread = std::thread(&PgLogReader::_msg_worker, this);
    }

    PgLogReader::~PgLogReader()
    {
        _msg_queue.shutdown(true);
        _msg_thread.join();
    }

    void
    PgLogReader::Batch::commit(uint64_t xid, PostgresTimestamp commit_ts)
    {
        time_trace::Trace commit_trace;
        TIME_TRACE_START(commit_trace);
        // update any changes in the table invalidation state
        for (const auto &entry : _table_validations) {
            if (entry.second) {
                TableValidator::get_instance()->mark_invalid(entry.first, *entry.second);
            } else {
                TableValidator::get_instance()->mark_valid(entry.first);
            }
        }

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
        // if (_span->IsRecording()) {
        //     _span->SetAttribute("xid", static_cast<int64_t>(xid));
        //     _span->SetAttribute("pg_xids", fmt::format("{}", fmt::join(pg_xids, ",")));
        // }
        // _span->End();
        TIME_TRACE_STOP(commit_trace);
        TIME_TRACESET_UPDATE(time_trace::traces, fmt::format("commit-xid_{}", xid), commit_trace);
    }

    void
    PgLogReader::Batch::abort(PostgresTimestamp abort_ts)
    {
        auto scope = open_telemetry::OpenTelemetry::tracer("PgLogReader")->WithActiveSpan(_span);
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
        auto scope = open_telemetry::OpenTelemetry::tracer("PgLogReader")->WithActiveSpan(_span);

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
        auto columns = table_schema->column_order();
        CHECK_GT(columns.size(), 0);

        auto sort_keys = table_schema->get_sort_keys();
        sort_keys.push_back("__springtail_lsn");

        SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
        SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);

        std::vector<SchemaColumn> new_columns{op, lsn};

        schema = table_schema->create_schema(columns, new_columns, sort_keys);

        op_f = schema->get_mutable_field("__springtail_op");
        lsn_f = schema->get_mutable_field("__springtail_lsn");

        // reset fields; forces a resync of fields during add_mutation()
        fields = nullptr;
    }

    FieldArrayPtr
    PgLogReader::Batch::TableEntry::get_pg_fields(Batch *batch,
                                                  const XidLsn &xid,
                                                  const MutableFieldArrayPtr fields,
                                                  const std::vector<int32_t> &pg_types)
    {
        auto pg_fields = std::make_shared<FieldArray>();
        for (int i = 0; i < fields->size(); i++) {
            // check if the pg_type is a user type
            auto type = fields->at(i)->get_type();
            if (pg_types[i] >= constant::FIRST_USER_DEFINED_PG_OID) {
                auto utp = batch->_usertype_cache_lookup(pg_types[i], xid);
                DCHECK(utp->exists);
                pg_fields->push_back(std::make_shared<PgEnumField>(type, i, utp));
            } else {
                pg_fields->push_back(std::make_shared<PgLogField>(type, i));
            }
        }
        return pg_fields;
    }

    void
    PgLogReader::Batch::TableEntry::update_fields(Batch *batch,
                                                  const XidLsn &xid)
    {
        DCHECK_NE(schema, nullptr);
        auto sort_keys = table_schema->get_sort_keys();
        bool has_primary = !sort_keys.empty();
        auto columns = table_schema->column_order();

        fields = schema->get_mutable_fields(columns);
        if (has_primary) {
            pkey_fields = schema->get_mutable_fields(sort_keys);
        } else {
            pkey_fields = fields;
        }

        // get the pg fields for all columns
        auto pg_types = table_schema->get_pg_types();
        pg_fields = get_pg_fields(batch, xid, fields, pg_types);

        // with no primary key; pg_fields and pg_key_fields are the same
        if (fields == pkey_fields) {
            pg_pkey_fields = pg_fields;
            return;
        }

        // primary keys are defined and different from the fields
        // get the pg fields for the primary key columns
        auto pkey_pg_types = table_schema->get_sort_key_pg_types();
        pg_pkey_fields = get_pg_fields(batch, xid, pkey_fields, pkey_pg_types);
    }

    template <int T>
    void
    PgLogReader::Batch::add_mutation(uint64_t current_xid,
                                     int32_t pg_xid,
                                     int32_t tid,
                                     PgMsgTupleData &data)
    {
        trace add_mutation_trace(fmt::format("add_mutation-xid_{}", current_xid));
        XidLsn xidlsn(current_xid);

        // check if we should skip the mutation due to an invalid table
        if (_check_invalid(tid)) {
            LOG_DEBUG(LOG_PG_REPL, "Skip mutation for invalid table with tid {}", tid);
            return;
        }

        // check if we should skip the mutation due to ongoing table sync
        auto sync_skip = SyncTracker::get_instance()->should_skip(_db, tid, _pg_xid);
        if (sync_skip.should_skip()) {
            LOG_DEBUG(LOG_PG_LOG_MGR,
                      "Skip mutation due to ongoing sync: oid={} pg_xid={}\n", tid,
                      pg_xid);
            return;
        }

        // check if the system is aware of this table -- if not, need to skip this mutation
        if (!sync_skip.is_syncing() && !_table_exists(tid, xidlsn)) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Skip mutation due to unknown table: tid={} pg_xid={}\n", tid,
                      pg_xid);
            return;
        }

        auto scope = open_telemetry::OpenTelemetry::tracer("PgLogReader")->WithActiveSpan(_span);
        auto txn = _get_txn(pg_xid);

        // get the Extent containing mutations
        auto &entry = txn->table_map[tid];
        if (entry.extent == nullptr) {
            if (entry.schema == nullptr) {
                entry.table_schema = sync_skip.schema();
                if (entry.table_schema == nullptr) {
                    entry.table_schema = SchemaMgr::get_instance()->get_extent_schema(_db, tid, xidlsn);
                }
                entry.update_schema();
            }

            entry.extent = std::make_shared<Extent>(ExtentType{}, 0, entry.schema->row_size());
            entry.start_lsn = _lsn;
        }

        // check if we need to update the fields
        if (entry.fields == nullptr) {
            entry.update_fields(this, xidlsn);
        }
        CHECK_NE(entry.fields, nullptr);

        // add the mutation to the batch
        LOG_DEBUG(LOG_PG_LOG_MGR, "Adding row: pg_xid={} tid={} op={}", pg_xid, tid, T);
        auto row = entry.extent->append();
        if constexpr (T == PgMsgEnum::INSERT || T == PgMsgEnum::UPDATE) {
            MutableTuple(entry.fields, row).assign(FieldTuple(entry.pg_fields, static_cast<const PgMsgTupleData*>(&data)));
        } else if constexpr (T == PgMsgEnum::DELETE) {
            MutableTuple(entry.pkey_fields, row).assign(FieldTuple(entry.pg_pkey_fields, static_cast<const PgMsgTupleData*>(&data)));
        } else {
            static_assert(false, "Invalid template parameter: PgLogReader::Batch::add_mutation");
        }
        entry.op_f->set_uint8(row, T);
        entry.lsn_f->set_uint64(row, _lsn++);

        LOG_DEBUG(LOG_PG_LOG_MGR, "Adding row: pg_xid={} tid={} op={}", pg_xid, tid, entry.op_f->get_uint8(row));

        // XXX we need some way to limit the total memory used by a batch across all extents

        // if the batch has grown to the maximum size, pass it to the WriteCache
        if (entry.extent->byte_count() > MAX_BATCH_SIZE) {
            WriteCacheFuncImpl::add_extent(_db, tid, pg_xid, entry.start_lsn, entry.extent);
            entry.extent = nullptr;
        }

        // TIME_TRACE_STOP(add_mutation_trace);
        // TIME_TRACESET_UPDATE(time_trace::traces, fmt::format("add_mutation-xid_{}", current_xid), add_mutation_trace);
    }

    void
    PgLogReader::Batch::truncate(uint64_t current_xid,
                                 const PgMsgTruncate &msg)
    {
        auto scope = open_telemetry::OpenTelemetry::tracer("PgLogReader")->WithActiveSpan(_span);

        // get the current txn
        auto txn = _get_txn(msg.xid);

        // note: there may be multiple truncates due to CASCADE
        for (auto tid : msg.rel_ids) {
            // check if the table is invalid
            if (_check_invalid(tid)) {
                LOG_DEBUG(LOG_PG_LOG_MGR, "Skip truncate due to invalid table: tid={} pg_xid={} xid={}\n", tid, _pg_xid, current_xid);
                return;
            }

            // check if we should skip this table due to ongoing table sync
            auto sync_skip = SyncTracker::get_instance()->should_skip(_db, tid, _pg_xid);
            if (sync_skip.should_skip()) {
                LOG_DEBUG(LOG_PG_LOG_MGR, "Skip truncate: oid={} pg_xid={}\n", tid, _pg_xid);
                continue;
            }

            // check if the system is aware of this table -- if not, need to skip this mutation
            if (!sync_skip.is_syncing() && !_table_exists(tid, XidLsn(current_xid))) {
                LOG_DEBUG(LOG_PG_LOG_MGR,
                          "Skip truncate due to unknown table: tid={} pg_xid={} xid={}\n", tid,
                          _pg_xid, current_xid);
                return;
            }

            // if we sent any batches to the WriteCache, evict them
            WriteCacheFuncImpl::drop_table(_db, tid, _cur_pg_xid);

            // XXX if this is a subtxn, then once it's committed, we could also drop any earlier
            //     mutations to the table in it's parent pg xid, but it's not strictly necessary

            // create a new extent that begins with a truncate operation
            auto &entry = txn->table_map[tid];
            if (entry.schema == nullptr) {
                XidLsn current(current_xid);
                entry.table_schema = sync_skip.schema();
                if (entry.table_schema == nullptr) {
                    entry.table_schema = SchemaMgr::get_instance()->get_extent_schema(_db, tid, current);
                }
                entry.update_schema();
            }

            entry.extent = std::make_shared<Extent>(ExtentType{}, 0, entry.schema->row_size());
            entry.start_lsn = _lsn;

            // add a TRUNCATE row
            auto &&row = entry.extent->append();
            entry.op_f->set_uint8(row, PgMsgEnum::TRUNCATE);
            entry.lsn_f->set_uint64(row, _lsn++);
        }
    }

    bool
    PgLogReader::Batch::_check_invalid(uint32_t table_oid)
    {
        bool was_invalid;
        auto valid_i = _table_validations.find(table_oid);
        if (valid_i != _table_validations.end()) {
            was_invalid = valid_i->second.has_value();
        } else {
            was_invalid = TableValidator::get_instance()->check_invalid(table_oid);
        }
        return was_invalid;
    }

    bool
    PgLogReader::Batch::_handle_validation(PgMsgPtr msg)
    {
        // check for DROP_TABLE to clear any existing invalidation
        if (msg->msg_type == PgMsgEnum::DROP_TABLE) {
            PgMsgDropTable &drop_msg = std::get<PgMsgDropTable>(msg->msg);
            _table_validations[drop_msg.oid] = std::nullopt;  // marked as valid
            return true;
        }

        // if not CREATE_TABLE or ALTER_TABLE at this point, can process the message
        if (msg->msg_type != PgMsgEnum::CREATE_TABLE && msg->msg_type != PgMsgEnum::ALTER_TABLE) {
            return true;
        }
        PgMsgTable &table_msg = std::get<PgMsgTable>(msg->msg);

        // check if the table contains invalid columns -- if so we need to ignore this table
        auto invalid_columns =
            TableValidator::get_instance()->validate_columns<PgMsgSchemaColumn>(table_msg.columns);

        if (invalid_columns.size() > 0) {
            // mark the table as invalid in this batch
            nlohmann::json table_info = {{"schema", table_msg.namespace_name},
                                         {"table", table_msg.oid},
                                         {"columns", invalid_columns}};
            _table_validations[table_msg.oid] = table_info;

            // if this was an ALTER then we need to drop the table
            if (msg->msg_type == PgMsgEnum::ALTER_TABLE) {
                LOG_DEBUG(LOG_PG_LOG_MGR,
                          "Dropping invalid table as part of alter: pg_xid={}, tid={}",
                          table_msg.xid, table_msg.oid);
                PgMsgDropTable drop_msg;
                drop_msg.xid = table_msg.xid;
                drop_msg.lsn = table_msg.lsn;
                drop_msg.oid = table_msg.oid;
                drop_msg.table = table_msg.table;
                drop_msg.namespace_name = table_msg.namespace_name;

                msg->msg_type = PgMsgEnum::DROP_TABLE;
                msg->msg = drop_msg;
            } else {
                return false;  // don't perform the CREATE_TABLE in this case
            }
        } else {
            // Check if the table was previously in an invalid state
            bool was_invalid = _check_invalid(table_msg.oid);

            if (was_invalid) {
                // The table is no longer invalid, remove the redis entry for the table
                _table_validations[table_msg.oid] = std::nullopt;  // marked as valid

                // if an ALTER_TABLE made the table valid, then we need to resync it
                if (msg->msg_type == PgMsgEnum::ALTER_TABLE) {
                    LOG_DEBUG(LOG_PG_LOG_MGR,
                              "Recreating invalid table as part of ALTER: pg_xid={}, tid={}",
                              table_msg.xid, table_msg.oid);
                    msg->msg_type = PgMsgEnum::ALTER_RESYNC;
                }
            }
        }

        return true;
    }

    void
    PgLogReader::Batch::schema_change(uint64_t current_xid,
                                      std::optional<uint32_t> tid,
                                      int32_t oid,
                                      uint32_t pg_xid,
                                      uint32_t pg_xid_txn,
                                      PgMsgPtr msg)
    {
        auto scope = open_telemetry::OpenTelemetry::tracer("PgLogReader")->WithActiveSpan(_span);

        // perform the table column validations and update the message accordingly
        if (!_handle_validation(msg)) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Skip CREATE_TABLE due to invalid table: tid={} pg_xid={}\n", oid, pg_xid);
            return;
        }

        // find the transaction
        auto txn = _get_txn(pg_xid);

        // check if we need to skip this message
        switch (msg->msg_type) {
            case PgMsgEnum::CREATE_NAMESPACE:
            case PgMsgEnum::ALTER_NAMESPACE:
            case PgMsgEnum::DROP_NAMESPACE:
                break;  // nothing to check for these

            case PgMsgEnum::ALTER_TYPE:
            case PgMsgEnum::CREATE_TYPE:
            case PgMsgEnum::DROP_TYPE: {
                // update the batch usertype cache appropriately
                auto &user_type = std::get<PgMsgUserType>(msg->msg);
                int32_t oid = user_type.oid;
                if (msg->msg_type == PgMsgEnum::DROP_TYPE) {
                    _user_types.erase(oid);
                    _user_types[oid] = std::make_shared<UserType>(oid, false);
                } else if (msg->msg_type == PgMsgEnum::CREATE_TYPE) {
                    UserTypePtr utp = std::make_shared<UserType>(oid, user_type.namespace_id, user_type.type, user_type.name, user_type.value_json);
                    _user_types[oid] = utp;
                } else if (msg->msg_type == PgMsgEnum::ALTER_TYPE) {
                    _user_types.erase(oid);
                    UserTypePtr utp = std::make_shared<UserType>(oid, user_type.namespace_id, user_type.type, user_type.name, user_type.value_json);
                    _user_types[oid] = utp;
                }
                break;
            }

            case PgMsgEnum::DROP_INDEX:
                break;  // XXX we should check both sync tracker and table existence against the
                        // table ID here

            case PgMsgEnum::CREATE_TABLE:
            case PgMsgEnum::ALTER_RESYNC: {
                // check if there's an ongoing sync for this table
                auto sync_skip = SyncTracker::get_instance()->should_skip(_db, *tid, pg_xid_txn);
                if (sync_skip.should_skip()) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip DDL: tid={} pg_xid={}\n", *tid, pg_xid_txn);
                    return;
                }

                // note: we don't check for existence here since this is going to cause existence
                break;
            }

            case PgMsgEnum::CREATE_INDEX:
            case PgMsgEnum::ALTER_TABLE:
            case PgMsgEnum::DROP_TABLE: {
                // check if there's an ongoing sync for this table
                auto sync_skip = SyncTracker::get_instance()->should_skip(_db, *tid, pg_xid_txn);
                if (sync_skip.should_skip()) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip DDL: tid={} pg_xid={}\n", *tid, pg_xid_txn);
                    return;
                }

                // check if the system is aware of this table -- if not, need to skip this mutation
                // note: if there's an ongoing sync then we consider that as existence unless it's
                //       overridden by a local mutation
                if (!sync_skip.is_syncing() && !_table_exists(*tid, XidLsn(current_xid))) {
                    LOG_DEBUG(LOG_PG_LOG_MGR,
                              "Skip schema change due to unknown table: tid={} pg_xid={}\n", *tid,
                              pg_xid);
                    return;
                }
                break;
            }

            default:
                LOG_ERROR("Message type {} not handled", static_cast<uint8_t>(msg->msg_type));
                throw Error();
        }

        // get the table entry
        // note: we use the OID for indexes since they don't impact the table schema
        auto &entry = txn->table_map[oid];

        // push any data mutations into the write cache since the schema is going to change
        if (entry.extent) {
            WriteCacheFuncImpl::add_extent(_db, oid, pg_xid,
                                           entry.start_lsn, entry.extent);
            entry.extent = nullptr;
        }

        // for table creation / mutation create a new schema
        if (msg->msg_type == PgMsgEnum::CREATE_TABLE || msg->msg_type == PgMsgEnum::ALTER_TABLE) {
            auto &table = std::get<PgMsgTable>(msg->msg);
            std::vector<SchemaColumn> columns;
            for (auto column : table.columns) {
                columns.emplace_back(SchemaColumn{
                        column.name,
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
    PgLogReader::Batch::_mark_table_resync(uint64_t table_oid, const XidLsn &xidlsn)
    {
        // mark the table as syncing to ensure we properly skip messages
        bool is_first = SyncTracker::get_instance()->mark_resync(_db, table_oid, xidlsn);

        // notify the PgLogParser to resync the table
        auto key = fmt::format(redis::QUEUE_SYNC_TABLES,
                                Properties::get_db_instance_id(), _db);
        RedisQueue<TableSyncRequest> table_sync_queue(key);
        TableSyncRequest request(table_oid, xidlsn);
        table_sync_queue.push(request);

        // Add a message to skip indexes for this table
        // for the currently building indexes and the ones
        // belonging to this transaction
        nlohmann::json ddl;
        RedisDDL redis_ddl;
        ddl["action"] = "abort_index";
        ddl["table_id"] = table_oid;
        redis_ddl.add_index_ddl(_db, xidlsn.xid, ddl.dump());

        // notify the Committer to stop committing XIDs
        if (is_first) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Stop committing XIDs for db: {}", _db);
            _committer_queue->push(std::make_shared<committer::XidReady>(_db));
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
                LOG_DEBUG(LOG_PG_LOG_MGR, "CREATE TABLE: xid={}, pg_xid={}, tid={}", xidlsn.xid,
                          table_msg.xid, table_msg.oid);

                std::string &&ddl_stmt = client->create_table(_db, xidlsn, table_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                _exists_cache->insert(_db, table_msg.oid, true);
                break;
            }
        case PgMsgEnum::ALTER_TABLE:
            {
                auto &table_msg = std::get<PgMsgTable>(change->msg);
                LOG_DEBUG(LOG_PG_LOG_MGR, "ALTER TABLE: xid={}, pg_xid={}, tid={}", xidlsn.xid,
                          table_msg.xid, table_msg.oid);

                std::string &&ddl_stmt = client->alter_table(_db, xidlsn, table_msg);

                // check for re-sync
                nlohmann::json action = nlohmann::json::parse(ddl_stmt).at("action");
                if (action.get<std::string>() == "resync") {
                    _mark_table_resync(table_msg.oid, xidlsn);
                } else if (action.get<std::string>() != "no_change") {
                    redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                }
                break;
            }
        case PgMsgEnum::DROP_TABLE:
            {
                auto &drop_msg = std::get<PgMsgDropTable>(change->msg);
                LOG_DEBUG(LOG_PG_LOG_MGR, "DROP TABLE: xid={}, pg_xid={}, tid={}", xidlsn.xid,
                          drop_msg.xid, drop_msg.oid);

                std::string &&ddl_stmt = client->drop_table(_db, xidlsn, drop_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                _exists_cache->insert(_db, drop_msg.oid, false);
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
        case PgMsgEnum::CREATE_TYPE:
            {
                auto &type_msg = std::get<PgMsgUserType>(change->msg);
                std::string &&ddl_stmt = client->create_usertype(_db, xidlsn, type_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::ALTER_TYPE:
            {
                auto &type_msg = std::get<PgMsgUserType>(change->msg);
                std::string &&ddl_stmt = client->alter_usertype(_db, xidlsn, type_msg);
                redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                break;
            }
        case PgMsgEnum::DROP_TYPE:
            {
                auto &type_msg = std::get<PgMsgUserType>(change->msg);
                std::string &&ddl_stmt = client->drop_usertype(_db, xidlsn, type_msg);
                auto json = nlohmann::json::parse(ddl_stmt);
                LOG_DEBUG(LOG_PG_LOG_MGR, "DROP TYPE: xid={}, pg_xid={}, tid={}, ddl={}", xidlsn.xid,
                          type_msg.xid, type_msg.oid, json.dump());
                if (json.at("action").get<std::string>() != "no_change") {
                    redis_ddl.add_ddl(_db, xidlsn.xid, ddl_stmt);
                }
                break;
            }
        case PgMsgEnum::CREATE_INDEX:
            {
                auto &index_msg = std::get<PgMsgIndex>(change->msg);
                std::string &&ddl_stmt = client->create_index(_db, xidlsn, index_msg,
                                                              sys_tbl::IndexNames::State::NOT_READY);

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
        case PgMsgEnum::ALTER_RESYNC:
            {
                // process the resync caused by an ALTER_TABLE
                auto &table_msg = std::get<PgMsgTable>(change->msg);
                _mark_table_resync(table_msg.oid, xidlsn);
                break;
            }

        default:
            LOG_ERROR("Message type {} not handled", static_cast<uint8_t>(change->msg_type));
            throw Error();
        }
    }

    void
    PgLogReader::get_queue_details()
    {
        if ( _msg_queue.size() > 0 ){
            LOG_INFO("[TRACE] Getting queue details, Queue size {}", _msg_queue.size());
            PgMsgPtr front_msg = _msg_queue.front();
            auto current_ts = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) * 1000;
            auto first_msg_ts = front_msg->pg_log_timestamp;
            LOG_INFO("[TRACE] Queue details: first message timestamp {}, current timestamp {}",
                    first_msg_ts, current_ts);
        }
    }

    void
    PgLogReader::enqueue_msg(PgMsgPtr msg)
    {
        _msg_queue.push(msg);
    }

    void
    PgLogReader::_msg_worker()
    {
        while (!_msg_queue.is_shutdown()) {
            // Try to get next message from queue, wait up to 1 second
            auto msg = _msg_queue.pop(1);
            if (msg == nullptr) {
                continue;
            }

            // Process the message
            _process_msg(msg);
        }
    }

    void
    PgLogReader::process_log(const std::filesystem::path &path,
                             uint64_t timestamp,
                             uint64_t start_offset,
                             int num_messages)
    {
        // init stream reader
        _reader.set_file(path, start_offset);

        static std::vector<char> filter = {
            pg_msg::MSG_BEGIN,
            pg_msg::MSG_COMMIT,
            pg_msg::MSG_STREAM_START,
            pg_msg::MSG_STREAM_COMMIT,
            pg_msg::MSG_STREAM_ABORT,
            pg_msg::MSG_INSERT,
            pg_msg::MSG_UPDATE,
            pg_msg::MSG_DELETE,
            pg_msg::MSG_TRUNCATE,
            pg_msg::MSG_MESSAGE // this will capture create_table, drop_table, alter_table,
                                // create_index, drop_index
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
                msg->pg_log_timestamp = timestamp;

                // process the message
                this->enqueue_msg(msg);
            }

            if (num_messages > 0) {
                num_messages--;
            }
        }
    }

    void
    PgLogReader::_remove_old_log_files()
    {
        uint64_t min_timestamp = _xid_ts_tracker->get_min_timestamp();
        fs::cleanup_files_from_dir(_repl_log_path, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX, min_timestamp, _archive_logs);
        fs::cleanup_files_from_dir(_repl_log_path, PgLogMgr::LOG_PREFIX_REPL_STREAMING, PgLogMgr::LOG_SUFFIX, min_timestamp, _archive_logs);
        if (min_timestamp == 0) {
            min_timestamp = _pg_log_timestamp;
        }
        xid_mgr::XidMgrServer::get_instance()->cleanup(_db_id, min_timestamp);
    }

    void
    PgLogReader::_process_msg(PgMsgPtr msg)
    {
        // note: it would probably be cheaper to send the rotate as an explicit one-time message
        //       rather than packing the log-timestamp into every message -- alternately we could
        //       only send it with the commit messages, since those are the only ones that affect
        //       the xact_log
        if (_pg_log_timestamp < msg->pg_log_timestamp) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Logs rollover to the new log timestamp id: {}", msg->pg_log_timestamp);
            _remove_old_log_files();
            _pg_log_timestamp = msg->pg_log_timestamp;
            if (_is_streaming) {
                fs::create_empty_file_with_timestamp(_repl_log_path, PgLogMgr::LOG_PREFIX_REPL_STREAMING, PgLogMgr::LOG_SUFFIX, _pg_log_timestamp);
            }
        }
        _is_streaming = msg->is_streaming;
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
                if (_current_batch == nullptr) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip INSERT because no batch tid={}", insert.rel_id);
                    break;
                }

                int32_t pg_xid = (msg->is_streaming) ? insert.xid : _current_xact->xid;
                LOG_DEBUG(LOG_PG_LOG_MGR, "INSERT pg_xid={} tid={}", pg_xid, insert.rel_id);

                // note: the current XID is only used to determine table existence
                _current_batch->add_mutation<PgMsgEnum::INSERT>(this->get_current_xid(), pg_xid,
                                                                insert.rel_id, insert.new_tuple);
                break;
            }
        case PgMsgEnum::DELETE:
            {
                auto &remove = std::get<PgMsgDelete>(msg->msg);
                if (_current_batch == nullptr) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip DELETE because no batch tid={}", remove.rel_id);
                    break;
                }

                int32_t pg_xid = (msg->is_streaming) ? remove.xid : _current_xact->xid;
                LOG_DEBUG(LOG_PG_LOG_MGR, "DELETE pg_xid={} tid={}", pg_xid, remove.rel_id);

                // note: the current XID is only used to determine table existence
                _current_batch->add_mutation<PgMsgEnum::DELETE>(this->get_current_xid(), pg_xid,
                                                                remove.rel_id, remove.tuple);
                break;
            }
        case PgMsgEnum::UPDATE:
            {
                auto &update = std::get<PgMsgUpdate>(msg->msg);
                if (_current_batch == nullptr) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip UPDATE because no batch tid={}", update.rel_id);
                    break;
                }

                int32_t pg_xid = (msg->is_streaming) ? update.xid : _current_xact->xid;
                LOG_DEBUG(LOG_PG_LOG_MGR, "UPDATE pg_xid={} tid={}", pg_xid, update.rel_id);

                // note: the current XID is only used to determine table existence
                uint64_t current_xid = this->get_current_xid();
                if (update.old_type == 0) {
                    _current_batch->add_mutation<PgMsgEnum::UPDATE>(current_xid, pg_xid,
                                                                    update.rel_id, update.new_tuple);
                } else {
                    _current_batch->add_mutation<PgMsgEnum::DELETE>(current_xid, pg_xid,
                                                                    update.rel_id, update.old_tuple);
                    _current_batch->add_mutation<PgMsgEnum::INSERT>(current_xid, pg_xid,
                                                                    update.rel_id, update.new_tuple);
                }
                break;
            }
            break;

        case PgMsgEnum::TRUNCATE:
            {
                LOG_DEBUG(LOG_PG_LOG_MGR, "TRUNCATE pg_xid={}", this->get_current_xid());
                if (_current_batch == nullptr) {
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip TRUNCATE because no batch");
                    break;
                }

                // note: the current XID is only used to determine table existence
                _current_batch->truncate(this->get_current_xid(),
                                         std::get<PgMsgTruncate>(msg->msg));
                break;
            }
        case PgMsgEnum::CREATE_TABLE:
        case PgMsgEnum::ALTER_TABLE:
            {
                PgMsgTable &table_msg = std::get<PgMsgTable>(msg->msg);
                _process_ddl(table_msg.oid, table_msg.oid, table_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::DROP_TABLE:
            {
                PgMsgDropTable &drop_msg = std::get<PgMsgDropTable>(msg->msg);
                _process_ddl(drop_msg.oid, drop_msg.oid, drop_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::CREATE_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(std::nullopt, namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::ALTER_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(std::nullopt, namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::DROP_NAMESPACE:
            {
                PgMsgNamespace &namespace_msg = std::get<PgMsgNamespace>(msg->msg);
                _process_ddl(std::nullopt, namespace_msg.oid, namespace_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::CREATE_TYPE:
            {
                PgMsgUserType &usertype_msg = std::get<PgMsgUserType>(msg->msg);
                _process_ddl(std::nullopt, usertype_msg.oid, usertype_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::ALTER_TYPE:
            {
                PgMsgUserType &usertype_msg = std::get<PgMsgUserType>(msg->msg);
                _process_ddl(std::nullopt, usertype_msg.oid, usertype_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::DROP_TYPE:
            {
                PgMsgUserType &usertype_msg = std::get<PgMsgUserType>(msg->msg);
                _process_ddl(std::nullopt, usertype_msg.oid, usertype_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::CREATE_INDEX:
            {
                const auto &index_msg = std::get<PgMsgIndex>(msg->msg);
                _process_ddl(index_msg.table_oid, index_msg.oid, index_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::DROP_INDEX:
            {
                const auto &index_msg = std::get<PgMsgDropIndex>(msg->msg);
                _process_ddl(std::nullopt, index_msg.oid, index_msg.xid, msg->is_streaming, msg);
                break;
            }
        case PgMsgEnum::COPY_SYNC:
            {
                const auto &sync_msg = std::get<PgMsgCopySync>(msg->msg);
                _check_sync_commit(_db_id, sync_msg.pg_xid);
                break;
            }
        case PgMsgEnum::RECONCILE_INDEX:
            {
                const auto &idx_reconcile_msg = std::get<PgMsgReconcileIndex>(msg->msg);
                _process_index_reconciliation(idx_reconcile_msg.db_id, idx_reconcile_msg.reconcile_xid);
                break;
            }
        default:
            LOG_WARN("Unknown message type: {}", static_cast<uint8_t>(msg->msg_type));
            break;
        }
    }

    void
    PgLogReader::_check_sync_commit(uint64_t db_id,
                                    int32_t pg_xid)
    {
        // check if we need to perform a table swap / commit before proceeding
        auto swap = SyncTracker::get_instance()->check_commit(db_id, pg_xid);
        if (swap != nullptr) {
            // once the swap/commit is ready, we can clear the entries from the sync tracker
            SyncTracker::get_instance()->clear_tables(swap);

            // set the XID of the swap/commit
            uint64_t xid = get_next_xid();

            // for operations at the SysTblMgr
            auto client = sys_tbl_mgr::Client::get_instance();
            nlohmann::json ddls = nlohmann::json::array({});

            // issue the updates to the system tables
            for (auto &entry : swap->table_info()) {
                // update the existence cache for the referenced tables
                _exists_cache->insert(db_id, entry->table_id, true);

                auto copy_info = entry->info;
                if (copy_info == nullptr) {
                    // During resync if the table is found to be invalid as part of the copy flow, the table
                    // becomes invalidated the copy_ptr becomes null, in those cases we don't need to
                    // perform any operaion and just skip
                    LOG_DEBUG(LOG_PG_LOG_MGR, "Copy info not present for table {}", entry->table_id);
                    continue;
                }
                LOG_DEBUG(LOG_PG_LOG_MGR, "table_id {}", entry->table_id);

                // perform the table swap
                auto *namespace_req = copy_info->mutable_namespace_req();
                namespace_req->set_xid(xid);
                namespace_req->set_lsn(constant::RESYNC_NAMESPACE_LSN);

                auto *create = copy_info->mutable_table_req();
                create->set_xid(xid);
                create->set_lsn(constant::RESYNC_CREATE_LSN);

                auto *indexes = copy_info->mutable_index_reqs();
                std::vector<proto::IndexRequest> indexes_vec;
                for (auto &index : *indexes) {
                    index.set_xid(xid);
                    index.set_lsn(constant::RESYNC_CREATE_LSN);
                    indexes_vec.push_back(index);
                }

                auto *roots = copy_info->mutable_roots_req();
                roots->set_xid(xid);

                // note: this will also invalidate the table's client cache entry
                auto ddl_str = client->swap_sync_table(*namespace_req, *create, indexes_vec, *roots);

                // store the ddl mutations for the FDWs
                auto ddl = nlohmann::json::parse(ddl_str);
                assert(ddl.is_array());
                ddls.insert(ddls.end(), ddl.begin(), ddl.end());
            }
            LOG_INFO("Swapped synced tables: {}@{}", db_id, xid);

            auto xid_msg = std::make_shared<committer::XidReady>
                (swap->type(), swap->db(), _pg_log_timestamp,
                 committer::XidReady::SwapMsg(xid, std::move(ddls)));

            // issue the swap/commit at the GC-2 prior to processing this xid
            LOG_DEBUG(LOG_PG_LOG_MGR, "Issue COMMIT/SWAP message to committer on {} @ {}, type {}",
                      db_id, xid, std::string(1, xid_msg->type()));
            _committer_queue->push(xid_msg);
        }
    }

    void
    PgLogReader::_process_index_reconciliation(const uint64_t db_id, const uint64_t reconcile_xid)
    {
        uint64_t xid = this->get_next_xid();
        LOG_DEBUG(LOG_PG_LOG_MGR, "Issue Index Reconciliation message to committer on {} @ {}", db_id, xid);
        _committer_queue->push(std::make_shared<committer::XidReady>(db_id, _pg_log_timestamp, committer::XidReady::ReconcileMsg(xid, reconcile_xid)));
    }

    void
    PgLogReader::_process_begin(const PgMsgBegin &begin_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Begin: xid={}, xact_lsn={}\n", begin_msg.xid, begin_msg.xact_lsn);

        PgTransactionPtr xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_COMMIT);
        xact->xact_lsn = begin_msg.xact_lsn;
        xact->xid = begin_msg.xid;
        _current_xact = xact;

        // prepare a batch for processing
        _current_batch = std::make_shared<Batch>(_db_id, begin_msg.xid, _committer_queue, _exists_cache);
        _batch_map.try_emplace(begin_msg.xid, _current_batch);
        _xid_ts_tracker->add_pg_xid(_current_xact->xid, _pg_log_timestamp);
    }

    void
    PgLogReader::_process_commit(const PgMsgCommit &commit_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Commit: commit_lsn={}, xact_lsn={}\n", commit_msg.commit_lsn, commit_msg.xact_lsn);

        PgTransactionPtr xact = _current_xact;
        if (_current_xact == nullptr || commit_msg.commit_lsn != _current_xact->xact_lsn) {
            // we don't have the start of the transaction...
            LOG_WARN("No matching xact for commit: commit_lsn={}\n", commit_msg.commit_lsn);
            return;
        }

        CHECK_EQ(xact->type, PgTransaction::TYPE_COMMIT);

        // check if we need to perform a table swap / commit before proceeding
        _check_sync_commit(_db_id, xact->xid);

        // assign a Springtail XID to this transaction
        uint64_t xid = this->get_next_xid();

        // If the assigned XID is <= the most recently committed XID, then we need to skip this
        // transaction.  This can occur in the unlikely case that we are performing a log recovery
        // and the Committer got ahead of the XactLog flushing so that the committed XID is ahead of
        // the most recently written PGXID -> Springtail XID mapping.
        auto postgres_timestamp = PostgresTimestamp(commit_msg.commit_ts);
        if (xid <= _committed_xid) {
            // we abort this batch since it was already processed
            _current_batch->abort(postgres_timestamp);
            _xid_ts_tracker->remove_pg_xid(_current_xact->xid);
        } else {
            // update the write cache and system tables as needed
            _current_batch->commit(xid, postgres_timestamp);
            _xid_ts_tracker->add_xid(_current_xact->xid, xid);
        }

        // clear the current batch
        _batch_map.erase(xact->xid);
        _current_batch = nullptr;

        // note: this check should only be false when re-processing records during recovery
        if (xid > _committed_xid) {
            // Record latency between postgres commit time and when we process it
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - postgres_timestamp.to_system_time());
            open_telemetry::OpenTelemetry::record_histogram(PG_LOG_MGR_LOG_READER_LATENCIES, duration.count());
            LOG_DEBUG(LOG_PG_LOG_MGR, "Commit processed {} milliseconds after postgres commit",
                      duration.count());

            // message the Committer
            LOG_DEBUG(LOG_PG_LOG_MGR, "Issue XactMsg to committer on {} @ {}", _db_id, xid);
            _committer_queue->push(std::make_shared<committer::XidReady>(_db_id, _pg_log_timestamp, committer::XidReady::XactMsg(xact->xid, xid),
                                    _xid_ts_tracker));
        }

        // clear the current xact
        _current_xact = nullptr;
    }

    void
    PgLogReader::_process_stream_start(const PgMsgStreamStart &start_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Stream start: xid={}, first={}\n", start_msg.xid, start_msg.first);

        if (!start_msg.first) {
            auto itr = _batch_map.find(start_msg.xid);
            assert(itr != _batch_map.end());

            _current_batch = itr->second;
            return;
        }

        // new transaction
        PgTransactionPtr xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_COMMIT);
        xact->xid = start_msg.xid;
        _xact_map.insert({xact->xid, xact});

        // prepare a batch for processing
        _current_batch = std::make_shared<Batch>(_db_id, start_msg.xid, _committer_queue, _exists_cache);
        _batch_map.try_emplace(start_msg.xid, _current_batch);
        _xid_ts_tracker->add_pg_xid(xact->xid, _pg_log_timestamp);
    }

    void
    PgLogReader::_process_stream_commit(const PgMsgStreamCommit &commit_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Stream commit: xid={}, xact_lsn={}\n", commit_msg.xid, commit_msg.xact_lsn);

        // commit only happens for the top level xid, subxacts under the xid
        // automatically commit unless they were previously aborted
        auto itr = _xact_map.find(commit_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            LOG_WARN("No matching xact for stream commit: xid={}, xact_lsn={}",
                        commit_msg.xid, commit_msg.xact_lsn);
            return;
        }

        PgTransactionPtr xact = itr->second;
        xact->xact_lsn = commit_msg.xact_lsn;

        CHECK_EQ(xact->type, PgTransaction::TYPE_COMMIT);

        // check if we need to perform a table swap / commit before proceeding
        _check_sync_commit(_db_id, xact->xid);

        // assign the transaction a Springtail XID
        uint64_t xid = this->get_next_xid();

        // If the assigned XID is <= the most recently committed XID, then we need to skip this
        // transaction.  This can occur in the unlikely case that we are performing a log recovery
        // and the Committer got ahead of the XactLog flushing so that the committed XID is ahead of
        // the most recently written PGXID -> Springtail XID mapping.
        if (xid <= _committed_xid) {
            // we abort this batch since it was already processed
            _current_batch->abort(PostgresTimestamp(commit_msg.commit_ts));
            _xid_ts_tracker->remove_pg_xid(commit_msg.xid);
        } else {
            // update the write cache and system tables as needed
            _current_batch->commit(xid, PostgresTimestamp(commit_msg.commit_ts));
            _xid_ts_tracker->add_xid(commit_msg.xid, xid);
        }

        // free the batch
        _batch_map.erase(commit_msg.xid);

        if (xid > _committed_xid) {
            // message the Committer
            LOG_DEBUG(LOG_PG_LOG_MGR, "Issue XactMsg to committer on {} @ {}", _db_id, xid);
            _committer_queue->push(std::make_shared<committer::XidReady>(_db_id, _pg_log_timestamp, committer::XidReady::XactMsg(commit_msg.xid, xid),
                                    _xid_ts_tracker));
        }

        // remove the xact from the active map
        _xact_map.erase(itr);
        _current_batch = nullptr;
    }

    void
    PgLogReader::_process_stream_abort(const PgMsgStreamAbort &abort_msg)
    {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Stream abort: xid={}, sub_xid={}\n", abort_msg.xid, abort_msg.sub_xid);

        auto itr = _xact_map.find(abort_msg.xid);
        if (itr == _xact_map.end()) {
            // no start streaming xact found...
            LOG_WARN("No matching xact for stream abort: xid={}, xact_lsn={}",
                        abort_msg.xid, abort_msg.abort_lsn);
            return;
        }

        if (abort_msg.sub_xid == abort_msg.xid) {
            // if sub_xid == xid, then it's a top level xact that aborted
            // remove it from the map
            _xact_map.erase(itr);

            // abort the txn
            _current_batch->abort(PostgresTimestamp(abort_msg.abort_ts));
            _batch_map.erase(abort_msg.xid);
            _xid_ts_tracker->remove_pg_xid(abort_msg.xid);
        } else {
            // abort the subtxn
            _current_batch->abort_subtxn(abort_msg.sub_xid, PostgresTimestamp(abort_msg.abort_ts));
            _batch_map.erase(abort_msg.sub_xid);
        }

        _current_batch = nullptr;
    }

    void
    PgLogReader::_process_ddl(std::optional<uint32_t> table_oid, uint32_t oid, int32_t pg_xid, bool is_streaming, PgMsgPtr msg)
    {
        if (_current_batch == nullptr) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Skip DDL because no batch type={}", static_cast<char>(msg->msg_type));
            return;
        }

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
                LOG_WARN("PG_XID not found for message: pg_xid={}\n", pg_xid);
                return;
            }

            xact = itr->second;
            pg_xid_txn = xact->xid; // the parent pgxid for this txn
        }

        LOG_DEBUG(LOG_PG_LOG_MGR, "Process DDL: oid={} pg_xid={}\n", oid, pg_xid_txn);

        // record the schema change into the batch
        // note: the current XID is only used to determine table existence
        _current_batch->schema_change(this->get_current_xid(), table_oid, oid, pg_xid, pg_xid_txn, msg);
    }
} // namespace springtail::pg_log_mgr
