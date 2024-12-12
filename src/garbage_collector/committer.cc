#include <common/coordinator.hh>
#include <common/constants.hh>
#include <garbage_collector/committer.hh>
#include <garbage_collector/log_parser.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <pg_log_mgr/pg_redis_xact.hh>
#include <redis/db_state_change.hh>

namespace springtail::gc {

    void
    Committer::run()
    {
        // perform cleanup for any Committer threads in a previous run
        cleanup();

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        coordinator->register_thread(daemon_type, _worker_id);

        // initiate the worker threads
        for (int i = 0; i < _worker_count; i++) {
            _worker_threads.push_back(std::thread(&Committer::_run_worker, this, i));
        }

        // XXX we are currently processing XIDs one at a time, but we should eventually bundle
        //     together XID ranges when possible.  It makes sense to do this when the FDW nodes can
        //     do their own roll-forward.
        // XXX we could also process the XIDs from different databases within the instance in parallel

        // enter a loop polling for data from the write cache
        while (!_shutdown) {
            // update the coordinator
            coordinator->mark_alive(daemon_type, _worker_id);

            // figure out if there's an XID to process
            // note: this is a blocking call that will timeout after keep_alive secs
            auto result = _redis.pop(_worker_id, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (result == nullptr) {
                continue; // got a timeout, try again
            }
            uint64_t db_id = result->db();

            // handle a TABLE_SYNC_START
            if (result->type() == XidReady::Type::TABLE_SYNC_START) {
                // stop performing commits on this db until the table syncs are complete and aligned
                _block_commit.insert(db_id);

                // commit this work item and continue
                _redis.commit(_worker_id);
                continue;
            }

            // initialize the most recently completed XID for this database if needed
            uint64_t completed_xid;
            auto itr = _completed_xids.find(db_id);
            if (itr == _completed_xids.end()) {
                completed_xid = _xid_mgr->get_committed_xid(db_id, 0);
                _completed_xids[db_id] = completed_xid;
                _committed_xids[db_id] = completed_xid;
            } else {
                completed_xid = itr->second;
            }
            SPDLOG_INFO("Last completed XID: {}@{}", db_id, completed_xid);

            // handle a TABLE_SYNC_COMMIT
            if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT ||
                result->type() == XidReady::Type::TABLE_SYNC_SWAP) {
                SPDLOG_INFO("Handle a TABLE_SYNC_SWAP/COMMIT: {}, {}, completed xid @{}",
                            static_cast<char>(result->type()), db_id, completed_xid);

                nlohmann::json ddls;

                auto redis = RedisMgr::get_instance()->get_client();
                std::string key = fmt::format(redis::HASH_SYNC_TABLE_OPS,
                                              Properties::get_db_instance_id(), db_id);

                // note: Need to check the completed XID against the most recent committed XID.  If
                //       it is ahead, then we commit at the completed XID.  If it is the same then
                //       we commit at the provided XID.
                if (completed_xid == _committed_xids[db_id]) {
                    completed_xid = result->swap().xid();
                }

                // for operations at the SysTblMgr
                auto client = sys_tbl_mgr::Client::get_instance();

                // go through the hash of sys tbl operations
                for (auto table_id : result->swap().tids()) {
                    auto ops_str = redis->hget(key, fmt::format("{}", table_id));
                    SPDLOG_DEBUG_MODULE(LOG_GC, "table_id {}, ops: {}", table_id, *ops_str);
                    auto json = nlohmann::json::parse(*ops_str);

                    // perform the table swap
                    // note: we wait to perform this operation in the GC-2 to ensure that all system
                    //       table mutations up to this XID have already been applied, otherwise we
                    //       could potentially get a stray column added before the swap XID showing
                    //       up in the schema since it wouldn't get deleted by the DROP TABLE
                    auto create = common::json_to_thrift<sys_tbl_mgr::TableRequest>(json[0]);
                    create.xid = completed_xid;
                    create.lsn = constant::MAX_LSN - 1;

                    auto roots = common::json_to_thrift<sys_tbl_mgr::UpdateRootsRequest>(json[1]);
                    roots.xid = completed_xid;

                    auto ddl_str = client->swap_sync_table(create, roots);

                    // store the ddl mutations for the FDWs
                    auto ddl_ops = nlohmann::json::parse(ddl_str);
                    assert(ddl_ops.is_array());
                    for (int i = 0; i < ddl_ops.size(); ++i) {
                        ddls.push_back(ddl_ops[i]);
                    }

                    // clear the hash entry for the table
                    redis->hdel(key, fmt::format("{}", table_id));
                }

                SPDLOG_INFO("Swapped synced tables: {}@{}", db_id, completed_xid);

                // pre-commit the DDLs in case there's a failure
                _redis_ddl.precommit_ddl(db_id, completed_xid, ddls);

                if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
                    // finalize the system metadata
                    client->finalize(db_id, completed_xid);

                    // perform a commit to the XidMgr
                    _xid_mgr->commit_xid(db_id, completed_xid, true);
                    _committed_xids[db_id] = completed_xid;
                } else {
                    _xid_mgr->record_ddl_change(db_id, completed_xid);
                }
                _completed_xids[db_id] = completed_xid;

                // notify the FDW of the schema changes
                _redis_ddl.commit_ddl(db_id, completed_xid);

                if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
                    // notify everyone that the database is now in the "ready" state
                    redis::db_state_change::set_db_state(db_id, redis::db_state_change::DB_STATE_RUNNING);

                    // allow commits on future XIDs
                    _block_commit.erase(db_id);
                }

                // signal the GC-1 LogParser that it can unblock and continue operation
                _parser_notify.push(XidReady(db_id));

                // commit this work item and continue
                _redis.commit(_worker_id);
                continue;
            }

            // note: from here we know we have an XACT_MSG
            assert(result->type() == XidReady::Type::XACT_MSG);
            uint64_t xid = result->xact().xid();
            SPDLOG_INFO("Process XID: {}@{}", db_id, xid);
            assert(xid > completed_xid);

            // find every table associated with this XID
            uint64_t table_cursor = 0;
            bool tid_done = false;
            while (!tid_done) {
                // query the write cache for the tables modified through this XID
                auto table_list = _write_cache->list_tables(db_id, xid, 100, table_cursor);

                SPDLOG_DEBUG_MODULE(LOG_GC, "Got {} tables from the write cache", table_list.size());

                // check if we are done processing this XID
                if (table_list.empty()) {
                    tid_done = true;
                    break;
                }

                for (auto tid : table_list) {
                    SPDLOG_DEBUG_MODULE(LOG_GC, "Pass table {} to a worker", tid);
                    // mark this table as in-flight
                    {
                        boost::unique_lock lock(_mutex);
                        _tid_set.emplace(tid);
                    }

                    // pass each table to a worker thread to process it's mutations
                    auto entry = std::make_shared<WorkerEntry>(db_id, tid, completed_xid, xid);
                    _worker_queue.push(entry);
                }
            }

            // wait for tables to complete their processing
            // XXX ideally we could start working on the next XID while the finalize() operations
            //     are being completed.
            SPDLOG_DEBUG_MODULE(LOG_GC, "Wait for {} tables to complete", _tid_set.size());
            {
                boost::unique_lock lock(_mutex);
                _cv.wait(lock, [this]() { return _tid_set.empty(); });
            }
            SPDLOG_DEBUG_MODULE(LOG_GC, "All table processing complete for XID {}", xid);

            // retrieve any schema changes available in Redis
            auto &&ddls = _redis_ddl.get_ddls_xid(db_id, xid);
            if (!ddls.is_null()) {
                auto client = sys_tbl_mgr::Client::get_instance();

                // finalize the system metadata
                client->finalize(db_id, xid);

                // pre-commit the DDLs to be applied to the FDWs
                _redis_ddl.precommit_ddl(db_id, xid, ddls);
            }

            // check if we are doing an active table sync, in which case we have to block commits
            if (!_block_commit.contains(db_id)) {
                // commit the completed XID
                _xid_mgr->commit_xid(db_id, xid, !ddls.is_null());
                _committed_xids[db_id] = xid;
            } else if (!ddls.is_null()) {
                // don't commit, but record any DDL changes to the history
                _xid_mgr->record_ddl_change(db_id, xid);
            }
            _completed_xids[db_id] = xid;

            // push any DDL changes to the FDWs
            if (!ddls.is_null()) {
                _redis_ddl.commit_ddl(db_id, xid);
            }

            SPDLOG_DEBUG_MODULE(LOG_GC, "XID completed: {}@{}", db_id, xid);

            // mark the XID message as complete in the redis queue
            _redis.commit(_worker_id);

            // clear the DDL dependency data from the redis SortedSet
            // XXXXXX do we still need this?
            std::string key = fmt::format(redis::SET_PG_OID_XIDS,
                                          Properties::get_db_instance_id(), db_id);
            pg_log_mgr::RSSOidValue set(key);
            set.remove_by_score(0, xid);
        }

        // join all of the worker threads
        for (auto &thread : _worker_threads) {
            thread.join();
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, _worker_id);
    }

    void
    Committer::shutdown()
    {
        _shutdown = true;
        // XXX close the redis connection to speed up the shutdown?
    }

    void
    Committer::cleanup()
    {
        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        std::vector<std::string> cleanup_threads;

        // retrieve all of the threads for the daemon
        // note: we do this because there is a single GC daemon for both GC1 and GC2
        auto &&threads = coordinator->get_threads(daemon_type);
        for (const auto &thread_id : threads) {
            // check which class this is for
            std::vector<std::string> parts;
            common::split_string("_", thread_id, parts);

            // check the id is valid
            assert(parts.size() == 3 &&
                   (parts[0] == LogParser::THREAD_TYPE || parts[0] == THREAD_TYPE));

            // only handle "parser" threads here
            if (parts[0] != THREAD_TYPE) {
                continue;
            }
            cleanup_threads.push_back(thread_id);

            // perform thread-type-specific cleanup
            if (parts[1] == THREAD_MAIN) {
                // get the set of pre-committed DDL statements
                auto &&precommit = _redis_ddl.get_precommit_ddl();

                for (const auto &entry : precommit) {
                    uint64_t commit_xid = _xid_mgr->get_committed_xid(entry.first, 0);

                    if (entry.second <= commit_xid) {
                        // for those that are <= the committed XID, commit them
                        _redis_ddl.commit_ddl(entry.first, entry.second);
                    } else {
                        // for those that are > the committed XID, abort them
                        _redis_ddl.abort_ddl(entry.first, entry.second);
                    }
                }
            }
        }

        // unregister all parser threads from the previous run
        coordinator->unregister_threads(daemon_type, cleanup_threads);
    }

    void
    Committer::_run_worker(int thread_id)
    {
        std::string worker_id = fmt::format("{}_{}_{}", THREAD_TYPE, THREAD_WORKER, thread_id);

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        coordinator->register_thread(daemon_type, worker_id);

        // note: also wait on an empty queue to ensure it is drained before shutdown
        while (!_shutdown || !_worker_queue.empty()) {
            // update the coordinator
            coordinator->mark_alive(daemon_type, worker_id);

            // wait for work on the queue
            auto entry = _worker_queue.pop(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (entry == nullptr) {
                // check if this is due to a queue shutdown
                if (_worker_queue.is_shutdown()) {
                    break;
                }

                // timed out, try again
                continue;
            }

            // process all of the mutations for a given table in a given XID
            _process_table(entry->db_id, entry->tid, entry->completed_xid, entry->xid);

            // mark the table processing as complete
            {
                std::unique_lock lock(_mutex);
                _tid_set.erase(entry->tid);
            }

            // notify the main loop
            _cv.notify_one();
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, worker_id);
    }

    void
    Committer::_process_table(uint64_t db_id,
                              uint64_t tid,
                              uint64_t completed_xid,
                              uint64_t xid)
    {
        // construct the mutable table object
        auto table = TableMgr::get_instance()->get_mutable_table(db_id, tid, completed_xid, xid, true);

        // retrieve extents and apply the mutations to them
        uint64_t extent_cursor = 0;
        bool done = false;
        while (!done) {
            // XXX would be better if we could perform an async prefetch to reduce IO latency
            auto &&extent_list = _write_cache->get_extents(db_id, tid, xid, 1, extent_cursor);

            // if we didn't receive any extents then we're done
            if (extent_list.empty()) {
                done = true;
                break;
            }

            // process each extent of ordered mutations
            for (auto wc_extent : extent_list) {
                // get the schema at the given XID/LSN
                // note: we are guaranteed that the entire batch will utilize the same schema
                XidLsn xid(wc_extent.xid, wc_extent.lsn);
                auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, tid, xid);

                auto sort_keys = schema->get_sort_keys();
                sort_keys.push_back("__springtail_lsn");

                auto columns = schema->column_order();

                SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
                SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);
                std::vector<SchemaColumn> new_columns{op, lsn};

                auto wc_schema = schema->create_schema(columns, new_columns, sort_keys);

                // deserialize the extent
                ExtentHeader header(ExtentType(), wc_extent.xid, wc_schema->row_size());
                Extent extent(header);
                extent.deserialize(wc_extent.data);

                // process the rows
                auto op_f = wc_schema->get_field("__springtail_op");
                auto wc_fields = wc_schema->get_fields(columns);
                auto wc_key_fields = wc_schema->get_fields(schema->get_sort_keys());
                
                // XXX We know that these operations are sorted in key + LSN order, so we should be
                //     able to perform a more efficient merge using hints.  For a large extent we
                //     could parallelize the mutations.  The one exception is a table truncation,
                //     which must always appear first in a batch (although not necessarily first in
                //     the transaction).
                for (auto &row : extent) {
                    uint8_t op = op_f->get_uint8(row);
                    switch (op) {
                    case INSERT:
                        {
                            auto tuple = std::make_shared<FieldTuple>(wc_fields, row);
                            table->insert(tuple, wc_extent.xid, constant::UNKNOWN_EXTENT);
                            break;
                        }
                    case UPDATE:
                        {
                            auto tuple = std::make_shared<FieldTuple>(wc_fields, row);
                            table->update(tuple, wc_extent.xid, constant::UNKNOWN_EXTENT);
                            break;
                        }
                    case DELETE:
                        {
                            auto tuple = std::make_shared<FieldTuple>(wc_key_fields, row);
                            table->remove(tuple, wc_extent.xid, constant::UNKNOWN_EXTENT);
                            break;
                        }

                    case TRUNCATE:
                        {
                            // note: this should always be the first operation within an extent
                            table->truncate();
                            break;
                        }
                    }
                }
            }
        }

        // finalize the table
        auto &&metadata = table->finalize();

        // update the system table roots
        TableMgr::get_instance()->update_roots(table->db(), table->id(), xid, metadata);
    }
}
