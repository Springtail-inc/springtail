#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

#include <common/constants.hh>
#include <common/coordinator.hh>
#include <common/open_telemetry.hh>
#include <pg_log_mgr/pg_redis_xact.hh>
#include <proto/pg_copy_table.pb.h>
#include <redis/db_state_change.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_func.hh>
#include <xid_mgr/xid_mgr_server.hh>

#include <pg_log_mgr/committer.hh>

namespace springtail::committer {

    bool
    _index_exists(uint64_t db_id, uint64_t tid, uint64_t index_id, uint64_t xid)
    {
        auto meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, tid, xid);
        auto it =
            std::ranges::find_if(meta->roots, [&](auto const &v) { return index_id == v.index_id; });
        return it != meta->roots.end();
    }

    void
    Committer::run()
    {
        // perform cleanup for any Committer threads in a previous run
        cleanup();
        _create_indexer();

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        auto &keep_alive = coordinator->register_thread(daemon_type, "committer");

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
            Coordinator::mark_alive(keep_alive);

            // figure out if there's an XID to process
            // note: this is a blocking call that will timeout after keep_alive secs
            auto result = _committer_queue->pop(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (result == nullptr) {
                continue; // got a timeout, try again
            }

            // perform rotation if needed
            uint64_t db_id = result->db();
            uint64_t timestamp = result->timestamp();
            uint64_t stored_timestamp = 0;
            auto emplace_result = _db_to_timestamp.try_emplace(db_id, timestamp);
            if (!emplace_result.second) {
                // set stored_timestamp
                stored_timestamp = emplace_result.first->second;
            }
            if (timestamp > stored_timestamp) {
                xid_mgr::XidMgrServer::get_instance()->rotate(db_id, timestamp);
                emplace_result.first->second = timestamp;
            }

            auto token_1 = open_telemetry::OpenTelemetry::set_context_variables({{"db_id", std::to_string(db_id)}});

            // handle a TABLE_SYNC_START
            if (result->type() == XidReady::Type::TABLE_SYNC_START) {
                LOG_DEBUG(LOG_COMMITTER, "Stop committing due to table sync: {}", db_id);
                // stop performing commits on this db until the table syncs are complete and aligned
                _block_commit.insert(db_id);

                continue;
            }

            // initialize the most recently completed XID for this database if needed
            uint64_t completed_xid;
            auto itr = _completed_xids.find(db_id);
            if (itr == _completed_xids.end()) {
                completed_xid = xid_mgr::XidMgrServer::get_instance()->get_committed_xid(db_id, 0);
                _completed_xids[db_id] = completed_xid;
            } else {
                completed_xid = itr->second;
            }

            auto token_2 = open_telemetry::OpenTelemetry::set_context_variables({{"xid", std::to_string(completed_xid)}});
            LOG_INFO("Last completed XID: {}@{}", db_id, completed_xid);

            // handle a TABLE_SYNC_COMMIT
            if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT ||
                result->type() == XidReady::Type::TABLE_SYNC_SWAP) {
                LOG_DEBUG(
                    LOG_COMMITTER,
                    "Handle a TABLE_SYNC_SWAP/COMMIT: {}, {}, completed xid @{}, request xid @{}",
                    static_cast<char>(result->type()), db_id, completed_xid, result->swap().xid());
                CHECK_GT(result->swap().xid(), completed_xid);

                // note: we used to bundle the commit onto the previous XID, but now the XID is guaranteed to be in-order
                completed_xid = result->swap().xid();
                nlohmann::json ddls = result->swap().ddls();

                auto token_3 = open_telemetry::OpenTelemetry::set_context_variables({{"xid", std::to_string(completed_xid)}});

                // pre-commit the DDLs in case there's a failure
                _redis_ddl.precommit_ddl(db_id, completed_xid, ddls);
                _has_ddl_precommit = true;

                if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
                    // finalize the system metadata
                    sys_tbl_mgr::Client::get_instance()->finalize(db_id, completed_xid);

                    // perform a commit to the XidMgr
                    xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, 0, completed_xid, true);

                    LOG_DEBUG(LOG_COMMITTER, "Commit DDL changes db {} xid {}", db_id, completed_xid);
                    // notify the FDW of the schema changes
                    if (_has_ddl_precommit) {
                        _redis_ddl.commit_ddl(db_id, completed_xid);
                        _has_ddl_precommit = false;
                    }
                } else {
                    LOG_DEBUG(LOG_COMMITTER, "Record DDL changes db {} xid {}", db_id, completed_xid);
                    xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, 0, completed_xid, true);
                }
                _completed_xids[db_id] = completed_xid;

                if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
                    // notify everyone that the database is now in the "ready" state
                    redis::db_state_change::set_db_state(db_id,
                                                         redis::db_state_change::DB_STATE_RUNNING);

                    // allow commits on future XIDs
                    _block_commit.erase(db_id);
                }

                continue;
            }

            // note: from here we know we have an XACT_MSG or RECONCILE_INDEX
            // XXX: Once we confirm we can commit the index at table's last XID safely,
            //      we can remove the type RECONCILE_INDEX
            CHECK(result->type() == XidReady::Type::XACT_MSG || result->type() == XidReady::Type::RECONCILE_INDEX);
            uint64_t xid = 0;
            uint64_t pg_xid = 0;
            if (result->type() == XidReady::Type::RECONCILE_INDEX) {
                xid = result->reconcile().xid();
            } else {
                xid = result->xact().xid();
                pg_xid = result->xact().pg_xid();
            }
            auto token_4 = open_telemetry::OpenTelemetry::set_context_variables({{"xid", std::to_string(xid)}});
            LOG_INFO("Process XID: {}@{}", db_id, xid);
            assert(xid > completed_xid);

            // check if there were DDL mutations as part of this txn, invalidate the schema cache
            // accordingly
            nlohmann::json completed_ddls = _redis_ddl.get_ddls_xid(db_id, xid);
            if (!completed_ddls.is_null()) {
                _invalidate_systbl_cache(db_id, completed_ddls);
            }

            // find every table associated with this XID
            uint64_t table_cursor = 0;
            bool tid_done = false;
            while (!tid_done) {
                // query the write cache for the tables modified through this XID
                auto table_list = WriteCacheFuncImpl::list_tables(db_id, xid, 100, table_cursor);

                LOG_DEBUG(LOG_COMMITTER, "Got {} tables from the write cache", table_list.size());

                // check if we are done processing this XID
                if (table_list.empty()) {
                    tid_done = true;
                    break;
                }

                for (auto tid : table_list) {
                    LOG_DEBUG(LOG_COMMITTER, "Pass table {} to a worker", tid);
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
            LOG_DEBUG(LOG_COMMITTER, "Wait for {} tables to complete", _tid_set.size());
            {
                boost::unique_lock lock(_mutex);
                _cv.wait(lock, [this]() { return _tid_set.empty(); });
            }
            LOG_DEBUG(LOG_COMMITTER, "All table processing complete for XID {}", xid);

            nlohmann::json index_ddls = _redis_ddl.get_index_ddls_xid(db_id, xid);

            // Trigger index reconciliation for the earliest pending XID
            if (result->type() == XidReady::Type::RECONCILE_INDEX) {
                _indexer->process_index_reconciliation(db_id, result->reconcile().reconcile_xid(), xid);
            }

            if (!index_ddls.is_null()) {
                _redis_ddl.precommit_index_ddl(db_id, xid, index_ddls);

                // process the indexes - create/drop, allowing them to happen in the background
                _indexer->process_ddls(db_id, xid, index_ddls);

                // Abort index_ddls if they have only abort_index
                bool only_abort_index_ddls = std::ranges::all_of(index_ddls, [](const auto& ddl) {
                        return ddl["action"] == "abort_index";
                        });

                if (only_abort_index_ddls) {
                    _redis_ddl.abort_index_ddl(db_id, xid);
                }
            }

            if (!completed_ddls.is_null()) {
                // pre-commit the DDLs to be applied to the FDWs
                _redis_ddl.precommit_ddl(db_id, xid, completed_ddls);
                _has_ddl_precommit = true;
            }

            // check if we are doing an active table sync, in which case we have to block commits
            if (!_block_commit.contains(db_id)) {
                // finalize the system metadata
                // note: we do this even without DDL changes to ensure the primary and secondary
                //       index root offsets are written to disk
                sys_tbl_mgr::Client::get_instance()->finalize(db_id, xid);

                // commit the completed XID
                xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, pg_xid, xid, !completed_ddls.is_null());

                // push completed DDL changes to the FDWs
                if (_has_ddl_precommit) {
                    LOG_DEBUG(LOG_COMMITTER, "Commit DDL changes db {} xid {}", db_id, xid);
                    _redis_ddl.commit_ddl(db_id, xid);
                    _has_ddl_precommit = false;
                }
            } else {
                // don't commit, but record any DDL changes to the history
                xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, pg_xid, xid, !completed_ddls.is_null());
            }
            _completed_xids[db_id] = xid;

            if (result->type() == XidReady::Type::RECONCILE_INDEX) {
                // Commit index XID as they complete reconciliation
                _redis_ddl.commit_index_ddl(db_id, result->reconcile().reconcile_xid());
            } else if (result->type() == XidReady::Type::XACT_MSG) {
                result->notify_tracker(xid);
            }

            LOG_DEBUG(LOG_COMMITTER, "XID completed: {}@{}", db_id, xid);
        }

        // join all of the worker threads
        for (auto &thread : _worker_threads) {
            thread.join();
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, "committer");

        _indexer.reset();
        LOG_DEBUG(LOG_COMMITTER, "Committer shutdown");
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
            assert(parts.size() == 3 && parts[0] == THREAD_TYPE);

            // record the thread ID for cleanup
            cleanup_threads.push_back(thread_id);

            // perform thread-type-specific cleanup
            if (parts[1] == THREAD_MAIN) {
                // get the set of pre-committed DDL statements
                auto &&precommit = _redis_ddl.get_precommit_ddl();

                for (const auto &entry : precommit) {
                    uint64_t commit_xid = xid_mgr::XidMgrServer::get_instance()->get_committed_xid(entry.first, 0);

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
    Committer::_invalidate_systbl_cache(uint64_t db, const nlohmann::json &completed_ddls)
    {
        auto client = sys_tbl_mgr::Client::get_instance();
        for (auto ddl : completed_ddls) {
            if (!ddl.contains("tid")) {
                continue; // mutation doesn't reference a specific table
            }

            uint64_t tid = ddl["tid"].get<uint64_t>();
            XidLsn ddl_xid(ddl["xid"].get<uint64_t>(), ddl["lsn"].get<uint64_t>());
            client->invalidate_table(db, tid, ddl_xid);
        }
    }

    void
    Committer::_create_indexer()
    {
        // use the same worker count for Indexer
        _indexer = std::make_unique<Indexer>(_worker_count, _index_reconciliation_queue);

        // cleanup
        auto &&precommit = _redis_ddl.get_precommit_index_ddl();

        //make sure it is sorted
        std::ranges::sort(precommit, [](auto const& a, auto const& b) {
                    auto const& [db_id1, xid1, v1] = a;
                    auto const& [db_id2, xid2, v2] = b;
                    if (db_id1 == db_id2) {
                        return xid1 < xid2;
                    }
                    return db_id1 < db_id2;
                });

        for (auto [db_id, xid, ddls] : precommit) {
            _indexer->process_ddls(db_id, xid, ddls["ddls"]);
        }
    }

    void
    Committer::_run_worker(int thread_id)
    {
        std::string worker_id = fmt::format("{}_{}_{}", THREAD_TYPE, THREAD_WORKER, thread_id);

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        auto& keep_alive = coordinator->register_thread(daemon_type, worker_id);

        // note: also wait on an empty queue to ensure it is drained before shutdown
        while (!_shutdown || !_worker_queue.empty()) {
            // update the coordinator
            Coordinator::mark_alive(keep_alive);

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
        std::optional<PostgresTimestamp> min_commit_ts;
        while (true) {
            // XXX would be better if we could perform an async prefetch to reduce IO latency
            PostgresTimestamp commit_ts;
            auto &&extent_list = WriteCacheFuncImpl::get_extents(db_id, tid, xid, 1, extent_cursor, commit_ts);
            if (!min_commit_ts || commit_ts < *min_commit_ts) {
                min_commit_ts = commit_ts;
            }

            // if we didn't receive any extents then we're done
            if (extent_list.empty()) {
                break;
            }

            // process each extent of ordered mutations
            for (auto wc_extent : extent_list) {
                _process_extent(db_id, tid, table, wc_extent);
            }
        }

        // finalize the table
        auto &&metadata = table->finalize();

        if (min_commit_ts) {
            // log how long it took to process this table
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() - min_commit_ts->to_system_time());
            open_telemetry::OpenTelemetry::record_histogram(PG_LOG_MGR_BTREE_LATENCIES, duration.count());
            LOG_DEBUG(LOG_COMMITTER, "Processed table {} in {} milliseconds", tid, duration.count());
            LOG_ERROR("Processed table {} in {} milliseconds", tid, duration.count());
        }
        // update the system table roots
        TableMgr::get_instance()->update_roots(table->db(), table->id(), xid, metadata);
    }

    void
    Committer::_process_extent(uint64_t db_id,
                               uint64_t tid,
                               MutableTablePtr table,
                               const std::shared_ptr<springtail::WriteCacheIndexExtent> wc_extent)
    {
        // get the schema at the given XID/LSN
        // note: we are guaranteed that the entire batch will utilize the same schema
        XidLsn xid(wc_extent->xid, wc_extent->xid_seq);
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, tid, xid);

        auto sort_keys = schema->get_sort_keys();
        sort_keys.push_back("__springtail_lsn");

        auto columns = schema->column_order();
        LOG_DEBUG(LOG_COMMITTER, "xid={}:{}, columns={}",
                            xid.xid, xid.lsn,
                            common::join_string(",", columns.begin(), columns.end()));

        SchemaColumn op("__springtail_op", 0, SchemaType::UINT8, 0, false);
        SchemaColumn lsn("__springtail_lsn", 0, SchemaType::UINT64, 0, false);
        std::vector<SchemaColumn> new_columns{op, lsn};

        auto wc_schema = schema->create_schema(columns, new_columns, sort_keys);

        // Get the extent from the write cache index
        Extent extent(*wc_extent->data);
        LOG_DEBUG(LOG_COMMITTER, "xid={} rows={}", xid.xid, extent.row_count());

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
            uint8_t op = op_f->get_uint8(&row);
            switch (op) {
            case INSERT:
                {
                    auto tuple = std::make_shared<FieldTuple>(wc_fields, &row);
                    LOG_DEBUG(LOG_COMMITTER, "INSERT value={}", tuple->to_string());
                    table->insert(tuple, constant::UNKNOWN_EXTENT);
                    break;
                }
            case UPDATE:
                {
                    auto tuple = std::make_shared<FieldTuple>(wc_fields, &row);
                    LOG_DEBUG(LOG_COMMITTER, "UPDATE value={}", tuple->to_string());
                    table->update(tuple, constant::UNKNOWN_EXTENT);
                    break;
                }
            case DELETE:
                {
                    if (wc_key_fields->empty()) {
                        // no sort key, so need to handle non-primary key by using the entire row
                        auto tuple = std::make_shared<FieldTuple>(wc_fields, &row);
                        LOG_DEBUG(LOG_COMMITTER, "DELETE value={}", tuple->to_string());
                        table->remove(tuple, constant::UNKNOWN_EXTENT);
                    } else {
                        auto tuple = std::make_shared<FieldTuple>(wc_key_fields, &row);
                        LOG_DEBUG(LOG_COMMITTER, "DELETE value={}", tuple->to_string());
                        table->remove(tuple, constant::UNKNOWN_EXTENT);
                    }
                    break;
                }

            case TRUNCATE:
                {
                    LOG_DEBUG(LOG_COMMITTER, "TRUNCATE");
                    // note: this should always be the first operation within an extent
                    table->truncate();
                    break;
                }
            default:
                {
                    LOG_ERROR("Invalid operation: {}", op);
                    CHECK(false);
                }
            }
        }
    }

}  // namespace springtail::gc
