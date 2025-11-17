#include <common/time_trace.hh>
#include <pg_log_mgr/committer.hh>
#include <redis/db_state_change.hh>
#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include <pg_ext/extn_registry.hh>

namespace springtail::committer {

    // ---------------- BatchState implementation ----------------
    std::pair<MutableTablePtr, bool>
    Committer::BatchState::get_or_create_table(
        uint64_t tid,
        uint64_t db_id,
        uint64_t completed_xid,
        const ExtensionCallback& extension_callback)
    {
        std::unique_lock lock(_mutex);

        auto table_it = _table_cache.find(tid);
        if (table_it != _table_cache.end()) {
            return {table_it->second, false};
        }

        // Create new mutable table with target_xid set to final_xid
        CHECK_GT(_final_xid, 0);
        auto table = TableMgr::get_instance()->get_mutable_table(db_id, tid, completed_xid, _final_xid, extension_callback);

        // Initialize write cache schema once for this table (performance optimization)
        table->initialize_wc_schema(extension_callback);

        _table_cache[tid] = table;
        return {table, true};
    }

    uint64_t
    Committer::BatchState::get_final_xid() const
    {
        std::unique_lock lock(_mutex);
        return _final_xid;
    }

    void
    Committer::BatchState::set_final_xid(uint64_t xid)
    {
        std::unique_lock lock(_mutex);
        _final_xid = xid;
    }

    void
    Committer::BatchState::update_xid_metadata(uint64_t xid, const WriteCacheTableSet::Metadata& md)
    {
        std::unique_lock lock(_mutex);
        auto it = _xid_metadata.find(xid);
        if (it == _xid_metadata.end()) {
            _xid_metadata[xid] = md;
        } else if (md.pg_commit_ts < it->second.pg_commit_ts) {
            _xid_metadata[xid] = md;
        }
    }

    void
    Committer::BatchState::add_xid_result(const std::shared_ptr<XidReady>& result)
    {
        std::unique_lock lock(_mutex);
        _xid_results.push_back(result);
    }

    void
    Committer::BatchState::clear_table_cache()
    {
        std::unique_lock lock(_mutex);
        _table_cache.clear();
    }

    std::map<uint64_t, MutableTablePtr>&
    Committer::BatchState::table_cache()
    {
        return _table_cache;
    }

    std::vector<std::shared_ptr<XidReady>>&
    Committer::BatchState::xid_results()
    {
        return _xid_results;
    }

    std::map<uint64_t, WriteCacheTableSet::Metadata>&
    Committer::BatchState::xid_metadata()
    {
        return _xid_metadata;
    }

    // ---------------- Committer implementation ----------------

    bool
    _index_exists(uint64_t db_id, uint64_t tid, uint64_t index_id, uint64_t xid)
    {
        auto meta = sys_tbl_mgr::Server::get_instance()->get_roots(db_id, tid, xid);
        DCHECK(meta != nullptr);
        auto it =
            std::ranges::find_if(meta->roots, [&](auto const &v) { return index_id == v.index_id; });
        return it != meta->roots.end();
    }

    void
    Committer::remove_db(uint64_t db_id)
    {
        _indexer->remove_db(db_id);
        std::unique_lock lock(_main_mutex);
        _completed_xids.erase(db_id);
    }

    void
    Committer::run()
    {
        // perform cleanup for any Committer threads in a previous run
        cleanup();

        // use the same worker count for Indexer
        _indexer = std::make_unique<Indexer>(_indexer_worker_count, _index_reconciliation_queue_mgr);
        // we use a fraction of the committer workers for table sync processing
        // we don't want to starve the committer workers with too many fsync tasks
        _table_sync_processor = std::make_unique<TableSyncProcessor>(_fsync_interval, _fsync_worker_count);

        auto coordinator = Coordinator::get_instance();
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;

        // register the thread on startup
        coordinator->register_thread(daemon_type, "committer");

        // initiate the worker threads
        for (int i = 0; i < _worker_count; i++) {
            std::string thread_name = fmt::format("CommitterW_{}", i);
            _worker_threads.emplace_back(&Committer::_run_worker, this, i);
            pthread_setname_np(_worker_threads.back().native_handle(), thread_name.c_str());
        }

        // XXX we are currently processing XIDs one at a time, but we should eventually bundle
        //     together XID ranges when possible.  It makes sense to do this when the FDW nodes can
        //     do their own roll-forward.
        // XXX we could also process the XIDs from different databases within the instance in parallel

        // enter a loop polling for data from the write cache
        while (!_shutdown) {
            // update the coordinator
            auto &keep_alive = coordinator->find_thread(daemon_type, "committer");
            Coordinator::mark_alive(keep_alive);

            // figure out if there are XIDs to process
            // note: this is a blocking call that will timeout after keep_alive secs
            auto results = _committer_queue->pop_all(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (results.empty()) {
                continue; // got a timeout, try again
            }

            // clear batch state from previous iteration
            {
                std::unique_lock lock(_batch_state_mutex);
                _batch_state.clear();
            }

            std::unique_lock lock(_main_mutex);

            // process all messages, grouping by db_id and handling special cases
            // use iterator-based loop to allow peeking ahead for batch boundaries
            for (auto it = results.begin(); it != results.end(); ++it) {
                auto result = *it;

                uint64_t db_id = result->db();

                // Process index recovery first as this message doesnt require xid
                // or timestamp processing for xact_log
                if (result->type() == XidReady::Type::INDEX_RECOVERY_TRIGGER) {
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Initiate indexes recovery: {}", db_id);
                    _indexer->recover_indexes(db_id);
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Indexes recovery initiated: {}", db_id);
                    continue;
                }

                auto token_1 = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});

                // if the message isn't an XACT then make sure we've done a commit
                if (result->type() != XidReady::Type::XACT_MSG) {
                    // commit any pending batch for this database before blocking
                    std::shared_ptr<BatchState> batch;
                    {
                        std::unique_lock batch_lock(_batch_state_mutex);
                        auto batch_it = _batch_state.find(db_id);
                        if (batch_it != _batch_state.end()) {
                            batch = batch_it->second;
                            _batch_state.erase(batch_it);
                        }
                    }
                    if (batch) {
                        uint64_t completed_xid = _completed_xids[db_id];
                        _commit_batch(db_id, batch, completed_xid);
                    }
                }

                // handle a TABLE_SYNC_START - commit any pending batch for this db first
                if (result->type() == XidReady::Type::TABLE_SYNC_START) {
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Stop committing due to table sync: {}", db_id);

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

                auto token_2 = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"xid", std::to_string(completed_xid)}});
                LOG_INFO("Last completed XID: {}@{}", db_id, completed_xid);

                // handle a TABLE_SYNC_COMMIT or TABLE_SYNC_SWAP
                // note: no need to commit batch since SYNC_START already blocked commits
                if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT ||
                    result->type() == XidReady::Type::TABLE_SYNC_SWAP) {

                    CHECK_GT(result->swap().xid(), completed_xid);
                    _handle_table_sync_message(result, db_id);
                    continue;
                }

                // handle RECONCILE_INDEX - process in isolation
                if (result->type() == XidReady::Type::RECONCILE_INDEX) {
                    _handle_index_reconciliation(result, db_id, completed_xid);
                    continue;
                }

                // handle XACT_MSG - process mutations and accumulate in batch
                // Check if this is the first XACT_MSG in a new batch (final_xid not yet set)
                if (result->type() == XidReady::Type::XACT_MSG) {
                    std::shared_ptr<BatchState> batch;
                    {
                        std::unique_lock batch_lock(_batch_state_mutex);
                        auto batch_it = _batch_state.find(db_id);
                        if (batch_it == _batch_state.end()) {
                            // Create new batch state for this database
                            batch = std::make_shared<BatchState>();
                            _batch_state[db_id] = batch;
                        } else {
                            batch = batch_it->second;
                        }
                    }

                    // If final_xid is not set, this is the start of a new batch - scan ahead
                    if (batch->get_final_xid() == 0) {
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "First XACT_MSG in batch for db {}, scanning ahead for final_xids", db_id);

                        // Scan from current position to find final_xid for all databases in this batch
                        auto final_xids = _scan_batch_final_xids(it, results.end());

                        // Set final_xid for all databases found in the scan
                        for (auto& [scan_db_id, final_xid] : final_xids) {
                            std::shared_ptr<BatchState> scan_batch;
                            {
                                std::unique_lock batch_lock(_batch_state_mutex);
                                auto scan_it = _batch_state.find(scan_db_id);
                                if (scan_it == _batch_state.end()) {
                                    scan_batch = std::make_shared<BatchState>();
                                    _batch_state[scan_db_id] = scan_batch;
                                } else {
                                    scan_batch = scan_it->second;
                                }
                            }
                            scan_batch->set_final_xid(final_xid);
                        }
                    }
                }

                _handle_transaction_message(result, db_id, completed_xid);
            }

            // commit all accumulated batches
            std::vector<std::pair<uint64_t, std::shared_ptr<BatchState>>> batches_to_commit;
            {
                std::unique_lock batch_lock(_batch_state_mutex);
                for (auto& [db_id, batch] : _batch_state) {
                    batches_to_commit.emplace_back(db_id, batch);
                }
            }
            for (auto& [db_id, batch] : batches_to_commit) {
                uint64_t completed_xid = _completed_xids[db_id];
                _commit_batch(db_id, batch, completed_xid);
            }
        }

        // join all of the worker threads
        for (auto &thread : _worker_threads) {
            thread.join();
        }

        // unregister the thread on shutdown
        coordinator->unregister_thread(daemon_type, "committer");

        _indexer.reset();
        _table_sync_processor.reset();

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Committer shutdown");
    }

    std::map<uint64_t, uint64_t>
    Committer::_scan_batch_final_xids(
        std::deque<std::shared_ptr<XidReady>>::iterator start_it,
        std::deque<std::shared_ptr<XidReady>>::iterator end_it)
    {
        std::map<uint64_t, uint64_t> final_xids;

        for (auto it = start_it; it != end_it; ++it) {
            auto& result = *it;

            // Skip INDEX_RECOVERY_TRIGGER as it doesn't participate in batching
            if (result->type() == XidReady::Type::INDEX_RECOVERY_TRIGGER) {
                continue;
            }

            // Stop at batch boundary (non-XACT_MSG)
            if (result->type() != XidReady::Type::XACT_MSG) {
                break;
            }

            // Track last XACT_MSG per database - this becomes final_xid for that db
            uint64_t db_id = result->db();
            uint64_t xid = result->xact().xid();
            final_xids[db_id] = xid;
        }

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Scanned batch: found {} databases", final_xids.size());
#if DEBUG
        for (auto& [db_id, final_xid] : final_xids) {
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "  db {} final_xid {}", db_id, final_xid);
        }
#endif

        return final_xids;
    }

    void
    Committer::_commit_batch(
        uint64_t db_id,
        std::shared_ptr<BatchState> batch,
        uint64_t completed_xid)
    {
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Committing batch for db {} with {} XIDs",
                  db_id, batch->xid_results().size());

        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;
        Coordinator::get_instance()->unregister_thread(daemon_type, "committer");

        uint64_t final_xid = batch->get_final_xid();

        // finalize all tables in the batch
        // Note: table_cache() is not thread-safe, but safe here since all workers are done
        std::vector<MutableTablePtr> tables_to_sync;
        for (auto& [tid, table] : batch->table_cache()) {
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Finalizing table {}", tid);
            auto metadata = table->finalize(false);
            sys_tbl_mgr::Server::get_instance()->update_roots(db_id, tid, final_xid, metadata);
            tables_to_sync.push_back(table);
        }

        Coordinator::get_instance()->register_thread(daemon_type, "committer");

        // process each XID in the batch
        // Note: xid_results() is not thread-safe, but safe here since all workers are done
        for (auto& result : batch->xid_results()) {
            CHECK(result->type() == XidReady::Type::XACT_MSG);
            uint64_t xid = result->xact().xid();
            uint64_t pg_xid = result->xact().pg_xid();

            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Processing batch XID: {}@{}", db_id, xid);

            // get DDL changes for this XID
            nlohmann::json completed_ddls = RedisDDL::get_instance()->get_ddls_xid(db_id, xid);

            if (!completed_ddls.is_null()) {
                // pre-commit the DDLs to be applied to the FDWs
                RedisDDL::get_instance()->precommit_ddl(db_id, xid, completed_ddls);
                _has_ddl_precommit = true;
            }

            // check if we are doing an active table sync, in which case we have to block commits
            bool is_last_xid = (xid == final_xid);

            if (!_block_commit.contains(db_id)) {
                // only finalize system metadata once at the end
                if (is_last_xid) {
                    // finalize the system metadata
                    // note: we do this even without DDL changes to ensure the primary and secondary
                    //       index root offsets are written to disk
                    // we will fsync system table along with the user tables in the table sync processor:w
                    sys_tbl_mgr::Server::get_instance()->finalize(db_id, xid, batch->table_cache().empty() == true);
                }

                // Check and notify vacuumer about dropped tables
                if (!completed_ddls.is_null()) {
                    _expire_table_drops(db_id, completed_ddls, xid);
                }

                // Sync expired extents on the XID with vacuum
                Vacuumer::get_instance()->commit_expired_extents(db_id, xid);

                // use record_mapping for intermediate XIDs, commit_xid only for last
                if (is_last_xid) {
                    if (batch->table_cache().empty()) {
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "No table mutations in batch for db {} xid: {}", db_id, xid);
                        xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, pg_xid, xid, !completed_ddls.is_null(),
                                result->timestamp(), result->get_tracker());
                    } else {
                        // commit the completed XID without xlog update because table data has not been persisted (fsync).
                        xid_mgr::XidMgrServer::get_instance()->commit_xid_no_xlog(db_id, pg_xid, xid, !completed_ddls.is_null(), true,
                                result->timestamp(), result->get_tracker());
                        _table_sync_processor->add(final_xid, std::move(tables_to_sync));
                    }

                    // push completed DDL changes to the FDWs
                    if (_has_ddl_precommit) {
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Commit DDL changes db {} xid {}", db_id, xid);
                        RedisDDL::get_instance()->commit_ddl(db_id, xid);
                        _has_ddl_precommit = false;
                    }
                } else {
                    // for intermediate XIDs, just record the mapping
                    xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, pg_xid, xid, !completed_ddls.is_null(),
                            result->timestamp(), result->get_tracker());
                }
            } else {
                if (is_last_xid) {
                    if (batch->table_cache().empty()) {
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "No table mutations in batch for db {} xid: {}", db_id, xid);
                        // don't commit, but record any DDL changes to the history
                        xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, pg_xid, xid, !completed_ddls.is_null(),
                                result->timestamp(), result->get_tracker());
                    } else {
                        // commit the completed XID without xlog update
                        xid_mgr::XidMgrServer::get_instance()->commit_xid_no_xlog(db_id, pg_xid, xid, !completed_ddls.is_null(), false,
                                result->timestamp(), result->get_tracker());
                        _table_sync_processor->add(final_xid, std::move(tables_to_sync));
                    }
                } else {
                    // don't commit, but record any DDL changes to the history
                    xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, pg_xid, xid, !completed_ddls.is_null(),
                            result->timestamp(), result->get_tracker());
                }
            }

            _completed_xids[db_id] = xid;

            // evict from write cache
            WriteCacheServer::get_instance()->evict_xid(db_id, xid);

            // Record per-transaction latency metrics for this XID
            // Note: xid_metadata() is not thread-safe, but safe here since all workers are done
            auto metadata_it = batch->xid_metadata().find(xid);
            if (metadata_it != batch->xid_metadata().end()) {
                auto now = std::chrono::steady_clock::now();
                auto finalize_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - metadata_it->second.local_commit_ts);
                auto transaction_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - metadata_it->second.local_begin_ts);

                open_telemetry::OpenTelemetry::get_instance()->record_histogram(WRITE_CACHE_FINALIZE_LATENCIES, finalize_duration.count());
                open_telemetry::OpenTelemetry::get_instance()->record_histogram(TRANSACTION_LATENCIES, transaction_duration.count());

                LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG2, "XID {} latencies: transaction={}ms, finalize={}ms",
                        xid, transaction_duration.count(), finalize_duration.count());
            }

            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "XID completed: {}@{}", db_id, xid);
        }

        // Collect all index requests across all XIDs in the batch
        std::list<proto::IndexProcessRequest> combined_index_requests;
        for (auto& result : batch->xid_results()) {
            uint64_t xid = result->xact().xid();

            auto index_requests = _index_requests_mgr->get_index_requests(db_id, xid);
            if (!index_requests.empty()) {
                // Append to combined list
                combined_index_requests.splice(combined_index_requests.end(), index_requests);
            }
        }

        // Process all index requests once at the final XID
        if (!combined_index_requests.empty()) {
            auto final_xid = _completed_xids[db_id];
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Processing {} index requests for batch at final_xid {}",
                      combined_index_requests.size(), final_xid);

            // Handle all index operations at final_xid
            _indexer->process_requests(db_id, final_xid, combined_index_requests);

            // Check and notify vacuumer about dropped indexes (use final_xid for all)
            _expire_index_drops(db_id, combined_index_requests, final_xid);
        }

        // clear the table cache for this batch
        batch->clear_table_cache();
    }

    void
    Committer::_handle_index_reconciliation(
        const std::shared_ptr<XidReady>& result,
        uint64_t db_id,
        uint64_t& completed_xid)
    {
        CHECK(result->type() == XidReady::Type::RECONCILE_INDEX);
        uint64_t xid = result->reconcile().xid();

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1,
                  "Handling RECONCILE_INDEX in isolation: {}@{}", db_id, xid);

        // Step 1: Process index reconciliation (no table mutations for reconciliation XIDs)
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"xid", std::to_string(xid)}});
        LOG_INFO("Process RECONCILE_INDEX XID: {}@{}", db_id, xid);
        assert(xid > completed_xid);

        // Trigger index reconciliation
        _indexer->process_index_reconciliation(db_id, result->reconcile().reconcile_xid(), xid);

        // Step 2: Finalize and commit the reconciliation XID
        if (!_block_commit.contains(db_id)) {
            // Finalize system metadata after index reconciliation
            sys_tbl_mgr::Server::get_instance()->finalize(db_id, xid, true);

            // Commit the reconciliation XID (no DDL changes for reconciliation)
            xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, 0, xid, false, result->timestamp());
        } else {
            // Record mapping if commits are blocked
            xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, 0, xid, false, result->timestamp(), nullptr);
        }

        _completed_xids[db_id] = xid;

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "RECONCILE_INDEX XID completed: {}@{}", db_id, xid);

        // Update completed_xid for caller
        completed_xid = xid;
    }

    void
    Committer::_handle_table_sync_message(const std::shared_ptr<XidReady>& result,
                                          uint64_t db_id)
    {
        uint64_t completed_xid = result->swap().xid();
        nlohmann::json ddls = result->swap().ddls();
        auto swapped_tids = result->swap().tids();

        LOG_DEBUG(
            LOG_COMMITTER, LOG_LEVEL_DEBUG1,
            "Handle a TABLE_SYNC_SWAP/COMMIT: {}, {}, request xid @{}",
            static_cast<char>(result->type()), db_id, completed_xid);

        auto token_3 = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"xid", std::to_string(completed_xid)}});

        // Table sync should always have DDL operations
        CHECK(!ddls.is_null()) << "TABLE_SYNC should have DDL operations";

        // Invalidate schema cache for tables with DDL changes during table sync
        _invalidate_systbl_cache(db_id, ddls);

        // pre-commit the DDLs in case there's a failure
        RedisDDL::get_instance()->precommit_ddl(db_id, completed_xid, ddls);
        _has_ddl_precommit = true;

        if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
            // finalize the system metadata
            sys_tbl_mgr::Server::get_instance()->finalize(db_id, completed_xid, true);

            // perform a commit to the XidMgr
            xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, 0, completed_xid, true, result->timestamp());

            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Commit DDL changes db {} xid {}", db_id, completed_xid);
            // notify the FDW of the schema changes
            if (_has_ddl_precommit) {
                RedisDDL::get_instance()->commit_ddl(db_id, completed_xid);
                _has_ddl_precommit = false;
            }

            for (const auto swapped_tid: swapped_tids) {
                // Notify vacuumer to expire old table snapshot
                // Send completed_xid - 1 to get the previous old snapshot dir
                // and then expire that at the completed_xid
                auto swapped_table_old_dir = TableMgr::get_instance()->get_table_data_dir(db_id, swapped_tid, completed_xid - 1);
                if (swapped_table_old_dir.has_value()) {
                    Vacuumer::get_instance()->expire_snapshot(db_id, swapped_table_old_dir.value(), completed_xid);
                }
            }
        } else {
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Record DDL changes db {} xid {}", db_id, completed_xid);
            xid_mgr::XidMgrServer::get_instance()->record_mapping(db_id, 0, completed_xid, true, result->timestamp(), nullptr);
        }
        _completed_xids[db_id] = completed_xid;
        WriteCacheServer::get_instance()->evict_xid(db_id, completed_xid);

        if (result->type() == XidReady::Type::TABLE_SYNC_COMMIT) {
            // notify everyone that the database is now in the "ready" state
            redis::db_state_change::set_db_state(db_id,
                                                 redis::db_state_change::DB_STATE_RUNNING);

            // allow commits on future XIDs
            _block_commit.erase(db_id);
        }
    }

    void
    Committer::_handle_transaction_message(const std::shared_ptr<XidReady>& result,
                                           uint64_t db_id,
                                           uint64_t completed_xid)
    {
        // note: from here we know we have an XACT_MSG
        CHECK(result->type() == XidReady::Type::XACT_MSG);
        uint64_t xid = result->xact().xid();

        auto token_4 = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"xid", std::to_string(xid)}});
        LOG_INFO("Process XID: {}@{}", db_id, xid);
        assert(xid > completed_xid);

        // accumulate this XID in the batch state
        std::shared_ptr<BatchState> batch;
        {
            std::unique_lock lock(_batch_state_mutex);
            batch = _batch_state[db_id];
        }
        batch->add_xid_result(result);
        // Note: final_xid is set when the first XACT_MSG of a batch is encountered in run()

        // check if there were DDL mutations as part of this txn, invalidate the schema cache
        // accordingly
        nlohmann::json completed_ddls = RedisDDL::get_instance()->get_ddls_xid(db_id, xid);
        if (!completed_ddls.is_null()) {
            _invalidate_systbl_cache(db_id, completed_ddls);
        }

        // find every table associated with this XID
        uint64_t table_cursor = 0;
        bool tid_done = false;
        while (!tid_done) {
            // query the write cache for the tables modified through this XID
            auto table_list = WriteCacheServer::get_instance()->list_tables(db_id, xid, 100, table_cursor);

            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Got {} tables from the write cache", table_list.size());

            // check if we are done processing this XID
            if (table_list.empty()) {
                tid_done = true;
                break;
            }

            for (auto tid : table_list) {
                LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Pass table {} to a worker", tid);
                // mark this table as in-flight
                {
                    boost::unique_lock lock(_mutex);
                    _tid_set.emplace(tid);
                }

                // pass each table to a worker thread to process it's mutations
                auto entry = std::make_shared<WorkerEntry>(db_id, tid, completed_xid, xid);
                _worker_queue.push(entry);
            }

            // update the coordinator
            constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;
            auto &keep_alive = Coordinator::get_instance()->find_thread(daemon_type, "committer");
            Coordinator::mark_alive(keep_alive);
        }

        // wait for tables to complete their processing
        // Note: finalization (finalize, update_roots, commit) is now deferred until _commit_batch()
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Wait for {} tables to complete", _tid_set.size());
        {
            boost::unique_lock lock(_mutex);
            while (!_cv.wait_for(lock, boost::chrono::seconds(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT),
                                 [this]() { return _tid_set.empty(); })) {
                constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;
                auto &keep_alive = Coordinator::get_instance()->find_thread(daemon_type, "committer");
                Coordinator::mark_alive(keep_alive); // update the coordinator
            }
        }
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "All table processing complete for XID {}", xid);
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
                auto &&precommit = RedisDDL::get_instance()->get_precommit_ddl();

                for (const auto &entry : precommit) {
                    uint64_t commit_xid = xid_mgr::XidMgrServer::get_instance()->get_committed_xid(entry.first, 0);

                    if (entry.second <= commit_xid) {
                        // for those that are <= the committed XID, commit them
                        RedisDDL::get_instance()->commit_ddl(entry.first, entry.second);
                    } else {
                        // for those that are > the committed XID, abort them
                        RedisDDL::get_instance()->abort_ddl(entry.first, entry.second);
                    }
                }
            }
        }

        // unregister all parser threads from the previous run
        coordinator->unregister_threads(daemon_type, cleanup_threads);
    }

    void
    Committer::_expire_index_drops(uint64_t db_id, std::list<proto::IndexProcessRequest>& index_requests, uint64_t committed_xid)
    {
        for (auto const& index_request: index_requests) {
            auto action = index_request.action();
            if (action == "drop_index") {
                auto tid = index_request.index().table_id();
                auto index_id = index_request.index().id();
                auto _dropped_index_table_dir = TableMgr::get_instance()->get_table_data_dir(db_id, tid, committed_xid - 1);
                if (_dropped_index_table_dir.has_value()) {
                    auto index_file_path = _dropped_index_table_dir.value() / fmt::format(constant::INDEX_FILE, index_id);
                    Vacuumer::get_instance()->expire_snapshot(db_id, index_file_path, committed_xid);
                }
            }
        }
    }

    void
    Committer::_expire_table_drops(uint64_t db_id, const nlohmann::json &completed_ddls, uint64_t committed_xid)
    {
        for (auto& ddl: completed_ddls) {
            if (ddl.contains("tid") && ddl.contains("action")) {
                uint64_t tid = ddl["tid"].get<uint64_t>();
                auto action = ddl["action"].get<std::string>();
                if (action == "drop") {
                    auto dropped_table_dir = TableMgr::get_instance()->get_table_data_dir(db_id, tid, committed_xid - 1);
                    if (dropped_table_dir.has_value()) {
                        Vacuumer::get_instance()->expire_snapshot(db_id, dropped_table_dir.value(), committed_xid);
                    }
                }
            }
        }
    }

    void
    Committer::_invalidate_systbl_cache(uint64_t db, const nlohmann::json &completed_ddls)
    {
        auto server = sys_tbl_mgr::Server::get_instance();
        for (auto ddl : completed_ddls) {
            if (!ddl.contains("tid")) {
                continue; // mutation doesn't reference a specific table
            }

            uint64_t tid = ddl["tid"].get<uint64_t>();
            XidLsn ddl_xid(ddl["xid"].get<uint64_t>(), ddl["lsn"].get<uint64_t>());
            server->invalidate_table(db, tid, ddl_xid);

            // Invalidate the extent schema cache for this table
            TableMgr::get_instance()->record_schema_change(db, tid, ddl_xid);
        }
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
            open_telemetry::OpenTelemetry::get_instance()->record_histogram(COMMITTER_QUEUE_SIZE, _worker_queue.size());

            // update the coordinator
            auto &keep_alive = coordinator->find_thread(daemon_type, worker_id);
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
            _process_table(entry->db_id, entry->tid, entry->completed_xid, entry->xid, worker_id);

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
                              uint64_t xid,
                              const std::string &thread_name)
    {
        // find the coordinator keep-alive
        constexpr auto daemon_type = Coordinator::DaemonType::GC_MGR;
        auto &keep_alive = Coordinator::get_instance()->find_thread(daemon_type, thread_name);

        // Get the batch state for this database
        std::shared_ptr<BatchState> batch;
        {
            std::unique_lock lock(_batch_state_mutex);
            batch = _batch_state[db_id];
        }

        uint64_t final_xid = batch->get_final_xid();

        if (!sys_tbl_mgr::Server::get_instance()->exists(db_id, tid, XidLsn{xid}) ||
            !sys_tbl_mgr::Server::get_instance()->exists(db_id, tid, XidLsn{final_xid})) {
            // This could happen if the table is dropped in the same transaction
            // BEGIN/INSERT/DROP/COMMIT
            // TODO: another way to handle the case would be to drop the table mutation
            // records from the Batch object in the log reader. Marking this as TODO
            // just to keep the question open for now.
            LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "The table doesn't exist at access/target XID: {} @ {} || {}", tid, xid, final_xid);
            return;
        }

        // get or create the mutable table object from batch cache
        ExtensionCallback extension_callback = {PgExtnRegistry::get_instance()->comparator_func};
        auto [table, is_new] = batch->get_or_create_table(tid, db_id, completed_xid, extension_callback);

        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "{} table {} for batch (target_xid={})",
                  is_new ? "Created new" : "Reusing", tid, final_xid);

        // retrieve extents and apply the mutations to them
        uint64_t extent_cursor = 0;
        std::optional<WriteCacheTableSet::Metadata> min_md;
        time_trace::Trace process_extents_trace;
        TIME_TRACE_START(process_extents_trace);

        while (true) {
            // XXX would be better if we could perform an async prefetch to reduce IO latency
            WriteCacheTableSet::Metadata md;
            auto &&extent_list = WriteCacheServer::get_instance()->get_extents(db_id, tid, xid, 1000, extent_cursor, md);

            // if we didn't receive any extents then we're done
            if (extent_list.empty()) {
                break;
            }

            // Track the earliest metadata (first extent processed)
            if (!min_md || md.pg_commit_ts < min_md->pg_commit_ts) {
                min_md = md;
            }


            // process each extent of ordered mutations
            for (auto &wc_extent : extent_list) {

                // update the coordinator
                Coordinator::mark_alive(keep_alive);

                // process the extent
                _process_extent(db_id, tid, table, wc_extent);
            }
        }
        TIME_TRACE_STOP(process_extents_trace);
        TIME_TRACESET_UPDATE(time_trace::traces, fmt::format("process_extents-xid_{}", xid), process_extents_trace);

        // Note: finalize() and update_roots() are now deferred until _commit_batch()
        // This allows us to batch multiple XIDs' mutations to the same table before finalizing

        // Update the batch's per-XID metadata (used for per-transaction metrics in _commit_batch)
        // Track the earliest metadata for this XID across all its tables
        if (min_md) {
            batch->update_xid_metadata(xid, *min_md);
        }

    }

    void
    Committer::_process_extent(uint64_t db_id,
                               uint64_t tid,
                               MutableTablePtr table,
                               const std::shared_ptr<springtail::WriteCacheIndexExtent> wc_extent)
    {
        // Get pre-computed schema and fields from table (already initialized in _process_table)
        auto wc_schema = table->wc_schema();
        auto op_f = table->wc_op_field();
        auto wc_fields = table->wc_fields();
        auto wc_key_fields = table->wc_key_fields();
        auto actual_table_fields = table->actual_table_fields();

        time_trace::Trace process_extent_trace;
        TIME_TRACE_START(process_extent_trace);

        // Get the extent from the write cache index
        XidLsn xid(wc_extent->xid, wc_extent->xid_seq);
        Extent extent(*wc_extent->data);
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "xid={} rows={}", xid.xid, extent.row_count());

        // To add internal_row_id as part of INSERTS. Other mutations will have
        // internal_row_id as part of wc_extent as-is
        auto internal_row_id_field = std::make_shared<FieldArray>(1);

        TIME_TRACE_STOP(process_extent_trace);
        TIME_TRACESET_UPDATE(time_trace::traces, fmt::format("committer_write_extent-xid_{}", xid.xid), process_extent_trace);

        // XXX We know that these operations are sorted in key + LSN order, so we should be
        //     able to perform a more efficient merge using hints.  For a large extent we
        //     could parallelize the mutations.  The one exception is a table truncation,
        //     which must always appear first in a batch (although not necessarily first in
        //     the transaction).
        for (auto &row : extent) {
            time_trace::Trace process_row_trace;
            TIME_TRACE_START(process_row_trace);
            uint8_t op = op_f->get_uint8(&row);
            switch (op) {
            case INSERT:
                {
                    (*internal_row_id_field)[0] = std::make_shared<ConstTypeField<uint64_t>>(table->get_next_internal_row_id());
                    auto tuple = std::make_shared<KeyValueTuple>(actual_table_fields, internal_row_id_field, &row);
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "INSERT value={}", tuple->to_string());
                    table->insert(tuple, constant::UNKNOWN_EXTENT);
                    break;
                }
            case UPDATE:
                {
                    auto tuple = std::make_shared<FieldTuple>(wc_fields, &row);
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "UPDATE value={}", tuple->to_string());
                    table->update(tuple, constant::UNKNOWN_EXTENT);
                    break;
                }
            case DELETE:
                {
                    if (wc_key_fields->empty()) {
                        // no sort key, so need to handle non-primary key by using the entire row
                        auto tuple = std::make_shared<FieldTuple>(actual_table_fields, &row);
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "DELETE value={}", tuple->to_string());
                        table->remove(tuple, constant::UNKNOWN_EXTENT);
                    } else {
                        auto tuple = std::make_shared<FieldTuple>(wc_key_fields, &row);
                        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "DELETE value={}", tuple->to_string());
                        table->remove(tuple, constant::UNKNOWN_EXTENT);
                    }
                    break;
                }

            case TRUNCATE:
                {
                    LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "TRUNCATE");
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
            TIME_TRACE_STOP(process_row_trace);
            TIME_TRACESET_UPDATE(time_trace::traces, fmt::format("process_row-xid_{}", xid.xid), process_row_trace);
        }
    }

    // ---------------- TableSyncProcessor ----------------
    void 
    Committer::TableSyncProcessor::add(uint64_t xid, const std::vector<MutableTablePtr>& tables)
    {
        DCHECK(!tables.empty());

        std::unique_lock g(_m);

        auto db_id = tables[0]->db();
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "TableSyncProcessor adding work item for db {} xid {} with {} tables",
                db_id, xid, tables.size());

        auto& item = _work_map[db_id];
        item.xid = xid;

        for (const auto& table : tables) {
            auto files = table->get_table_files();
            for (auto const& file : files) {
                item.files.emplace(file);
            }
        }
    }

    void 
    Committer::TableSyncProcessor::task(std::stop_token st)
    {
        LOG_DEBUG(LOG_COMMITTER, LOG_LEVEL_DEBUG1, "Thread started, interval: {}ms",
                std::chrono::duration_cast<std::chrono::milliseconds>(_sync_interval).count()); 

        while(!st.stop_requested()) {
            uint64_t db_id;
            WorkItem item;
            {
                std::unique_lock lock(_m);
                // wait for timeout or stop request
                _cv.wait_for(lock, st, _sync_interval, [this]{ return !_work_map.empty(); });
                if (st.stop_requested()) {
                    break;
                }

                if (_work_map.empty()) {
                    continue;
                }

                auto db_it = _work_map.begin();
                db_id = db_it->first;
                item = std::move(db_it->second);
                _work_map.erase(db_it);
            }

            for (auto const& file : item.files) {
                if (st.stop_requested()) {
                    break;
                }
                if (std::filesystem::is_directory(file)) {
                    auto fd = ::open(file.c_str(), O_RDONLY | O_DIRECTORY);
                    CHECK(fd != -1) << "Failed to open directory " << file << ", error: " << strerror(errno);
                    ::fsync(fd);
                    ::close(fd);
                    continue;
                }

                auto handle = IOMgr::get_instance()->open(file, IOMgr::IO_MODE::APPEND, true);
                handle->sync();
            }

            if (!st.stop_requested()) {
                // sync the system tables
                sys_tbl_mgr::Server::get_instance()->sync(db_id, item.xid);

                // all files  have been fsync'ed for this xid, update xlog
                xid_mgr::XidMgrServer::get_instance()->commit_xlog(db_id, item.xid);

                // evict expired schema cache entries now that this XID is fully committed
                TableMgr::get_instance()->on_xid_committed(item.xid);
            }
        }
        LOG_INFO("thread joined");
    }

}  // namespace springtail::gc
