#include <thread>
#include <sys/time.h>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>
#include <common/coordinator.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_copy_table.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_redis_xact.hh>
#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/pg_log_recovery.hh>
#include <pg_log_mgr/sync_tracker.hh>

namespace springtail::pg_log_mgr {

    PgLogMgr::PgLogMgr(uint64_t db_id,
                       const std::filesystem::path &repl_log_path,
                       const std::filesystem::path &xact_log_path,
                       const std::string &host, const std::string &db_name,
                       const std::string &user_name, const std::string &password,
                       const std::string &pub_name, const std::string &slot_name,
                       uint64_t log_size_rollover_threshold,
                       int port,
                       bool archive_logs,
                       std::shared_ptr<ConcurrentQueue<committer::XidReady>> committer_queue,
                       std::shared_ptr<ConcurrentQueue<std::string>> index_reconciliation_queue,
                       std::shared_ptr<committer::Committer> committer)
    : _db_id(db_id), _db_instance_id(Properties::get_db_instance_id()),
      _host(host), _db_name(db_name), _user_name(user_name),
      _password(password), _pub_name(pub_name), _slot_name(slot_name),
      _log_size_rollover_threshold(log_size_rollover_threshold), _port(port),
      _pg_conn(_port, _host, _db_name, _user_name, _password, _pub_name, _slot_name),
      _repl_log_path(repl_log_path),
      _committer_queue(committer_queue),
      _xact_log_path(xact_log_path),
      _redis_sync_queue(fmt::format(redis::QUEUE_SYNC_TABLES, _db_instance_id, _db_id)),
      _index_reconciliation_queue(index_reconciliation_queue),
      _committer(committer)
    {
        _pg_log_reader = std::make_shared<PgLogReader>(_db_id, QUEUE_SIZE, repl_log_path, xact_log_path, _committer_queue, archive_logs);

        // construct the callback for watching for database state changes
        _cache_watcher_db_states = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR,"Replicated database state change; path: {}, state: {}",
                    path, new_value.dump(4));
                CHECK(path.starts_with(Properties::DATABASE_STATE_PATH));

                // extract database id
                std::vector<std::string> path_parts;
                common::split_string("/", path, path_parts);
                CHECK_EQ(path_parts.size(), 2);
                uint64_t db_id = stoull(path_parts[1]);
                CHECK_EQ(db_id, _db_id);

                // if we ever get in here, this means that this database will be deleted
                if (new_value.type() == nlohmann::json::value_t::null) {
                    return;
                }

                // extract state and handle state change
                CHECK(new_value.type() == nlohmann::json::value_t::string);
                std::string state_str = new_value.get<std::string>();
                redis::db_state_change::DBState state = redis::db_state_change::db_state_map[state_str];

                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received state change: {}", redis::db_state_change::db_state_to_name[state]);
                _handle_external_state_change(state);
            }
        );
    }


    void
    PgLogMgr::startup()
    {
        // get db state from Redis, default to RUNNING
        std::string state;
        try {
            state = Properties::get_db_state(_db_id);
        } catch (RedisNotFoundError &e) {
            // db state not found, assume initial state
            state = redis::db_state_change::REDIS_STATE_RUNNING;
            Properties::set_db_state(_db_id, state);
        }

        // reset state if we were stuck in syncing or copy tables
        if (state == redis::db_state_change::REDIS_STATE_SYNCING) {
            // reset state to running; GC will move us back to syncing
            state = redis::db_state_change::REDIS_STATE_RUNNING;
            Properties::set_db_state(_db_id, state);
        } else if (state == redis::db_state_change::REDIS_STATE_COPY_TABLES) {
            // we were in copy tables state, reset to initialize to restart copy tables
            state = redis::db_state_change::REDIS_STATE_INITIALIZE;
            Properties::set_db_state(_db_id, state);
        } else if (state == redis::db_state_change::REDIS_STATE_FAILED) {
            SPDLOG_ERROR("Database in failed state, cannot start up, db_id={}", _db_id);
            return;
        }

        // add redis cache callback for watching database state changes
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(
            std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(_db_id),
            _cache_watcher_db_states);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting up: DB state: {}", state);

        // need to add back table sync worker items to redis sync queue and clear the queue
        _redis_sync_queue.abort(REDIS_WORKER_ID);
        _redis_sync_queue.clear();

        // fetch latest xid from xid mgr
        XidMgrClient *xid_mgr = XidMgrClient::get_instance();
        uint64_t next_xid = xid_mgr->get_committed_xid(_db_id, 0) + 1;
        _pg_log_reader->set_next_xid(next_xid);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Last committed XID: {}", next_xid-1);

        // XXX currently we perform full recovery any time that the state is not INITIALIZE, but if
        //     we had a clean shutdown mechanism, we could start up without any recovery
        uint64_t lsn = INVALID_LSN;
        bool do_init = (state == redis::db_state_change::REDIS_STATE_INITIALIZE);
        PgLogRecovery recovery(_db_id, _repl_log_path, _xact_log_path, _pg_log_reader, next_xid - 1);
        if (do_init) {
            _startup_init();
        } else {
            lsn = recovery.repair_logs();
            _startup_running();
        }

        // initiate table copy thread; do this before we start replaying the log since it's needed
        // for table re-syncs that might have to be run
        _table_copy_thread = std::thread(&PgLogMgr::_copy_thread, this);

        // start the index reconciliation thread
        _reconciliation_thread = std::thread(&PgLogMgr::_index_reconciliation_thread, this);

        // start the log reader thread since it is also used to process recovery messages
        _reader_thread = std::thread(&PgLogMgr::_log_reader_thread, this);

        // note: we wait to perform these actions until the log reader has been started
        if (!do_init) {
            // perform the any required log recovery here
            recovery.replay_logs();
        }

        // system is ready to start streaming
        _start_streaming(lsn, do_init);
    }

    void
    PgLogMgr::_startup_running()
    {
        SPDLOG_INFO("Starting up from the RUNNING state.");

        // clear out any incomplete table syncs
        _redis_sync_queue.abort(REDIS_WORKER_ID);
        _redis_sync_queue.clear();

        // set state to running
        Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_RUNNING);

        // set internal state to running
        _internal_state.set(STATE_RUNNING);
    }

    void
    PgLogMgr::_startup_init()
    {
        // remove all files in log directories
        std::filesystem::remove_all(_repl_log_path);
        std::filesystem::remove_all(_xact_log_path);

        // create directories if they don't exist
        std::filesystem::create_directories(_repl_log_path);
        std::filesystem::create_directories(_xact_log_path);

        // set state to copy tables
        Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_COPY_TABLES);

        // set internal state to copy tables
        _internal_state.set(STATE_STARTUP_SYNC);
    }

    void
    PgLogMgr::_handle_external_state_change(const redis::db_state_change::DBState new_state)
    {
        StateEnum internal_state = _internal_state.get();

        // we only care about running state changes
        if (new_state != redis::db_state_change::DB_STATE_RUNNING) {
            return;
        }

        // new state is running, check internal state
        if (internal_state == STATE_RUNNING) {
            // already in running state
            return;
        }

        // if the new state is running, then we should have been in the replay done state
        // otherwise ignore the state change
        if (internal_state == STATE_SYNC_STALL ||
            internal_state == STATE_STARTUP_SYNC) {
            // if in replaying, ignore message will switch to running when replay is done
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received state change to running from replaying");
            return;
        }

        // if in replay done set to running XXX
        _internal_state.test_and_set(STATE_REPLAYING, STATE_RUNNING);
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Current state is running: {}", (_internal_state.get() == STATE_RUNNING));
    }

    void
    PgLogMgr::_index_reconciliation_thread()
    {
        while (!_shutdown) {
            // block on index reconciliation queue w/timeout for shutdown
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Waiting for index reconciliation request");
            if (auto request = _index_reconciliation_queue->pop(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT); request) {
                //Pass it to log reader to notify committer
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Request received for index reconciliation for {}", *request);
                auto msg = std::make_shared<PgMsg>(PgMsgEnum::RECONCILE_INDEX);
                _pg_log_reader->enqueue_msg(msg);
            }
        }
    }

    void
    PgLogMgr::_copy_thread()
    {
        // check initial state on thread startup
        // if in startup_sync state then switch to syncing
        if (_internal_state.is(STATE_STARTUP_SYNC)) {
            // Create the namespaces before starting the copy thread
            auto xid = _pg_log_reader->get_next_xid();
            auto token_init = logging::set_context_variables({{"db_id", std::to_string(_db_id)}, {"xid", std::to_string(xid)}});

            PgCopyTable::create_namespaces(_db_id, xid);

            _do_table_copies();
        }

        while (!_shutdown) {
            std::set<uint32_t> table_ids;

            // block on redis table sync queue w/timeout for shutdown
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Waiting for table sync queue");
            auto request = _redis_sync_queue.pop(REDIS_WORKER_ID, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (request == nullptr) {
                continue; // timeout, check for shutdown
            }

            do {
                // populate the tables to copy; check for more work
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table sync queue: {}@{}:{}", request->table_id(),
                                    request->xid().xid, request->xid().lsn);
                table_ids.insert(request->table_id());
                SyncTracker::get_instance()->mark_inflight(_db_id, request->table_id(), request->xid()); // shift from resyncing to inflight
                _committer->execute_on_resync(_db_id, request->table_id());

                request = _redis_sync_queue.try_pop(REDIS_WORKER_ID);
            } while (request != nullptr);

            CHECK(!table_ids.empty());

            auto token_commit_worker = logging::set_context_variables({{"db_id", std::to_string(_db_id)}});
            // copy tables
            _do_table_copies(table_ids);

            // update redis state
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Committing table sync queue");
            _redis_sync_queue.commit(REDIS_WORKER_ID);
        }
    }

    void
    PgLogMgr::_do_table_copies(std::optional<std::set<uint32_t>> table_ids)
    {
        // set state to sync stall, make sure we are in the running or startup sync state first
        // XXX blocked here if we get a second table sync while one is in-flight
        _internal_state.wait_and_set({STATE_RUNNING, STATE_STARTUP_SYNC, STATE_REPLAYING},
                                     STATE_SYNC_STALL);

        // notify xact handler to rollover log
        _notify_xact_start_sync();

        // set db state to syncing
        Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_SYNCING);

        // wait for pipeline stall to complete
        _internal_state.wait_for_state(STATE_SYNCING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Copying tables; state=synchronizing");

        // copy tables
        std::vector<PgCopyResultPtr> res;
        auto xid = _pg_log_reader->get_next_xid();

        auto token = logging::set_context_variables({{"xid", std::to_string(xid)}});
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Copying tables; target xid={}", xid);
        if (table_ids.has_value()) {
            res = PgCopyTable::copy_tables(_db_id, xid, table_ids.value());
        } else {
            res = PgCopyTable::copy_db(_db_id, xid);
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table copy done; res size={}", res.size());
        if (res.size() > 0) {
            // process copy results
            _process_copy_results(res);
        } else {
            // no tables copied
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "No tables copied; setting state=running");
            // set to running this unblocks the xact handler
            _internal_state.set(STATE_RUNNING);
            Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_RUNNING);
        }
    }


    void
    PgLogMgr::_notify_xact_start_sync()
    {
        // push a message onto xact handler's queue to stall the pipeline
        assert(_internal_state.is(STATE_SYNC_STALL));
        _logger_queue.push_stall();
    }


    void
    PgLogMgr::_process_copy_results(const std::vector<PgCopyResultPtr> &res)
    {
        assert(_internal_state.is(STATE_SYNCING));

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Pushing copy results to sync tracker");

        for (const auto &r : res) {
            // skip the result if it contains no tables
            if (r->tids.empty()) {
                continue;
            }

            // send table sync message to GC
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Recording table sync msgs: target_xid={}", r->target_xid);
            PgXactMsg redis_xact(_db_id, r);

            bool sync_start = SyncTracker::get_instance()->add_sync(std::get<pg_log_mgr::PgXactMsg::TableSyncMsg>(redis_xact.msg));

            // notify the Committer to stop committing XIDs
            if (sync_start) {
                _committer_queue->push(std::make_shared<committer::XidReady>(_db_id));
            }
        }

        // process stalled messages; set state to replaying
        _internal_state.set(STATE_REPLAYING);
        _internal_state.wait_for_state(STATE_RUNNING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table copy done; state=replaying");
    }

    void
    PgLogMgr::_start_streaming(uint64_t lsn, bool do_init)
    {
        try {
            _pg_conn.connect();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Connecting to postgres server: {}\n", _host);

            // create slot if need be
            bool create_slot = !_pg_conn.check_slot_exists();

            if (create_slot) {
                if (do_init) {
                    SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Creating replication slot: {}\n", _slot_name);
                    lsn = _pg_conn.create_replication_slot();
                } else {
                    SPDLOG_ERROR("Replication slot does not exist: db_id={}, slot={}", _db_id, _slot_name);
                    // shutdown
                    Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_FAILED);
                    return;
                }
            }

            // get the protocol version
            _proto_version = _pg_conn.get_protocol_version();

            // start steaming
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting streaming: lsn={}", lsn);
            _pg_conn.start_streaming(lsn, do_init);
        } catch (const PgConnectionError &e) {
            // this may be recoverable if we can reconnect, but not handled right now
            SPDLOG_ERROR("Connection error starting streaming in db_id={}: {}, setting state to failed",
                         _db_id, e.what());
            // shutdown
            Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_FAILED);
            return;
        } catch (const PgUnrecoverableError &e) {
            SPDLOG_ERROR("Unrecoverable Error starting streaming in db_id={}: {}, setting state to failed",
                         _db_id, e.what());
            // shutdown
            Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_FAILED);
            return;
        }

        // create the worker threads
        _writer_thread = std::thread(&PgLogMgr::_log_writer_thread, this);
    }

    /** Thread for writing log data */
    void
    PgLogMgr::_log_writer_thread()
    {
        PgLogWriterPtr logger = _create_repl_logger();

        PgCopyData data;
        uint64_t start_offset = logger->offset();

        auto coordinator = Coordinator::get_instance();
        std::string coordinator_id = fmt::format(WRITER_WORKER_ID, _db_id);

        coordinator->register_thread(Coordinator::DaemonType::LOG_MGR, coordinator_id);

        while (!_shutdown) {
            // mark alive with coordinator
            coordinator->mark_alive(Coordinator::DaemonType::LOG_MGR, coordinator_id);

            // read data from pg replication connection (blocks)
            try {
                // wait for data from pg; true if data is available
                if (!_pg_conn.wait_for_data(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT)) {
                    continue;
                }

                // read data from pg, length will be 0 if no data (timeout)
                _pg_conn.read_data(data);
                if (data.length == 0) {
                    continue;
                }

            } catch (const PgIOShutdown &e) {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received shutdown signal");
                break;
            } catch (const PgConnectionError &e) {
                SPDLOG_ERROR("Error reading data from pg: {}", e.what());
                // try reconnecting
                try {
                    _pg_conn.reconnect();
                } catch (const PgConnectionError &e) {
                    SPDLOG_ERROR("Error reconnecting to pg: {}", e.what());
                    // shutdown
                    PgLogCoordinator::get_instance()->shutdown();
                    break;
                }
            }

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Recevied data: length={}, msg_length={}, msg_offset={}",
                         data.length, data.msg_length, data.msg_offset);

            if (!logger->log_data(data)) {
                // data has been consumed by keep alive or not full message
                continue;
            }

            // push data to queue, if data message is complete then record start/end offsets
            uint64_t end_offset = logger->offset();

            // record start/end offsets for this message
            _logger_queue.push(start_offset, end_offset, logger->filename());
            start_offset = end_offset;

            // check to see if we should rollover log
            if (end_offset > _log_size_rollover_threshold) {
                logger->close();
                logger = _create_repl_logger();
                start_offset = 0;
            }
        }

        // shutdown; close logger
        logger->close();

        // shutdown queues queue
        _logger_queue.shutdown();

        // shutdown the pg connection
        _pg_conn.close();
    }

    /** Thread for reading log data that is written from writer */
    void
    PgLogMgr::_log_reader_thread()
    {
        auto coordinator = Coordinator::get_instance();
        std::string coordinator_id = fmt::format(READER_WORKER_ID, _db_id);

        coordinator->register_thread(Coordinator::DaemonType::LOG_MGR, coordinator_id);

        while (!_shutdown) {
            // mark alive with coordinator
            coordinator->mark_alive(Coordinator::DaemonType::LOG_MGR, coordinator_id);

            // get log entry from queue
            PgLogQueueEntryPtr log_entry = this->_logger_queue.pop(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (log_entry == nullptr) {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Timeout waiting for log entry");
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Got log entry: path={}, start_offset={}, num_messages={}",
                                log_entry->path, log_entry->start_offset, log_entry->num_messages);

            // check for stall message, if so then wait for sync to complete
            if (log_entry->is_stall_message) {
                assert (_internal_state.is(STATE_SYNC_STALL));
                // wait for sync to complete
                _internal_state.set(STATE_SYNCING);
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Waiting for sync to complete");
                _internal_state.wait_for_state({ STATE_REPLAYING, STATE_RUNNING });
                _internal_state.set(STATE_RUNNING);
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Sync to complete");
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Processing log entry: path={}, start_offset={}, num_messages={}",
                                log_entry->path, log_entry->start_offset, log_entry->num_messages);

            _pg_log_reader->process_log(log_entry->path, _logger_file_timestamp.load(),
                                        log_entry->start_offset, log_entry->num_messages);
        }
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Exiting log reader thread");
    }

    PgLogWriterPtr
    PgLogMgr::_create_repl_logger()
    {
        std::filesystem::path file = fs::create_log_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        auto file_timestamp = fs::extract_timestamp_from_file(file, LOG_PREFIX_REPL, LOG_SUFFIX);
        DCHECK(file_timestamp.has_value());
        _logger_file_timestamp.store(file_timestamp.value());

        return std::make_shared<PgLogWriter>(file,
            [this](LSN_t lsn) { _pg_conn.set_last_flushed_LSN(lsn); });
    }

} // namespace springtail::pg_log_mgr
