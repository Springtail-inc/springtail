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
#include <pg_log_mgr/sync_tracker.hh>

namespace springtail::pg_log_mgr {

    PgLogMgr::PgLogMgr(uint64_t db_id,
        const std::filesystem::path &repl_log_path,
        const std::filesystem::path &xact_log_path,
        const std::string &host, const std::string &db_name,
        const std::string &user_name, const std::string &password,
        const std::string &pub_name, const std::string &slot_name,
        int port)
            : _db_id(db_id), _db_instance_id(Properties::get_db_instance_id()),
            _host(host), _db_name(db_name), _user_name(user_name),
            _password(password), _pub_name(pub_name), _slot_name(slot_name), _port(port),
            _pg_conn(_port, _host, _db_name, _user_name, _password, _pub_name, _slot_name),
            _repl_log_path(repl_log_path),
            _xact_queue(std::make_shared<ConcurrentQueue<PgTransaction>>()),
            _pg_log_reader(db_id, _xact_queue), _xact_log_path(xact_log_path),
            _redis_sync_queue(fmt::format(redis::QUEUE_SYNC_TABLES, _db_instance_id, _db_id))
    {
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

                // extract state
                CHECK(new_value.type() == nlohmann::json::value_t::string);
                std::string state_str = new_value.get<std::string>();
                redis::db_state_change::DBState state = redis::db_state_change::db_state_map[state_str];

                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received state change: {}", redis::db_state_change::db_state_to_name[state]);
                _handle_external_state_change(state);
            }
        );
    }


    // XXX make sure GC is shutdown -- assume it for now
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

        // reset state if we were stuck in syncing
        if (state == redis::db_state_change::REDIS_STATE_SYNCING) {
            // XXX need to handle, not sure whether to reset to running or initialize
            assert(false);
        }

        // add redis cache callback for watching database state changes
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(
            std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(_db_id),
            _cache_watcher_db_states);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting up: DB state: {}", state);

        // fetch latest xid from xid mgr
        XidMgrClient *xid_mgr = XidMgrClient::get_instance();
        uint64_t next_xid = xid_mgr->get_committed_xid(_db_id, 0) + 1;
        _pg_log_reader.set_next_xid(next_xid);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Last committed XID: {}", next_xid-1);

        uint64_t lsn = INVALID_LSN;
        bool do_recovery = (state != redis::db_state_change::REDIS_STATE_INITIALIZE);
        if (!do_recovery) {
            _startup_init();
        } else {
            lsn = _startup_running();
        }

        // initiate table copy thread; do this before we start streaming
        _table_copy_thread = std::thread(&PgLogMgr::_copy_thread, this);

        // perform the actual log recovery here
        if (do_recovery) {
            _perform_log_recovery();
        }

        // start streaming
        _start_streaming(lsn);
    }

    uint64_t
    PgLogMgr::_startup_running()
    {
        SPDLOG_INFO("Starting up from the RUNNING state.");

        uint64_t lsn = INVALID_LSN;

        // create directories if they don't exist
        // XXX shouldn't these always exist if we're starting up from running?  Maybe we should skip
        //     recovery if the directories didn't exist?
        std::filesystem::create_directories(_repl_log_path);
        std::filesystem::create_directories(_xact_log_path);

        // scan latest replication log and extract ending LSN
        // if we find an empty log then remove file and go to previous log
        // if last message is truncated (not complete); truncate log file ignoring that message
        std::filesystem::path latest_log = fs::find_latest_modified_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        if (!latest_log.empty()) {
            lsn = PgMsgStreamReader::scan_log(latest_log, true);
        }

        // clear out any incomplete table syncs
        _redis_sync_queue.abort(REDIS_WORKER_ID);
        _redis_sync_queue.clear();

        // set state to running
        Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_RUNNING);

        // set internal state to running
        _internal_state.set(STATE_RUNNING);

        // perform recovery from the logs, replaying any transactions that didn't get committed
        _perform_log_recovery();

        //// Replay xact logs
        // 1) open the repl log and the xact log for reading
        std::filesystem::path repl_log = fs::find_earliest_modified_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        PgMsgStreamReader repl_reader(repl_log);
        PgXactLogReader xact_reader(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX);

        // 2) scan the repl log for any begin/commit/abort messages
        bool done = false;
        std::vector<char> filter = {pg_msg::MSG_BEGIN, pg_msg::MSG_COMMIT, pg_msg::MSG_STREAM_START,
                                    pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT};

        struct Position {
            uint32_t log_number;
            uint64_t offset;
            std::filesystem::path file;

            Position(uint32_t ln, uint64_t o, const std::filesystem::path &f)
                : log_number(ln), offset(o), file(f)
            { }

            std::strong_ordering operator<=>(const Position &rhs) {
                return std::tie(log_number, offset) <=> std::tie(rhs.log_number, rhs.offset);
            }
        };

        std::unordered_map<uint32_t, Position> active_pgxid;
        uint32_t log_number = 0;
        uint32_t first_uncommitted_pgxid = 0;
        while (!done) {
            bool eob, eos;
            auto msg = repl_reader.read_message(filter, eob, eos);

            switch (msg->msg_type) {
                    // a) when a begin is seen, record it's position into the active set as a
                    // possible
                    //    starting point for the scan along with pgxid
                case pg_msg::MSG_BEGIN: {
                    auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
                    Position p(log_number, repl_reader.header_offset(), repl_log);
                    active_pgxid.try_emplace({ begin_msg.xid, p });
                    break;
                }
                case pg_msg::MSG_STREAM_START: {
                    auto &start_msg = std::get<PgMsgStreamStart>(msg->msg);
                    if (start_msg.first) {
                        Position p(log_number, repl_reader.header_offset(), repl_log);
                        active_pgxid.try_emplace({ start_msg.xid, p });
                    }
                    break;
                }

                    // b) when an abort is seen, remove the pgxid from the active set
                case pg_msg::MSG_STREAM_ABORT: {
                    auto &abort_msg = std::get<PgMsgStreamAbort>(msg->msg);
                    if (abort_msg.xid == abort_msg.sub_xid) {
                        active_pgxid.erase(abort_msg.xid);
                    }
                    break;
                }

                    // c) when a commit is seen, check for it in the xact log
                    //    i)   if the xact log is empty, start the replay step
                    //    ii)  if the pgxid doesn't match the next entry, there's some kind of error
                    //         -- should never happen, but we could roll back the committed XID to
                    //         the XID prior to this one and replay?
                    //    iii) if the XID is <= the committed XID, remove from the active set
                    //    iv)  if the XID is > the committed XID, start the replay step
                case pg_msg::MSG_COMMIT:
                case pg_msg::MSG_STREAM_COMMIT: {
                    uint32_t pgxid;
                    if (msg->msg_type == pg_msg::MSG_COMMIT) {
                        auto &commit_msg = std::get<PgMsgCommit>(msg->msg);
                        pgxid = commit_msg.xid;
                    } else {
                        auto &commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                        pgxid = commit_msg.xid;
                    }                        
                    CHECK_EQ(pgxid, xact_log.get_pg_xid());

                    if (xact_log.get_xid() <= _committed_xid) {
                        active_pgxid.erase(pgxid);
                        done = !xact_log.next();
                    } else {
                        first_uncommitted_pgxid = pgxid;
                        done = true;
                    }
                    break;
                }
            }

            // check if we need to move to the next replication log file
            if (!done && eos) {
                repl_log = fs::get_next_file(last_xact->commit_path, LOG_PREFIX_REPL, LOG_SUFFIX);
                ++log_number;
                repl_reader.set_file(repl_log);
            }
        }

        // 3) in the replay step, we replay all records from pgxids in the active set, as well as
        //    any pgxids that are beyond the committed XID or recorded XID from the xact log.
        //    During this phase the PgLogReader needs to skip any assigned XIDs prior to the
        //    committed XID.

        // If the active_pgxid is empty, then we can jump directly to replaying everything remaining
        // in the repl_log.  Otherwise, we need to re-process all of the in-flight active xacts.
        if (!active_pgxid.empty()) {
            auto min_i = std::min_element(active_pgxid.begin(), active_pgxid.end(),
                                          [](const std::pair<uint32_t, Position>& lhs,
                                             const std::pair<uint32_t, Position>& rhs) {
                                              return lhs.second < rhs.second;
                                          });
            CHECK(min_i != active_pgxid.end());

            uint64_t start_offset = min_i->second.offset;
            repl_log = min_i->second.file;
            repl_reader.set_file(repl_log, start_offset);

            // replay repl log entries for the active_pgxid set... skip everything else until the
            // first_uncommitted_pgxid is committed
            while (!done) {
                auto msg = repl_reader.read_message(filter, eos, eob);
                // XXXXXX handle the message, skipping any messages for xacts that aren't active
            }
        }

        // now process all remaining messages out of the repl_log
        while (!done) {
            auto msg = repl_reader.read_message(filter, eos, eob);

            // XXXXXX push the message into the pg_log_reader queue
        }

        return lsn;
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

        // clear out Redis
        // Table sync queue
        _redis_sync_queue.clear();

        _internal_state.set(STATE_STARTUP_SYNC);

        // perform the table copies during initialization
        _do_table_copies();
    }

    void
    PgLogMgr::_handle_external_state_change(const redis::db_state_change::DBState new_state)
    {
        StateEnum internal_state = _internal_state.get();

        if (new_state == redis::db_state_change::DB_STATE_RUNNING) {
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
    }

    void
    PgLogMgr::_copy_thread()
    {

        // check initial state on thread startup
        // if in startup_sync state then switch to syncing
        if (_internal_state.is(STATE_STARTUP_SYNC)) {
            // Create the namespaces before starting the copy thread
            auto xid = _pg_log_reader.get_next_xid();
            PgCopyTable::create_namespaces(_db_id, xid);

            _do_table_copies();
        }

        while (!_shutdown) {
            std::vector<uint32_t> table_ids;

            // block on redis table sync queue w/timeout for shutdown
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Waiting for table sync queue");
            StringPtr table_id_ptr = _redis_sync_queue.pop(REDIS_WORKER_ID, constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (table_id_ptr == nullptr) {
                continue; // timeout, check for shutdown
            }

            // populate the tables to copy; check for more work
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table sync queue: {}", table_id_ptr->c_str());
            table_ids.push_back(strtol(table_id_ptr->c_str(), nullptr, 10));
            while (_redis_sync_queue.size() > 0) {
                table_id_ptr = _redis_sync_queue.pop(REDIS_WORKER_ID, 1);
                if (table_id_ptr == nullptr) {
                    break;
                }
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table sync queue: {}", table_id_ptr->c_str());
                table_ids.push_back(strtol(table_id_ptr->c_str(), nullptr, 10));
            }

            if (table_ids.size() == 0) {
                continue;
            }

            // copy tables
            _do_table_copies(table_ids);

            // update redis state
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Committing table sync queue");
            _redis_sync_queue.commit(REDIS_WORKER_ID);
        }
    }

    void
    PgLogMgr::_do_table_copies(std::optional<std::vector<uint32_t>> table_ids)
    {
        // set state to sync stall, make sure we are in the running or startup sync state first
        _internal_state.wait_and_set({STATE_RUNNING, STATE_STARTUP_SYNC}, STATE_SYNC_STALL);

        // notify xact handler to rollover log
        _notify_xact_start_sync();

        // set db state to syncing
        Properties::set_db_state(_db_id, redis::db_state_change::REDIS_STATE_SYNCING);

        // wait for pipeline stall to complete
        _internal_state.wait_for_state(STATE_SYNCING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Copying tables; state=synchronizing");

        // copy tables
        std::vector<PgCopyResultPtr> res;
        auto xid = _pg_log_reader.get_next_xid();
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
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();

        assert(_internal_state.is(STATE_SYNCING));

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Pushing copy results to redis");

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
                RedisQueue<gc::XidReady>
                    committer_queue(fmt::format(redis::QUEUE_GC_XID_READY,
                                                Properties::get_db_instance_id()));
                committer_queue.push(gc::XidReady(_db_id));
            }
        }

        // process stalled messages; set state to replaying
        _internal_state.set(STATE_REPLAYING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table copy done; state=replaying");
    }

    void
    PgLogMgr::_start_streaming(uint64_t lsn)
    {
        _pg_conn.connect();
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Connecting to postgres server: {}\n", _host);

        // create slot if need be
        bool create_slot = !_pg_conn.check_slot_exists();

        if (create_slot) {
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Creating replication slot: {}\n", _slot_name);
            _pg_conn.create_replication_slot(false,  // export
                                             false); // temporary
        }

        // get the protocol version
        _proto_version = _pg_conn.get_protocol_version();

        // start steaming
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting streaming: lsn={}", lsn);
        _pg_conn.start_streaming(lsn);

        // create the worker threads
        _writer_thread = std::thread(&PgLogMgr::_log_writer_thread, this);
        _reader_thread = std::thread(&PgLogMgr::_log_reader_thread, this);
    }

    void
    PgLogMgr::_lsn_callback(LSN_t lsn)
    {
        _pg_conn.set_last_flushed_LSN(lsn);
    }

    /** Thread for writing log data */
    void
    PgLogMgr::_log_writer_thread()
    {
        PgLogWriterPtr logger = this->_create_repl_logger();

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
            if (end_offset > LOG_ROLLOVER_SIZE_BYTES) {
                logger->close();
                logger = this->_create_repl_logger();
                start_offset = 0;
            }
        }

        // shutdown; close logger
        logger->close();

        // shutdown queues queue
        _logger_queue.shutdown();
        _xact_queue->shutdown();

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
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Sync to complete");
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Processing log entry: path={}, start_offset={}, num_messages={}",
                                log_entry->path, log_entry->start_offset, log_entry->num_messages);

            _pg_log_reader.process_log(log_entry->path, log_entry->start_offset,
                                       log_entry->num_messages);
        }
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Exiting log reader thread");
    }

    PgLogWriterPtr
    PgLogMgr::_create_repl_logger()
    {
        std::filesystem::path file = fs::find_latest_modified_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        if (file.empty()) {
            file = _repl_log_path / fmt::format("{}{:04d}{}", LOG_PREFIX_REPL, 0, LOG_SUFFIX);
        } else {
            file = fs::get_next_file(file, LOG_PREFIX_REPL, LOG_SUFFIX);
        }

        return std::make_shared<PgLogWriter>(file,
            [this](LSN_t lsn) { _pg_conn.set_last_flushed_LSN(lsn); });
    }

    PgXactLogWriterPtr
    PgLogMgr::_create_xact_logger()
    {
        if (_xact_logger != nullptr) {
            _xact_logger->close();
        }

        std::filesystem::path file = fs::find_latest_modified_file(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX);
        if (file.empty()) {
            file = _xact_log_path / fmt::format("{}{:04d}{}", LOG_PREFIX_XACT, 0, LOG_SUFFIX);
        } else {
            file = fs::get_next_file(file, LOG_PREFIX_XACT, LOG_SUFFIX);
        }

        _xact_logger = std::make_shared<PgXactLogWriter>(file);

        return _xact_logger;
    }

    /** Thread for handling transactions from log reader */
    void
    PgLogMgr::_xact_handler_thread()
    {
        _create_xact_logger(); ///< logger to write out xid log

        auto coordinator = Coordinator::get_instance();
        std::string coordinator_id = fmt::format(XACT_WORKER_ID, _db_id);

        coordinator->register_thread(Coordinator::DaemonType::LOG_MGR, coordinator_id);

        while (!_shutdown) {

            // mark alive with coordinator
            coordinator->mark_alive(Coordinator::DaemonType::LOG_MGR, coordinator_id);

            PgTransactionPtr xact = _xact_queue->pop(constant::COORDINATOR_KEEP_ALIVE_TIMEOUT);
            if (xact == nullptr) {
                // timeout
                continue;
            }

            // process the transaction
            _process_xact(xact);

            // check to see if we should rollover log
            if (_xact_logger->size() > LOG_ROLLOVER_SIZE_BYTES) {
                _create_xact_logger();
            }
        }

        // shutdown, close xact logger
        _xact_logger->close();
    }

    void
    PgLogMgr::_process_xact(const PgTransactionPtr xact)
    {
        // if stream start, just log it
        if (xact->type == PgTransaction::TYPE_STREAM_START ||
            xact->type == PgTransaction::TYPE_STREAM_ABORT) {
            _xact_logger->log_stream_msg(xact);
            return;
        }

        assert (xact->type == PgTransaction::TYPE_COMMIT);

        // check if we've already seen this xact, e.g., during replay
        if (xact->xact_lsn <= _last_pushed_lsn) {
            SPDLOG_WARN("Skipping xact (already seen): xact_lsn={}, last_pushed_lsn={}",
                         xact->xact_lsn, _last_pushed_lsn);
            return;
        }

        // log the xact
        _xact_logger->log_commit(xact);
    }

} // namespace springtail::pg_log_mgr
