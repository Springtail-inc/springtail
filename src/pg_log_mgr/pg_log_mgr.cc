#include <thread>
#include <sys/time.h>

#include <chrono>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/properties.hh>
#include <common/logging.hh>
#include <common/redis.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_copy_table.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_redis_xact.hh>
#include <pg_log_mgr/pg_log_coordinator.hh>

namespace springtail::pg_log_mgr {

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
            state = redis::REDIS_STATE_RUNNING;
            Properties::set_db_state(_db_id, state);
        }

        // reset state if we were stuck in syncing
        if (state == redis::REDIS_STATE_SYNCING) {
            // XXX need to handle, not sure whether to reset to running or initialize
            assert(false);
        }

        // clear out GC redis queue
        _redis_queue.clear();

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting up: DB state: {}", state);

        // fetch latest xid from xid mgr
        XidMgrClient *xid_mgr = XidMgrClient::get_instance();
        _next_xid = xid_mgr->get_committed_xid(_db_id, 0) + 1;

        uint64_t lsn = INVALID_LSN;
        if (state == redis::REDIS_STATE_INITIALIZE) {
            _startup_init();
        } else {
            lsn = _startup_running();
        }

        // initiate the pubsub thread; do this before copy thread
        _pubsub_thread = std::thread(&PgLogMgr::_redis_pubsub_thread, this);

        // initiate table copy thread; do this before we start streaming
        _table_copy_thread = std::thread(&PgLogMgr::_copy_thread, this);

        // start streaming
        _start_streaming(lsn);
    }

    uint64_t
    PgLogMgr::_startup_running()
    {
        uint64_t lsn = INVALID_LSN;

        // create directories if they don't exist
        std::filesystem::create_directories(_repl_log_path);
        std::filesystem::create_directories(_xact_log_path);

        // scan latest replication log and extract ending LSN
        // if we find an empty log then remove file and go to previous log
        // if last message is truncated (not complete); truncate log file ignoring that message
        std::filesystem::path latest_log = fs::find_latest_modified_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        if (!latest_log.empty()) {
            lsn = PgMsgStreamReader::scan_log(latest_log, true);
        }

        //// Replay xact logs

        // scan xact logs and transfer in progress xacts to log reader
        // fetch xaction list from xact reader, and add them to Redis
        // these are transactions with springtail XIDs that are > than last committed XID
        PgTransactionPtr last_xact = nullptr;
        PgXactLogReader xact_reader(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX, _next_xid-1);
        xact_reader.begin();
        while (true) {
            // go through log and fetch batches of transactions to send to redis
            std::vector<PgTransactionPtr> committed_xacts;
            int num_xacts = xact_reader.next(MAX_REDIS_BATCH_SIZE, committed_xacts);
            if (num_xacts == 0) {
                break;
            }
            _push_xacts_to_redis(committed_xacts);
            last_xact = committed_xacts.back();
            committed_xacts.clear();
        }

        // update next xid if we find an xid higher than committed xid in the log
        uint64_t last_allocated_xid = xact_reader.get_max_sp_xid();
        if (last_allocated_xid > _next_xid) {
            _next_xid = last_allocated_xid + 1;
        }

        // move contents of stream map into pg log reader, invalidates stream map
        std::map<uint32_t, springtail::PgTransactionPtr> stream_map = xact_reader.get_stream_map();
        _pg_log_reader.set_xact_map(stream_map);

        // if xact log is behind pg log then we need to catchup before starting to stream
        // find last xact from xact_list, and start from there (file + offset)
        // inserts into xact queue
        if (last_xact != nullptr) {
            _pg_log_reader.process_log(last_xact->commit_path, last_xact->commit_offset, -1);
            std::filesystem::path next_log = fs::get_next_file(last_xact->commit_path, LOG_PREFIX_REPL, LOG_SUFFIX);

            // iterate and process xacts from next log file
            while (!next_log.empty() && std::filesystem::exists(next_log)) {
                _pg_log_reader.process_log(next_log, 0, -1);
                next_log = fs::get_next_file(next_log, LOG_PREFIX_REPL, LOG_SUFFIX);
            }
        }

        // need to add back table sync worker items to redis sync queue
        _redis_sync_queue.abort(REDIS_WORKER_ID);

        // set state to running
        Properties::set_db_state(_db_id, redis::REDIS_STATE_RUNNING);

        // set internal state to running
        _internal_state.set(STATE_RUNNING);

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
        // GC queue
        _redis_queue.clear();
        // Table sync queue
        _redis_sync_queue.clear();
        // Table sync pg xid hset
        RedisMgr::get_instance()->get_client()->del(_redis_sync_table);

        _internal_state.set(STATE_STARTUP_SYNC);
    }

    void
    PgLogMgr::_handle_external_state_change(const std::string &new_state)
    {
        StateEnum internal_state = _internal_state.get();

        if (new_state == redis::REDIS_STATE_RUNNING) {
            // if the new state is running, then we should have been in the replay done state
            if (internal_state == STATE_REPLAYING) {
                // if in replaying, wait for replay done before switching to running
                // XXX this blocks the pubsub thread until replay is done
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received state change to running from replaying");
                _internal_state.wait_and_set(STATE_REPLAY_DONE, STATE_RUNNING);
            }
            // if in replay done set to running
            _internal_state.test_and_set(STATE_REPLAY_DONE, STATE_RUNNING);
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Current state is running: {}", (_internal_state.get() == STATE_RUNNING));
        }
    }

    void
    PgLogMgr::_redis_pubsub_thread()
    {
        // create subscriber for redis pubsub, set timeout to 5 secs.
        RedisMgr::SubscriberPtr subscriber = RedisMgr::get_instance()->get_subscriber(5);

        // subscribe to the state change channel
        std::string state_change_channel = fmt::format(redis::PUBSUB_DB_STATE_CHANGES, _db_instance_id, _db_id);
        subscriber->subscribe(state_change_channel);

        subscriber->on_message([this, state_change_channel](const std::string &channel, const std::string &msg) {
            if (channel == state_change_channel) {
                SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Received state change: {}", msg);
                _handle_external_state_change(msg);
            }
        });

        while (!_shutdown) {
            try {
                // consume from subscriber, timeout is set above
                subscriber->consume();
            } catch (const sw::redis::TimeoutError &e) {
                // timeout, check for shutdown
                continue;
            } catch (const sw::redis::Error &e) {
                SPDLOG_ERROR("Error consuming from redis: {}\n", e.what());
                break;
            }
        }
    }

    void
    PgLogMgr::_copy_thread()
    {
        // check initial state on thread startup
        // if in startup_sync state then switch to syncing
        if (_internal_state.is(STATE_STARTUP_SYNC)) {
            _do_table_copies();
        }

        while (!_shutdown) {
            std::vector<uint32_t> table_ids;

            // block on redis table sync queue w/timeout for shutdown
            StringPtr table_id_ptr = _redis_sync_queue.pop(REDIS_WORKER_ID, 1);
            if (table_id_ptr == nullptr) {
                continue; // timeout, check for shutdown
            }

            // populate the tables to copy; check for more work
            table_ids.push_back(strtol(table_id_ptr->c_str(), nullptr, 10));
            while (_redis_sync_queue.size() > 0) {
                table_id_ptr = _redis_sync_queue.pop(REDIS_WORKER_ID, 1);
                if (table_id_ptr == nullptr) {
                    break;
                }
                table_ids.push_back(strtol(table_id_ptr->c_str(), nullptr, 10));
            }

            if (table_ids.size() == 0) {
                continue;
            }

            // copy tables
            _do_table_copies(table_ids);

            // update redis state
            _redis_sync_queue.commit(REDIS_WORKER_ID);
        }
    }

    void
    PgLogMgr::_do_table_copies(std::optional<std::vector<uint32_t>> table_ids)
    {
        // set state to sync stall
        _internal_state.set(STATE_SYNC_STALL);

        // notify xact handler to rollover log
        _notify_xact_start_sync();

        // set db state to syncing
        Properties::set_db_state(_db_id, redis::REDIS_STATE_SYNCING);

        // wait for pipeline stall to complete
        _internal_state.wait_for_state(STATE_SYNCING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Copying tables; state=synchronizing");

        // copy tables
        std::vector<PgCopyResultPtr> res;
        if (table_ids.has_value()) {
            res = PgCopyTable::copy_tables(_db_id, _get_next_xid(), table_ids.value());
        } else {
            res = PgCopyTable::copy_db(_db_id, _get_next_xid());
        }
        _process_copy_results(res);

        // replay xact logs (queued during stall)
        _replay_xact_logs();
    }


    void
    PgLogMgr::_notify_xact_start_sync()
    {
        // push a message onto xact handler's queue to stall the pipeline
        assert(_internal_state.is(STATE_SYNC_STALL));
        PgTransactionPtr xact = std::make_shared<PgTransaction>(PgTransaction::TYPE_PIPELINE_STALL);
        _xact_queue->push(xact);
    }


    void
    PgLogMgr::_process_copy_results(const std::vector<PgCopyResultPtr> &res)
    {
        RedisClientPtr redis = RedisMgr::get_instance()->get_client();

        assert(_internal_state.is(STATE_SYNCING));

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Pushing copy results to redis");


        for (const auto &r : res) {
            // go through result tids and update Redis with table state info
            for (const auto &tid : r->tids) {
                redis->hset(_redis_sync_table, std::to_string(tid), r->pg_xids);
            }

            // send table sync message to GC
            PgXactMsg redis_xact(_db_id, r);
            _redis_queue.push(redis_xact);
        }

        // push done message to redis GC queue
        PgXactMsg redis_xact2(_db_id);
        _redis_queue.push(redis_xact2);

        // process stalled messages; set state to replaying
        _internal_state.set(STATE_REPLAYING);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Table copy done; state=replaying");
    }


    void
    PgLogMgr::_replay_xact_logs()
    {
        assert(_internal_state.is(STATE_REPLAYING));

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Replaying xact logs");

        // replay xact log after a copy
        PgXactLogReader xact_reader(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX, 0);
        xact_reader.begin(_xact_sync_log_file);
        while (true) {
            // go through log and fetch batches of transactions to send to redis
            std::vector<PgTransactionPtr> committed_xacts;
            int num_xacts = xact_reader.next(MAX_REDIS_BATCH_SIZE, committed_xacts);
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Replaying xact logs: num_xacts={}", num_xacts);
            assert (num_xacts == committed_xacts.size());
            if (num_xacts == 0) {
                break;
            }
            _push_xacts_to_redis(committed_xacts);
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Replaying xact logs done");

        // rollover xact logger
        _create_xact_logger();

        // set state to replay done if we are in replaying state
        // this unblocks the xact handler
        _internal_state.set(STATE_REPLAY_DONE);

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Internal state set to: replay_done");
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
        _xact_thread = std::thread(&PgLogMgr::_xact_handler_thread, this);
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

        while (!_shutdown) {
            // read data from pg replication connection (blocks)
            try {
                _pg_conn.read_data(data);
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

            if (data.length == 0 || !logger->log_data(data)) {
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
        while (!_shutdown) {
            // get log entry from queue
            PgLogQueueEntryPtr log_entry = this->_logger_queue.pop();
            if (log_entry == nullptr) {
                continue;
            }

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Processing log entry: path={}, start_offset={}, num_messages={}",
                                log_entry->path, log_entry->start_offset, log_entry->num_messages);
            _pg_log_reader.process_log(log_entry->path, log_entry->start_offset,
                                       log_entry->num_messages);
        }
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

        while (!_shutdown) {
            PgTransactionPtr xact = _xact_queue->pop();
            if (xact == nullptr) {
                continue;
            }

            // if in sync stall state and we get a pipeline stall message
            // then rollover log and set state to syncing
            if (xact->type == PgTransaction::TYPE_PIPELINE_STALL &&
                _internal_state.is(STATE_SYNC_STALL))
            {
                // stall state, rollover log
                _create_xact_logger();

                // record in redis the log file
                RedisMgr::get_instance()->get_client()->set(fmt::format(redis::STRING_LOG_RESYNC, _db_instance_id, _db_id),
                                                            _xact_logger->file().string());

                _xact_sync_log_file = _xact_logger->file();

                _internal_state.set(STATE_SYNCING);

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
        StateEnum state = _internal_state.get();
        if (state == STATE_REPLAYING) {
            // replaying state, block further messages
            _internal_state.wait_for_state(STATE_REPLAY_DONE);
            // fall through to running state
        }

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

        // first allocate an xid for this xact
        uint64_t xid = _get_next_xid();

        // next log the data
        xact->springtail_xid = xid;
        _xact_logger->log_commit(xact);

        // block until we are in running state or replay done state
        if (!_internal_state.is(STATE_RUNNING) && !_internal_state.is(STATE_REPLAY_DONE)) {
            return;
        }

        // insert into redis queue
        _push_xact_to_redis(xact);
    }

    void
    PgLogMgr::_push_xact_to_redis(const PgTransactionPtr xact)
    {
        uint64_t xid = xact->springtail_xid;

        // find the correct RedisSortedSet
        RSSOidValuePtr oid_set = _get_oid_set(_db_id);

        // go through the oid map and update redis
        for (auto &oid : xact->oids) {
            oid_set->add(PgRedisOidValue(oid, xid), xid);
        }

        // finally send notification to GC
        PgXactMsg redis_xact(xact->begin_path, xact->commit_path,
                             _db_id, xact->begin_offset,
                             xact->commit_offset, xact->xact_lsn,
                             xid, xact->xid, xact->aborted_xids);

        // track last xact lsn we've pushed to redis
        assert (xact->xact_lsn > _last_pushed_lsn);
        _last_pushed_lsn = xact->xact_lsn;

        _redis_queue.push(redis_xact);
    }

    void
    PgLogMgr::_push_xacts_to_redis(const std::vector<PgTransactionPtr> &xacts)
    {
        // find the correct RedisSortedSet
        RSSOidValuePtr oid_set = _get_oid_set(_db_id);

        std::vector<std::string> msgs;
        for (auto &xact : xacts) {
            uint64_t xid = xact->springtail_xid;

            // go through the oid map and update redis
            for (auto &oid : xact->oids) {
                oid_set->add(PgRedisOidValue(oid, xid), xid);
            }

            PgXactMsg redis_xact(xact->begin_path, xact->commit_path,
                                 _db_id, xact->begin_offset,
                                 xact->commit_offset, xact->xact_lsn,
                                 xid, xact->xid, xact->aborted_xids);

            // track last xact lsn we've pushed to redis
            assert (xact->xact_lsn > _last_pushed_lsn);
            _last_pushed_lsn = xact->xact_lsn;

            // convert to string and push to redis queue
            msgs.push_back(static_cast<std::string>(redis_xact));
        }

        // do an underlying batch/bulk push
        _redis_queue.push(msgs);
    }

    RSSOidValuePtr
    PgLogMgr::_get_oid_set(uint64_t db_id)
    {
        std::unique_lock<std::mutex> lock(_oid_set_mutex);

        auto itr = _oid_set.find(db_id);
        if (itr != _oid_set.end()) {
            return itr->second;
        }

        std::string key = fmt::format(redis::SET_PG_OID_XIDS,
                                      Properties::get_db_instance_id(), db_id);

        RSSOidValuePtr oid_set = std::make_shared<RSSOidValue>(key);
        auto result = _oid_set.emplace(db_id, oid_set);
        if (!result.second) {
            throw Error("Failed to insert into oid set");
        }

        return oid_set;
    }

} // namespace springtail::pg_log_mgr
