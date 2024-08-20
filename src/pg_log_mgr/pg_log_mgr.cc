#include <thread>
#include <sys/time.h>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/properties.hh>
#include <common/logging.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_types.hh>

#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail::pg_log_mgr {

    // XXX make sure GC is shutdown -- assume it for now
    void
    PgLogMgr::startup()
    {
        uint64_t lsn = INVALID_LSN;

        // scan latest replication log and extract ending LSN
        // XXX if we find an empty log then remove file and go to previous log
        std::filesystem::path latest_log = fs::find_latest_modified_file(_repl_log_path, LOG_PREFIX_REPL, LOG_SUFFIX);
        if (!latest_log.empty()) {
            lsn = PgMsgStreamReader::scan_log(latest_log, true);
        }

        // fetch latest xid from xid mgr
        XidMgrClient *xid_mgr = XidMgrClient::get_instance();
        _next_xid = xid_mgr->get_committed_xid(0) + 1;

        // scan xact logs and transfer in progress xacts to log reader
        PgXactLogReader xact_reader(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX);
        xact_reader.scan_all_files(_next_xid-1);

        // update next xid if we find an xid higher than committed xid in the log
        uint64_t last_allocated_xid = xact_reader.get_max_sp_xid();
        if (last_allocated_xid > _next_xid) {
            _next_xid = last_allocated_xid + 1;
        }

        // move contents of stream map into pg log reader, invalidates stream map
        std::map<uint32_t, springtail::PgTransactionPtr> stream_map = xact_reader.get_stream_map();
        _pg_log_reader.set_xact_map(stream_map);

        // fetch xaction list from xact reader, and add them to Redis
        // these are transactions with springtail XIDs that are > than last committed XID
        // XXX clear out Redis first???  Assume we start pipeline from scratch, GC is stopped
        std::vector<PgTransactionPtr> xact_list = xact_reader.get_xact_list();
        for (const auto &xact : xact_list) {
            _push_xact_to_redis(xact);
        }

        // if xact log is behind pg log then we need to catchup before starting to stream
        // find last xact from xact_list, and start from there (file + offset)
        if (!xact_list.empty()) {
            PgTransactionPtr last_xact = xact_list.back();
            _pg_log_reader.process_log(last_xact->commit_path, last_xact->commit_offset, -1);
        }

        // XXX notify GC of startup??? notify an external coordinator?

        // start streaming
        start_streaming(lsn);
    }

    void
    PgLogMgr::start_streaming(uint64_t lsn)
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
            _pg_conn.read_data(data);

            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Recevied data: length={}, msg_length={}, msg_offset={}\n",
                         data.length, data.msg_length, data.msg_offset);

            if (data.length == 0) {
                // possible data has been consumed by keep alive
                continue;
            }

            // log data, if data message is complete then record start/end offsets
            if (logger->log_data(data)) {
                uint64_t end_offset = logger->offset();

                // record start/end offsets for this message
                _logger_queue.push(start_offset, end_offset, logger->filename());

                // check to see if we should rollover log
                if (end_offset > LOG_ROLLOVER_SIZE_BYTES) {
                    logger->close();
                    logger = this->_create_repl_logger();
                    start_offset = 0;
                } else {
                    start_offset = end_offset;
                }
            }
        }
    }

    /** Thread for reading log data */
    void
    PgLogMgr::_log_reader_thread()
    {
        while (!_shutdown) {
            // get log entry from queue
            PgLogQueueEntryPtr log_entry = this->_logger_queue.pop();
            if (log_entry == nullptr) {
                continue;
            }

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
        std::filesystem::path file = fs::find_latest_modified_file(_xact_log_path, LOG_PREFIX_XACT, LOG_SUFFIX);
        if (file.empty()) {
            file = _xact_log_path / fmt::format("{}{:04d}{}", LOG_PREFIX_XACT, 0, LOG_SUFFIX);
        } else {
            file = fs::get_next_file(file, LOG_PREFIX_XACT, LOG_SUFFIX);
        }

        return std::make_shared<PgXactLogWriter>(file);
    }

    /** Thread for handling transactions from log reader */
    void
    PgLogMgr::_xact_handler_thread()
    {
        PgXactLogWriterPtr logger = _create_xact_logger(); ///< logger to write out xid log

        while (!_shutdown) {
            PgTransactionPtr xact = _xact_queue->pop();
            if (xact == nullptr) {
                continue;
            }

            _process_xact(xact, logger);
        }
    }

    void
    PgLogMgr::_process_xact(const PgTransactionPtr xact, const PgXactLogWriterPtr logger)
    {
        // if stream start, just log it
        if (xact->type == PgTransaction::TYPE_STREAM_START ||
            xact->type == PgTransaction::TYPE_STREAM_ABORT) {
            logger->log_stream_msg(xact);
            return;
        }

        // first allocate an xid for this xact
        uint64_t xid = _next_xid;
        _next_xid++;

        // next log the data
        assert (xact->type == PgTransaction::TYPE_COMMIT);
        xact->springtail_xid = xid;
        logger->log_commit(xact);

        _push_xact_to_redis(xact);
    }

    void
    PgLogMgr::_push_xact_to_redis(const PgTransactionPtr xact)
    {
        uint64_t xid = xact->springtail_xid;

        // find the correct RedisSortedSet
        RSSOidValuePtr oid_set;
        {
            std::scoped_lock lock(_oid_set_mutex);

            auto set_i = _oid_set.find(_db_id);
            if (set_i == _oid_set.end()) {
                // construct one if it doesn't exist
                std::string key = fmt::format(redis::SET_PG_OID_XIDS,
                                              Properties::get_db_instance_id(), _db_id);
                oid_set = std::make_shared<RSSOidValue>(key);

                auto result = _oid_set.emplace(_db_id, oid_set);
                if (!result.second) {
                    throw Error();
                }
            } else {
                oid_set = set_i->second;
            }
        }

        // go through the oid map and update redis
        for (auto &oid : xact->oids) {
            oid_set->add(PgRedisOidValue(oid, xid), xid);
        }

        // finally send notification to GC
        PgRedisXactValue redis_xact(xact->begin_path, xact->commit_path, _db_id, xact->begin_offset,
                                    xact->commit_offset, xact->xact_lsn, xid, xact->xid, xact->aborted_xids);

        // XXX need to add customer ID
        _redis_queue.push(redis_xact);
    }
} // namespace springtail::pg_log_mgr
