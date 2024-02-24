#include <sys/time.h>

#include <fmt/core.h>

#include <common/common.hh>
#include <common/properties.hh>
#include <common/logging.hh>

#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_types.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail {

    void
    PgLogMgr::start_streaming()
    {
        _pg_conn.connect();
        std::cout << "Connecting to postgres server: " << _host << std::endl;

        // create slot if need be
        bool create_slot = !_pg_conn.check_slot_exists();

        if (create_slot) {
            std::cout << "Creating replication slot: " << _slot_name << std::endl;
            _pg_conn.create_replication_slot(false,  // export
                                             false); // temporary
        }

        // start steaming
        _pg_conn.start_streaming(INVALID_LSN);

        // get the protocol version
        _proto_version = _pg_conn.get_protocol_version();

        // create write thread
        _writer_thread = std::thread(&PgLogMgr::_log_writer, this);
        _reader_thread = std::thread(&PgLogMgr::_log_reader, this);
        _xact_thread = std::thread(&PgLogMgr::_xact_worker, this);
    }

    void
    PgLogMgr::_lsn_callback(LSN_t lsn)
    {
        _pg_conn.set_last_flushed_LSN(lsn);
    }

    /** Thread for writing log data */
    void
    PgLogMgr::_log_writer()
    {
        PgLogWriterPtr logger = this->_create_logger();

        PgCopyData data;
        uint64_t start_offset = logger->offset();

        while (!_shutdown) {
            _pg_conn.read_data(data);

            SPDLOG_DEBUG("Recevied data: length={}, msg_length={}, msg_offset={}\n",
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
                    logger = this->_create_logger();
                    start_offset = 0;
                } else {
                    start_offset = end_offset;
                }
            }
        }
    }

    /** Thread for reading log data */
    void
    PgLogMgr::_log_reader()
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
    PgLogMgr::_create_logger()
    {
        std::filesystem::path file;
        do {
            int offset = 0;
            file = _repl_log_path;
            file.append(fmt::format("{}", common::get_time_in_millis() + offset));
            // shouldn't ever have to loop here...
            offset++;
        } while (std::filesystem::exists(file));

        return std::make_shared<PgLogWriter>(file, _proto_version,
            [this](LSN_t lsn) { _pg_conn.set_last_flushed_LSN(lsn); });
    }

    void
    PgLogMgr::_xact_worker()
    {
        PgXactHandler xact_handler{_xact_log_path};

        while (!_shutdown) {
            PgReplMsgStream::PgTransactionPtr xact = _xact_queue->pop();
            if (xact == nullptr) {
                continue;
            }

            xact_handler.process(xact);
        }
    }
}