#include <sys/time.h>

#include <fmt/core.h>

#include <common/properties.hh>
#include <common/logging.hh>

#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/pg_types.hh>

#include <pg_log_mgr/pg_log_mgr.hh>

namespace springtail {

    static inline uint64_t get_time_in_millis()
    {
        struct timeval t;
        gettimeofday(&t, nullptr);
        return (uint64_t)t.tv_sec * 1000 + t.tv_usec / 1000;
    }

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
    }

    void
    PgLogMgr::process_log()
    {
        if (_logger == nullptr) {
            _create_logger();
        }

        PgCopyData data;
        uint64_t start_offset = _logger->offset();

        while (true) {
            _pg_conn.read_data(data);

            SPDLOG_DEBUG("Recevied data: length={}, msg_length={}, msg_offset={}\n",
                         data.length, data.msg_length, data.msg_offset);

            if (data.length == 0) {
                // possible data has been consumed by keep alive
                continue;
            }

            // log data, if data message is complete then record start/end offsets
            if (_logger->log_data(data)) {
                uint64_t end_offset = _logger->offset();

                // record start/end offsets for this message
                _queue.push(start_offset, end_offset, _logger->filename());

                // check to see if we should rollover log
                if (end_offset > LOG_ROLLOVER_SIZE_BYTES) {
                    _logger->close();
                    _create_logger();
                    start_offset = 0;
                } else {
                    start_offset = end_offset;
                }
            }
        }
    }

    void
    PgLogMgr::_create_logger()
    {
        std::filesystem::path file = _base_path;
        file.append(fmt::format("{}", get_time_in_millis()));
        _logger = std::make_shared<PgLogFile>(file);
    }
}