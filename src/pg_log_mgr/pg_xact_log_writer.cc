#include <unistd.h>
#include <errno.h>
#include <chrono>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>

#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail {

    PgXactLogWriter::PgXactLogWriter(const std::filesystem::path &file)
    {
        int fmode = O_APPEND | O_CREAT;
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP;

        _fd = ::open(file.c_str(), fmode, owner);
        if (_fd == -1) {
            SPDLOG_ERROR("Error opening file: path={}, errno={}", file.c_str(), errno);
            throw Error("Error opening file for PgLogFile");
        }

        _fsync_thread = std::thread(&PgXactLogWriter::_fsync_worker, this);
    }

    void
    PgXactLogWriter::close()
    {
        _shutdown = true;
        _fsync_thread.join();
        ::fsync(_fd);
    }

    void
    PgXactLogWriter::_fsync_worker()
    {
        while (!_shutdown) {
            // sleep for at least PG_LOG_MIN_FSYNC_MS
            std::this_thread::sleep_for(std::chrono::milliseconds(PG_XLOG_MIN_FSYNC_MS));
            if (_shutdown) {
                break;
            }

            if (!_need_fsync.load()) {
                continue;
            }

            // set flag before fsync in case data comes while fsync is in progress
            // can't be exactly sure of what was fsynced
            _need_fsync = false;
            ::fsync(_fd);
        }
    }

    void
    PgXactLogWriter::log_data(PgTransactionPtr xact, uint64_t xid)
    {
        // write message
        // 4B message length + 4B magic + 4B postgres XID + 8B springtail XID +
        // 8B lsn + 8B begin offset + 8B commit offset +
        // 4B path len + path string (starting offset path) +
        // 4B path len + path string (ending offset path)

        uint32_t buffer_len = (4+ 4 + 4 + 8 + 8 + 8 + 8 + 4 + 4); // fixed msg length
        uint32_t begin_path_len = ::strlen(xact->begin_path.c_str());
        uint32_t commit_path_len = ::strlen(xact->commit_path.c_str());

        buffer_len += begin_path_len;
        buffer_len += commit_path_len;

        char buffer[buffer_len];

        std::copy_n(reinterpret_cast<char *>(&buffer_len), 4, &buffer[0]);
        std::copy_n(reinterpret_cast<const char *>(&PG_XLOG_MAGIC), 4, &buffer[4]);
        std::copy_n(reinterpret_cast<char *>(&xact->xid), 4, &buffer[8]);
        std::copy_n(reinterpret_cast<char *>(&xid), 8, &buffer[12]);
        std::copy_n(reinterpret_cast<char *>(&xact->xact_lsn), 8, &buffer[20]);
        std::copy_n(reinterpret_cast<char *>(&xact->begin_offset), 8, &buffer[28]);
        std::copy_n(reinterpret_cast<char *>(&xact->commit_offset), 8, &buffer[36]);
        std::copy_n(reinterpret_cast<char *>(&begin_path_len), 4, &buffer[44]);
        std::copy_n(reinterpret_cast<char *>(&commit_path_len), 4, &buffer[48]);
        std::copy_n(xact->begin_path.c_str(), begin_path_len, &buffer[52]);
        std::copy_n(xact->commit_path.c_str(), commit_path_len, &buffer[52 + begin_path_len]);

        int res = ::write(_fd, buffer, buffer_len);
        if (res != buffer_len) {
            SPDLOG_ERROR("Error writing to xact log file={}, result={}, errno={}\n", _file.c_str(), res, errno);
            throw Error("Error writing to xact log file");
        }

        _need_fsync = true;
        _offset += buffer_len;
    }

}