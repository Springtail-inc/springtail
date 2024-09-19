#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <chrono>
#include <vector>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>

#include <pg_log_mgr/pg_xact_log_writer.hh>

namespace springtail::pg_log_mgr {

    PgXactLogWriter::PgXactLogWriter(const std::filesystem::path &file) : _file(file)
    {
        int fmode = O_WRONLY | O_APPEND | O_CREAT;
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP;

        // create the base directory for the file if it doesn't exist
        std::filesystem::create_directories(file.parent_path());

        _fd = ::open(file.c_str(), fmode, owner);
        if (_fd == -1) {
            SPDLOG_ERROR("Error opening file: path={}, errno={}", file.c_str(), errno);
            throw Error("Error opening file for PgLogFile");
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Opened xact log file: {}\n", file.c_str());

        _fsync_thread = std::thread(&PgXactLogWriter::_fsync_worker, this);
    }

    void
    PgXactLogWriter::close()
    {
        if (_fd == -1) {
            return;
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Xact log writer closing file: {}, fd={}", _file.c_str(), _fd);
        if (_shutdown) {
            return;
        }
        _shutdown = true;
        _fsync_thread.join();
        ::fsync(_fd);
        ::close(_fd);
        _fd = -1;
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
    PgXactLogWriter::log_stream_msg(PgTransactionPtr xact)
    {
        // TYPE_STREAM_START or TYPE_STREAM_ABORT
        // 4B message length + 3B magic + 1B Type
        // 4B postgres XID + 8B LSN + 8B begin offset + 4B path len + path string
        assert(xact->type == PgTransaction::TYPE_STREAM_START || xact->type == PgTransaction::TYPE_STREAM_ABORT);

        uint32_t buffer_len = (4 + 3 + 1 + 4 + 8 + 8 + 4); // fixed msg length
        uint32_t begin_path_len = ::strlen(xact->begin_path.c_str());

        // calculate total length of log entry
        uint32_t total_length = buffer_len + begin_path_len;
        std::vector<char> data(total_length); // allocate log entry on heap in vector
        char *buffer = data.data();           // get pointer to actual data

        // first write out fixed header
        std::copy_n(reinterpret_cast<char *>(&total_length), 4, &buffer[0]);
        std::copy_n(reinterpret_cast<const char *>(&PG_XLOG_MAGIC[0]), 3, &buffer[4]);
        std::copy_n(reinterpret_cast<const char *>(&xact->type), 1, &buffer[7]);
        std::copy_n(reinterpret_cast<char *>(&xact->xid), 4, &buffer[8]);
        std::copy_n(reinterpret_cast<char *>(&xact->xact_lsn), 8, &buffer[12]);
        std::copy_n(reinterpret_cast<char *>(&xact->begin_offset), 8, &buffer[20]);
        std::copy_n(reinterpret_cast<char *>(&begin_path_len), 4, &buffer[28]);

        // next write out variable length data
        int offset = 32;
        std::copy_n(xact->begin_path.c_str(), begin_path_len, &buffer[offset]);

        assert(offset + begin_path_len == total_length);

        // do the actual disk write
        int res = ::write(_fd, buffer, total_length);
        if (res != total_length) {
            SPDLOG_ERROR("Error writing to xact log file={}, result={}, errno={}\n", _file.c_str(), res, errno);
            throw Error("Error writing to xact log file");
        }

        _offset += total_length;
        _need_fsync = true;
    }

    void
    PgXactLogWriter::log_commit(PgTransactionPtr xact)
    {
        // TYPE_COMMIT
        // 4B message length + 3B magic + 1B Type
        // 4B postgres XID + 8B springtail XID +
        // 8B lsn + 8B begin offset + 8B commit offset +
        // 4B path len + path string (starting offset path) +
        // 4B path len + path string (ending offset path)
        // 4B oid count + oid list (8B each oid)
        // 4B xid count + xid list (4B each xid)

        uint32_t buffer_len = (4 + 3 + 1 + 4 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4); // fixed msg length
        uint32_t begin_path_len = ::strlen(xact->begin_path.c_str());
        uint32_t commit_path_len = ::strlen(xact->commit_path.c_str());
        uint32_t oid_cnt = xact->oids.size();
        uint32_t aborted_cnt = xact->aborted_xids.size();

        // calculate total length of log entry
        uint32_t total_length = buffer_len + begin_path_len + commit_path_len + oid_cnt * 8 + aborted_cnt * 4;
        std::vector<char> data(total_length); // allocate log entry on heap in vector
        char *buffer = data.data();           // get pointer to actual data

        // first write out fixed header
        std::copy_n(reinterpret_cast<char *>(&total_length), 4, &buffer[0]);
        std::copy_n(reinterpret_cast<const char *>(&PG_XLOG_MAGIC[0]), 3, &buffer[4]);
        std::copy_n(reinterpret_cast<const char *>(&xact->type), 1, &buffer[7]);
        std::copy_n(reinterpret_cast<char *>(&xact->xid), 4, &buffer[8]);
        std::copy_n(reinterpret_cast<char *>(&xact->springtail_xid), 8, &buffer[12]);
        std::copy_n(reinterpret_cast<char *>(&xact->xact_lsn), 8, &buffer[20]);
        std::copy_n(reinterpret_cast<char *>(&xact->begin_offset), 8, &buffer[28]);
        std::copy_n(reinterpret_cast<char *>(&xact->commit_offset), 8, &buffer[36]);
        std::copy_n(reinterpret_cast<char *>(&begin_path_len), 4, &buffer[44]);
        std::copy_n(reinterpret_cast<char *>(&commit_path_len), 4, &buffer[48]);
        std::copy_n(reinterpret_cast<char *>(&oid_cnt), 4, &buffer[52]);
        std::copy_n(reinterpret_cast<char *>(&aborted_cnt), 4, &buffer[56]);

        // next write out variable length data
        int offset = 60;
        std::copy_n(xact->begin_path.c_str(), begin_path_len, &buffer[offset]);
        offset += begin_path_len;

        std::copy_n(xact->commit_path.c_str(), commit_path_len, &buffer[offset]);
        offset += commit_path_len;

        for (uint64_t oid: xact->oids) {
            std::copy_n(reinterpret_cast<char *>(&oid), 8, &buffer[offset]);
            offset += 8;
        }

        for (uint32_t xid: xact->aborted_xids) {
            std::copy_n(reinterpret_cast<char *>(&xid), 4, &buffer[offset]);
            offset += 4;
        }

        assert(offset == total_length);

        // do the actual disk write
        int res = ::write(_fd, buffer, total_length);
        if (res != total_length) {
            SPDLOG_ERROR("Error writing to xact log file={}, result={}, errno={}\n", _file.c_str(), res, errno);
            throw Error("Error writing to xact log file");
        }

        _offset += total_length;
        _need_fsync = true;
    }
} // namespace springtail::pg_log_mgr
