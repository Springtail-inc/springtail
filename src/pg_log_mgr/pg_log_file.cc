#include <unistd.h>

#include <common/logging.hh>
#include <common/exception.hh>

#include <pg_repl/pg_types.hh>
#include <pg_log_mgr/pg_log_file.hh>


namespace springtail {

    PgLogFile::PgLogFile(const std::filesystem::path &file) : _file(file)
    {
        int fmode = O_APPEND | O_CREAT;
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP;

        _fd = ::open(file.c_str(), fmode, owner);
        if (_fd == -1) {
            SPDLOG_ERROR("Error opening file: path={}, errno={}", file.c_str(), errno);
            throw Error("Error opening file for PgLogFile");
        }
    }

    void
    PgLogFile::close()
    {
        ::fsync(_fd);
        ::close(_fd);
    }

    bool
    PgLogFile::log_data(const PgCopyData &data)
    {
        if (data.length == 0) {
            return false;
        }

        // write out header containing length if start of message
        if (data.msg_offset == 0) {
            char buffer[16];
            sendint32(PG_LOG_MAGIC, buffer);
            sendint32(data.msg_length, buffer + 4);
            sendint64(data.starting_lsn, buffer + 8);

            ::write(_fd, buffer, 16);
            _current_offset += 16;
            _msg_end_offset = _current_offset + data.msg_length;
        }

        // write message data
        ::write(_fd, data.buffer, data.length);
        _current_offset += data.length;

        if (_msg_end_offset == _current_offset) {
            return true; // full message written
        }
        return false;
    }
}