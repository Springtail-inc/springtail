#include <unistd.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>

#include <pg_repl/pg_types.hh>
#include <pg_log_mgr/pg_log_writer.hh>


namespace springtail {

    PgLogWriter::PgLogWriter(const std::filesystem::path &file, int proto_version)
        : _file(file), _proto_version(proto_version)

    {
        int fmode = O_APPEND | O_CREAT;
        mode_t owner = S_IRUSR | S_IWUSR | S_IRGRP;

        _fd = ::open(file.c_str(), fmode, owner);
        if (_fd == -1) {
            SPDLOG_ERROR("Error opening file: path={}, errno={}", file.c_str(), errno);
            throw Error("Error opening file for PgLogFile");
        }

         _fsync_thread = std::thread(&PgLogWriter::_fsync_worker, this);
    }

    void
    PgLogWriter::_fsync_worker()
    {
        while (!_shutdown) {
            // sleep for at least PG_LOG_MIN_FSYNC_MS
            std::this_thread::sleep_for(std::chrono::milliseconds(PG_LOG_MIN_FSYNC_MS));
            if (_shutdown) {
                break;
            }
            uint64_t offset = _current_offset.load();

            // only fsync if offset changed
            if (offset == _last_fsync_offset.load()) {
                continue;
            }

            // do fsync and update offset
            ::fsync(_fd);
            _last_fsync_offset = offset;

            update_lsn_from_queue();
        }
    }

    void
    PgLogWriter::add_lsn_to_queue(uint64_t start_offset, LSN_t start_lsn,
                                  uint64_t end_offset, LSN_t end_lsn)
    {
        std::unique_lock lock{_queue_mutex};
        _lsn_queue.push(std::make_shared<LsnOffset>(start_offset, start_lsn));
        _lsn_queue.push(std::make_shared<LsnOffset>(end_offset, end_lsn));
    }

    void
    PgLogWriter::update_lsn_from_queue()
    {
        std::unique_lock lock{_queue_mutex};
        uint64_t curr_offset = _last_fsync_offset.load();
        LSN_t latest_lsn = _latest_synced_lsn.load();
        LSN_t queued_lsn = INVALID_LSN;

        while (!_lsn_queue.empty()) {
            LsnOffsetPtr p = _lsn_queue.front();
            if (p->offset <= curr_offset) {
                queued_lsn = p->lsn;
                _lsn_queue.pop();
            } else {
                break;
            }
        }

        lock.unlock();

        if (latest_lsn != queued_lsn && queued_lsn != INVALID_LSN) {
            _latest_synced_lsn = queued_lsn;
        }
    }

    void
    PgLogWriter::close()
    {
        // shutdown the fsync thread
        _shutdown_fsync();

        // see if we need to do a final fsync
        uint64_t curr_offset = _current_offset.load();
        if (curr_offset != _last_fsync_offset.load()) {
            ::fsync(_fd);
            _last_fsync_offset = curr_offset;
            update_lsn_from_queue();
        }

        // finally close the fd
        ::close(_fd);
    }

    bool
    PgLogWriter::log_data(const PgCopyData &data)
    {
        if (data.length == 0) {
            return false;
        }

        uint64_t current_offset = _current_offset.load();

        // write out header containing length if start of message
        if (data.msg_offset == 0) {
            char buffer[PG_LOG_HDR_BYTES];
            sendint32(PG_LOG_MAGIC, buffer);
            sendint32(data.msg_length, buffer + 4);
            sendint64(data.starting_lsn, buffer + 8);
            sendint64(data.ending_lsn, buffer + 16);
            sendint32(_proto_version, buffer + 24);

            ::write(_fd, buffer, 16);
            current_offset += 16;
            _msg_end_offset = current_offset + data.msg_length;

            // add LSN data to queue for fsync thread
            add_lsn_to_queue(current_offset, data.starting_lsn, _msg_end_offset, data.ending_lsn);
        }

        // write message data
        ::write(_fd, data.buffer, data.length);
        current_offset += data.length;

        _current_offset = current_offset;

        if (_msg_end_offset == current_offset) {
            // full message written
            return true;
        }
        return false;
    }
}