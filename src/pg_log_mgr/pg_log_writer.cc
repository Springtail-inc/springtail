#include <unistd.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/exception.hh>
#include <common/coordinator.hh>

#include <pg_repl/pg_types.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_log_writer.hh>

namespace springtail::pg_log_mgr {

    PgLogWriter::PgLogWriter(uint64_t db_id,
                             const std::filesystem::path &file,
                             std::function<void (uint64_t)> lsn_callback_fn)
        : _db_id(db_id), _writer(file), _file(file), _lsn_callback_fn(lsn_callback_fn)
    {
        _fsync_thread = std::thread(&PgLogWriter::_fsync_worker, this);
    }

    void
    PgLogWriter::_fsync_worker()
    {
        std::string coordinator_id = fmt::format(PgLogMgr::FSYNC_WORKER_ID, _db_id);
        auto coordinator = Coordinator::get_instance();
        auto& keep_alive = coordinator->register_thread(Coordinator::DaemonType::LOG_MGR, coordinator_id);

        while (!_shutdown) {
            // mark alive with coordinator
            Coordinator::mark_alive(keep_alive);

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
            _writer.sync();
            _last_fsync_offset = offset;

            _update_lsn_from_queue();
        }

        // unregister thread before exiting
        coordinator->unregister_thread(Coordinator::DaemonType::LOG_MGR, coordinator_id);
    }

    void
    PgLogWriter::_add_lsn_to_queue(uint64_t offset, LSN_t lsn)
    {
        std::unique_lock lock{_queue_mutex};
        _lsn_queue.push(std::make_shared<LsnOffset>(offset, lsn));
    }

    void
    PgLogWriter::_update_lsn_from_queue()
    {
        std::unique_lock lock{_queue_mutex};
        uint64_t curr_offset = _last_fsync_offset.load();
        LSN_t latest_lsn = _latest_synced_lsn.load();
        LSN_t queued_lsn = INVALID_LSN;

        // go through the lsn queue and check if the current offset
        // is greater than the offset of the LSN in the queue, if so
        // then pop the LSN off the queue and update the latest LSN
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
            _lsn_callback_fn(queued_lsn);
        }
    }

    void
    PgLogWriter::close()
    {
        // shutdown the fsync thread and join
        _shutdown_fsync();

        // see if we need to do a final fsync
        uint64_t curr_offset = _current_offset.load();
        if (curr_offset != _last_fsync_offset.load()) {
            _writer.sync();
            _last_fsync_offset = curr_offset;
            _update_lsn_from_queue();
        }

        // finally close the fd
        _writer.close();
    }

    bool
    PgLogWriter::log_data(const PgCopyData &data, const char current_msg_type)
    {
        if (data.length == 0) {
            return false;
        }

        // get the offset before writing the data
        uint64_t start_offset = _writer.offset();

        LSN_t ack_lsn = INVALID_LSN;

        // write out header containing length if start of message
        if (data.msg_offset == 0) {
            _msg_end_offset = start_offset + data.msg_length;

            // write the header
            PgMsgStreamHeader header(data.msg_length, _last_commit_lsn);
            _writer.write_header(header);

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG4, "Write repl message start: start lsn={}, length={}, msg_length={}",
                      data.starting_lsn, data.length, data.msg_length);

            if (current_msg_type == pg_msg::MSG_COMMIT ||
                current_msg_type == pg_msg::MSG_STREAM_COMMIT) {

                // stream commit message is 30B; commit is 26B
                DCHECK(data.msg_length <= 30);

                // if this is a commit message, we may need to buffer it
                if (data.length == data.msg_length) {
                    // full msg, extract LSN from commit record and ack it
                    ack_lsn = PgMsgStreamReader::get_commit_lsn(data.buffer);
                    _have_partial_commit = false;
                } else {
                    // partial commit message, buffer it
                    memcpy(_commit_buffer, data.buffer, data.length);
                    _have_partial_commit = true;
                }
            }
        }

        if (_have_partial_commit) {
            // we have a partial commit message buffered, copy the rest
            memcpy(_commit_buffer + data.msg_offset, data.buffer, data.length);

            if (data.msg_offset + data.length == data.msg_length) {
                // we have a full commit message now, extract LSN and ack it
                ack_lsn = PgMsgStreamReader::get_commit_lsn(_commit_buffer);
                _have_partial_commit = false;
            }
        }

        // write message data, returns offset after write
        _current_offset = _writer.write_message(data);

        // check if a full message was written
        if (ack_lsn != INVALID_LSN) {
            // always ack one byte head of last commit LSN
            _add_lsn_to_queue(_msg_end_offset, ack_lsn + 1);

            _last_commit_lsn = ack_lsn;

            LOG_DEBUG(LOG_PG_LOG_MGR, LOG_LEVEL_DEBUG2, "Commit complete, ack lsn={}, length={}, msg_length={}",
                      ack_lsn, data.length, data.msg_length);

            return true;
        }

        return false;
    }

} // namespace springtail::pg_log_mgr
