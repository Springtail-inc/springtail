#include <common/filesystem.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_log_recovery.hh>

namespace springtail::pg_log_mgr {

uint64_t
PgLogRecovery::recover()
{
    uint64_t lsn = INVALID_LSN;

    std::filesystem::path latest_log =
        fs::find_latest_modified_file(_repl_path, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
    if (!latest_log.empty()) {
        lsn = PgMsgStreamReader::scan_log(latest_log, true);
    }

    return lsn;
}

void
PgLogRecovery::replay()
{
    // scan the replication log to skip any already committed records
    bool has_more = _skip_committed();

    // replay any messages from transactions that started before the most recently committed
    // transaction, but weren't committed
    if (!_active_map.empty()) {
        has_more = _replay_active();
    }

    // replay any messages fully after the most recently committed transaction's commit
    if (has_more) {
        _replay_uncommitted();
    }
}

bool
PgLogRecovery::_skip_committed()
{
    // open the repl log
    std::filesystem::path repl_log = fs::find_earliest_modified_file(
        _repl_path, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
    _repl_reader.set_file(repl_log);

    // Open the xact log
    PgXactLogReader xact_reader(_xact_path, PgLogMgr::LOG_PREFIX_XACT, PgLogMgr::LOG_SUFFIX);

    // note: once we are garbage collecting old log files, we'll need a way to ensure that the
    //       two log positions are aligned with eachother

    // Scan the repl log for any begin/commit/abort messages
    bool done = false;
    std::vector<char> filter = {pg_msg::MSG_BEGIN, pg_msg::MSG_COMMIT, pg_msg::MSG_STREAM_START,
                                pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT};

    uint32_t log_number = 0;
    uint32_t cur_pgxid = 0;
    while (!done) {
        // check if there are more messages in the replication log
        bool eob, eos;
        auto msg = _repl_reader.read_message(filter, eob, eos);
        if (msg != nullptr) {
            done = _process_msg(msg, log_number, repl_log, xact_reader, cur_pgxid);
        }

        // check if we need to move to the next replication log file
        if (!done && eos) {
            repl_log = fs::get_next_file(repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (!std::filesystem::exists(repl_log)) {
                return false;
            }

            ++log_number;
            _repl_reader.set_file(repl_log);
        }
    }

    return true;
}

bool
PgLogRecovery::_process_msg(PgMsgPtr msg,
                            uint32_t log_number,
                            const std::filesystem::path &repl_log,
                            PgXactLogReader &xact_reader,
                            uint32_t &cur_pgxid)
{
    switch (msg->msg_type) {
            // when a begin is seen, record it's position into the active set as a possible
            // starting point for the scan along with pgxid
        case PgMsgEnum::BEGIN: {
            auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
            Position p(log_number, _repl_reader.header_offset(), repl_log);
            _active_map.try_emplace(begin_msg.xid, p);
            cur_pgxid = begin_msg.xid;
            return false;
        }
        case PgMsgEnum::STREAM_START: {
            auto &start_msg = std::get<PgMsgStreamStart>(msg->msg);
            if (start_msg.first) {
                Position p(log_number, _repl_reader.header_offset(), repl_log);
                _active_map.try_emplace(start_msg.xid, p);
            }
            cur_pgxid = start_msg.xid;
            return false;
        }

            // when an abort is seen, remove the pgxid from the active set
        case PgMsgEnum::STREAM_ABORT: {
            auto &abort_msg = std::get<PgMsgStreamAbort>(msg->msg);
            if (abort_msg.xid == abort_msg.sub_xid) {
                _active_map.erase(abort_msg.xid);
            }
            return false;
        }

            // when a commit is seen, check for it in the xact log
            //   i)   if the xact log is empty, start the replay step
            //   ii)  if the pgxid doesn't match the next entry, there's some kind of error
            //        -- should never happen, but we could roll back the committed XID to
            //        the XID prior to this one and replay?
            //   iii) if the XID is <= the committed XID, remove from the active set
            //   iv)  if the XID is > the committed XID, start the replay step
        case PgMsgEnum::COMMIT:
        case PgMsgEnum::STREAM_COMMIT: {
            uint32_t pgxid;
            if (msg->msg_type == pg_msg::MSG_COMMIT) {
                pgxid = cur_pgxid;
            } else {
                auto &commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                pgxid = commit_msg.xid;
            }
            CHECK_EQ(pgxid, xact_reader.get_pg_xid());

            bool done = false;
            CHECK_LE(xact_reader.get_xid(), _committed_xid);
            _active_map.erase(pgxid);

            if (xact_reader.get_xid() == _committed_xid) {
                _final_committed = {log_number, _repl_reader.block_end_offset(), repl_log};
                done = true;
            } else {
                done = !xact_reader.next();  // move to the next record in the xact log
            }

            return done;
        }
        default: {
            int type = static_cast<int>(msg->msg_type);
            SPDLOG_ERROR("Received invalid message type: {}", type);
            throw Error(fmt::format("Received invalid message type: {}", type));
        }
    }
}

bool
PgLogRecovery::_replay_active()
{
    CHECK(!_active_map.empty());

    // Otherwise, we need to re-process all of the in-flight active xacts.  Find the earliest
    // starting position of an active transaction.
    auto min_i = std::min_element(
        _active_map.begin(), _active_map.end(),
        [](const std::pair<uint32_t, Position> &lhs, const std::pair<uint32_t, Position> &rhs) {
            return lhs.second < rhs.second;
        });
    CHECK(min_i != _active_map.end());

    uint64_t start_offset = min_i->second.offset;
    std::filesystem::path repl_log = min_i->second.file;
    _repl_reader.set_file(repl_log, start_offset);

    // replay repl log entries for the active set... skip everything else until we get to the end of
    // the _final_committed transaction
    bool done = false;
    std::vector<char> filter = {
        pg_msg::MSG_BEGIN,         pg_msg::MSG_COMMIT,       pg_msg::MSG_STREAM_START,
        pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT, pg_msg::MSG_INSERT,
        pg_msg::MSG_UPDATE,        pg_msg::MSG_DELETE,       pg_msg::MSG_TRUNCATE,
        pg_msg::MSG_MESSAGE  // this will capture create_table, drop_table, alter_table,
                             // create_index, drop_index
    };
    while (!done) {
        bool eob, eos;
        auto msg = _repl_reader.read_message(filter, eos, eob);

        // first entry of the block must be BEGIN or STREAM_START
        CHECK(msg->msg_type == pg_msg::MSG_BEGIN || msg->msg_type == pg_msg::MSG_STREAM_START);
        uint32_t pgxid;
        if (msg->msg_type == pg_msg::MSG_BEGIN) {
            auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
            pgxid = begin_msg.xid;
        } else {
            auto &start_msg = std::get<PgMsgStreamStart>(msg->msg);
            pgxid = start_msg.xid;
        }
        bool skip = !_active_map.contains(pgxid);

        if (skip) {
            // if skipping the block then reposition the reader
            _repl_reader.set_file(repl_log, _repl_reader.block_end_offset());
            eos = _repl_reader.end_of_stream();

            // check if we just passed the first uncommitted pgxid
            if (_repl_reader.offset() == _final_committed.offset) {
                done = true;  // from here process everything
            }
        } else {
            // process all of the messages in the block
            while (!eob) {
                auto msg = _repl_reader.read_message(filter, eos, eob);
                if (msg != nullptr) {
                    _pg_log_reader->enqueue_msg(msg);
                }
            }
        }

        // check if we need to move to the next file
        if (eos) {
            repl_log = fs::get_next_file(repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (!std::filesystem::exists(repl_log)) {
                return false;  // no more logs to process
            }

            _repl_reader.set_file(repl_log);
        }
    }

    return true;
}

void
PgLogRecovery::_replay_uncommitted()
{
    std::vector<char> filter = {
        pg_msg::MSG_BEGIN,         pg_msg::MSG_COMMIT,       pg_msg::MSG_STREAM_START,
        pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT, pg_msg::MSG_INSERT,
        pg_msg::MSG_UPDATE,        pg_msg::MSG_DELETE,       pg_msg::MSG_TRUNCATE,
        pg_msg::MSG_MESSAGE  // this will capture create_table, drop_table, alter_table,
                             // create_index, drop_index
    };

    bool done = false;
    while (!done) {
        bool eob, eos;
        auto msg = _repl_reader.read_message(filter, eos, eob);
        if (msg != nullptr) {
            // queue the message for processing
            _pg_log_reader->enqueue_msg(msg);
        }

        // check if we need to move to the next file
        if (eos) {
            std::filesystem::path repl_log =
                fs::get_next_file(repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (!std::filesystem::exists(repl_log)) {
                done = true;
            } else {
                _repl_reader.set_file(repl_log);
            }
        }
    }
}

}  // namespace springtail::pg_log_mgr
