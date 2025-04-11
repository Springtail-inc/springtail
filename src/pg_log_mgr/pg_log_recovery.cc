#include <cstdint>
#include <common/filesystem.hh>
#include <common/logging.hh>
#include <pg_log_mgr/pg_log_mgr.hh>
#include <pg_log_mgr/pg_log_recovery.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail::pg_log_mgr {

uint64_t
PgLogRecovery::repair_logs()
{
    uint64_t lsn = INVALID_LSN;

    // create directories if they don't exist
    // XXX shouldn't these always exist if we're starting up from running?  Maybe we should skip
    //     recovery if the directories didn't exist?
    std::filesystem::create_directories(_repl_path);
    std::filesystem::create_directories(_xact_path);

    auto latest_log =
        fs::find_latest_modified_file(_repl_path, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
    if (latest_log) {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Found latest log file: {}", *latest_log);
        lsn = PgMsgStreamReader::scan_log(*latest_log, true);
    } else {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Did not find any files in directory: {}", _repl_path.string());
    }

    return lsn;
}

void
PgLogRecovery::replay_logs()
{
    LOG_DEBUG(LOG_PG_LOG_MGR, "Start log replay");

    // revert the system tables to the committed XID
    _revert_system_tables();

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

void
PgLogRecovery::_revert_system_tables()
{
    // ask the SysTblMgr to revert the system tables to the most recently committed XID
    sys_tbl_mgr::Client::get_instance()->revert(_db_id, _committed_xid);

    // perform a commit at the next XID to ensure we have a clean snapshot from this point
    sys_tbl_mgr::Client::get_instance()->finalize(_db_id, _committed_xid + 1);
}

bool
PgLogRecovery::_skip_committed()
{
    // XXX we can change this to use the xact log only instead of scanning the repl log.  Will
    //     require changing the xact log to capture all of the relevant messages and their positions
    //     in the repl log so that we can replicate this behavior

    LOG_DEBUG(LOG_PG_LOG_MGR, "Skip already committed records");

    // open the repl log
    _repl_log = fs::find_earliest_modified_file(_repl_path, PgLogMgr::LOG_PREFIX_REPL,
                                                PgLogMgr::LOG_SUFFIX);
    if (!_repl_log) {
        LOG_DEBUG(LOG_PG_LOG_MGR, "No repl log found");
        return false;
    }

    LOG_DEBUG(LOG_PG_LOG_MGR, "Start with file {}", *_repl_log);

    if (fs::timestamp_file_exists(_repl_log.value(), PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_PREFIX_REPL_STREAMING, PgLogMgr::LOG_SUFFIX)) {
        LOG_DEBUG(LOG_PG_LOG_MGR, "Set streaming for file {}", _repl_log.value().string());
        _repl_reader.set_streaming();
    }

    _repl_reader.set_file(*_repl_log);

    // Open the xact log
    PgXactLogReaderMmap xact_reader(_xact_path, _committed_xid, _pg_log_reader->archive_logs());
    if (!xact_reader.begin()) {
        LOG_DEBUG(LOG_PG_LOG_MGR, "No xact log found");
        return true;
    }

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
        bool eob = false, eos = false;
        auto msg = _repl_reader.read_message(filter, eos, eob);
        if (msg != nullptr) {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Found message {}, eob {}, eos {}", static_cast<int>(msg->msg_type), eob, eos);
            done = _process_msg(msg, log_number, xact_reader, cur_pgxid);
        } else {
            LOG_DEBUG(LOG_PG_LOG_MGR, "Skipping message in repl log; offset {}, eob {}, eos {}", _repl_reader.offset(), eob, eos);
        }

        // check if we need to move to the next replication log file
        if (!done && eos) {
            _repl_log =
                fs::get_next_log_file(*_repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (!_repl_log) {
                LOG_DEBUG(LOG_PG_LOG_MGR, "No next replication log found, last committed xid is not found");
                return false;
            }
            LOG_DEBUG(LOG_PG_LOG_MGR, "Set streaming for file {}", _repl_log.value().string());

            ++log_number;
            _repl_reader.set_file(*_repl_log);
        }
    }

    xact_reader.cleanup_logs();
    return true;
}

bool
PgLogRecovery::_process_msg(PgMsgPtr msg,
                            uint32_t log_number,
                            PgXactLogReaderMmap &xact_reader,
                            uint32_t &cur_pgxid)
{
    switch (msg->msg_type) {
            // when a begin is seen, record it's position into the active set as a possible
            // starting point for the scan along with pgxid
        case PgMsgEnum::BEGIN: {
            auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
            Position p(log_number, _repl_reader.header_offset(), *_repl_log);
            _active_map.try_emplace(begin_msg.xid, p);
            cur_pgxid = begin_msg.xid;
            return false;
        }
        case PgMsgEnum::STREAM_START: {
            auto &start_msg = std::get<PgMsgStreamStart>(msg->msg);
            if (start_msg.first) {
                Position p(log_number, _repl_reader.header_offset(), *_repl_log);
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
            if (msg->msg_type == PgMsgEnum::COMMIT) {
                pgxid = cur_pgxid;
            } else {
                auto &commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                pgxid = commit_msg.xid;
            }
            LOG_DEBUG(LOG_PG_LOG_MGR, "Found COMMIT for pgxid {} == {} with xact_xid {}", pgxid, xact_reader.get_pg_xid(), xact_reader.get_xid());
            CHECK(pgxid == xact_reader.get_pg_xid() || pgxid == 0);

            bool done = false;
            CHECK_LE(xact_reader.get_xid(), _committed_xid);
            if (pgxid != 0) {
                _active_map.erase(pgxid);
            }

            if (xact_reader.get_xid() == _committed_xid) {
                LOG_DEBUG(LOG_PG_LOG_MGR, "Found final commit");
                _final_committed = {log_number, _repl_reader.block_end_offset(), *_repl_log};
                done = true;
            } else {
                done = !xact_reader.next();  // move to the next record in the xact log
                LOG_DEBUG(LOG_PG_LOG_MGR, "Advanced xact_reader to the next xid; done = {}", done);
            }

            return done;
        }
        default: {
            int type = static_cast<int>(msg->msg_type);
            LOG_ERROR("Received invalid message type: {}", type);
            throw Error(fmt::format("Received invalid message type: {}", type));
        }
    }
}

bool
PgLogRecovery::_replay_active()
{
    CHECK(!_active_map.empty());

    LOG_DEBUG(LOG_PG_LOG_MGR, "Replay active messages");

    // Otherwise, we need to re-process all of the in-flight active xacts.  Find the earliest
    // starting position of an active transaction.
    auto min_i = std::min_element(
        _active_map.begin(), _active_map.end(),
        [](const std::pair<uint32_t, Position> &lhs, const std::pair<uint32_t, Position> &rhs) {
            return lhs.second < rhs.second;
        });
    CHECK(min_i != _active_map.end());

    uint64_t start_offset = min_i->second.offset;
    _repl_log = min_i->second.file;
    uint64_t timestamp = fs::extract_timestamp_from_file(_repl_log.value(), PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX).value();
    _repl_reader.set_file(*_repl_log, start_offset);
    LOG_DEBUG(LOG_PG_LOG_MGR, "Replaying active from file {}", _repl_log.value().string());

    // replay repl log entries for the active set... skip everything else until we get to the end of
    // the _final_committed transaction
    std::vector<char> process_filter = {
        pg_msg::MSG_BEGIN,         pg_msg::MSG_COMMIT,       pg_msg::MSG_STREAM_START,
        pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT, pg_msg::MSG_INSERT,
        pg_msg::MSG_UPDATE,        pg_msg::MSG_DELETE,       pg_msg::MSG_TRUNCATE,
        pg_msg::MSG_MESSAGE  // this will capture create_table, drop_table, alter_table,
                             // create_index, drop_index
    };
    std::vector<char> scan_filter = {pg_msg::MSG_BEGIN,         pg_msg::MSG_STREAM_START,
                                     pg_msg::MSG_COMMIT,        pg_msg::MSG_STREAM_STOP,
                                     pg_msg::MSG_STREAM_COMMIT, pg_msg::MSG_STREAM_ABORT};
    std::vector<char> &filter = scan_filter;
    bool skip = true;

    bool done = false;
    while (!done) {
        // get the next matching message, returns nullptr when there are no more messages in any logs
        bool eob, eos;
        auto msg = _repl_reader.read_message(filter, eos, eob);
        if (msg != nullptr) {
            switch (msg->msg_type) {
            case PgMsgEnum::BEGIN: {
                // set the filter and flag to skip or process the messages for this txn
                auto &begin_msg = std::get<PgMsgBegin>(msg->msg);
                skip = !_active_map.contains(begin_msg.xid);
                filter = (skip) ? scan_filter : process_filter;
                LOG_DEBUG(LOG_PG_LOG_MGR, "Found BEGIN; pgxid {}, skip {}", begin_msg.xid, skip);
                break;
            }
            case PgMsgEnum::STREAM_START: {
                // set the filter and flag to skip or process the messages for this txn
                auto &start_msg = std::get<PgMsgBegin>(msg->msg);
                skip = !_active_map.contains(start_msg.xid);
                filter = (skip) ? scan_filter : process_filter;
                LOG_DEBUG(LOG_PG_LOG_MGR, "Found STREAM_START; pgxid {}, skip {}", start_msg.xid, skip);
                break;
            }
            case PgMsgEnum::STREAM_ABORT: {
                // skip or process msg
                auto &abort_msg = std::get<PgMsgStreamAbort>(msg->msg);
                skip = !_active_map.contains(abort_msg.xid);
                filter = (skip) ? scan_filter : process_filter;
                LOG_DEBUG(LOG_PG_LOG_MGR, "Found STREAM_ABORT; pgxid {}, skip {}", abort_msg.xid, skip);
                break;
            }
            case PgMsgEnum::STREAM_COMMIT: {
                // skip or process msg
                auto &commit_msg = std::get<PgMsgStreamCommit>(msg->msg);
                skip = !_active_map.contains(commit_msg.xid);
                filter = (skip) ? scan_filter : process_filter;
                LOG_DEBUG(LOG_PG_LOG_MGR, "Found STREAM_COMMIT; pgxid {}, skip {}", commit_msg.xid, skip);
                break;
            }
            default:
                // intentionally empty
                break;
            }

            // if we aren't skipping the message, process it
            if (!skip) {
                LOG_DEBUG(LOG_PG_LOG_MGR, "Process msg {}", static_cast<int>(msg->msg_type));
                msg->pg_log_timestamp = timestamp;
                _pg_log_reader->enqueue_msg(msg);
            }
        }

        // check if we need to move to the next file
        if (eos) {
            _repl_log =
                fs::get_next_log_file(*_repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (_repl_log) {
                _repl_reader.set_file(*_repl_log);
                timestamp = fs::extract_timestamp_from_file(_repl_log.value(), PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX).value();
                LOG_DEBUG(LOG_PG_LOG_MGR, "Replaying active from file {}", _repl_log.value().string());
            } else {
                return false;
            }
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

    uint64_t timestamp = fs::extract_timestamp_from_file(_repl_log.value(), PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX).value();
    LOG_DEBUG(LOG_PG_LOG_MGR, "Replaying uncommitted from file {}", _repl_log.value().string());

    while (_repl_log) {
        bool eob, eos;
        auto msg = _repl_reader.read_message(filter, eos, eob);
        if (msg != nullptr) {
            // queue the message for processing
            msg->pg_log_timestamp = timestamp;
            _pg_log_reader->enqueue_msg(msg);
        }

        // check if we need to move to the next file
        if (eos) {
            _repl_log =
                fs::get_next_log_file(*_repl_log, PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX);
            if (_repl_log) {
                _repl_reader.set_file(*_repl_log);
                timestamp = fs::extract_timestamp_from_file(_repl_log.value(), PgLogMgr::LOG_PREFIX_REPL, PgLogMgr::LOG_SUFFIX).value();
                LOG_DEBUG(LOG_PG_LOG_MGR, "Replaying uncommitted from file {}", _repl_log.value().string());
            }
        }
    }
}

}  // namespace springtail::pg_log_mgr
