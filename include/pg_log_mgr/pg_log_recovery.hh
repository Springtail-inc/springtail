#pragma once

#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_reader_mmap.hh>
#include <pg_repl/pg_msg_stream.hh>

namespace springtail::pg_log_mgr {

class PgLogRecovery {
public:
    PgLogRecovery(uint64_t db_id,
                  const std::filesystem::path &repl_path,
                  const std::filesystem::path &xact_path,
                  std::shared_ptr<PgLogReader> log_reader,
                  uint64_t committed_xid)
        : _repl_path(repl_path),
          _xact_path(xact_path),
          _committed_xid(committed_xid),
          _pg_log_reader(log_reader)
    {
        SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "Starting recovery with last committed xid = {}", _committed_xid);
    }

    /**
     * Scans the log files to ensure that they contain only valid records, truncating any incomplete
     * records.  Returns the last postgres LSN seen in the replication logs to allow the replication
     * stream to restart from the correct point.
     */
    uint64_t repair_logs();

    /**
     * Replays any uncommitted operations from the replication log.
     */
    void replay_logs();

private:
    /** Skip all of the committed transactions from the replication log, while tracking (1) any
        "active" transactions at the time of the commit of the final committed transaction and (2)
        the position in the log of the end of the block containing the last committed
        transaction. */
    bool _skip_committed();

    /** Helper to process an individual message during _skip_committed() */
    bool _process_msg(PgMsgPtr msg,
                      uint32_t log_number,
                      PgXactLogReaderMmap &xact_reader,
                      uint32_t &cur_pgxid);

    /** Play back only the "active" transaction from the replication log until we have replayed the
        first uncommitted transaciton. */
    bool _replay_active();

    /** Play back all of the remaining records in the replication log. */
    void _replay_uncommitted();

private:
    /** Helper structure to track the starting position of the first block in the replication log
        for a given pgxid. */
    struct Position {
        uint32_t log_number = 0; ///< A logical sequence number for the log files.
        uint64_t offset = 0; ///< The offset within the file.
        std::filesystem::path file; ///< The actual file path.

        Position() = default;
        Position(uint32_t ln, uint64_t o, const std::filesystem::path &f)
            : log_number(ln), offset(o), file(f)
        {
        }

        Position(const Position &) = default;
        Position &operator=(const Position &) = default;
        Position(Position &&) = default;
        Position &operator=(Position &&) = default;

        std::strong_ordering operator<=>(const Position &rhs) const
        {
            return std::tie(log_number, offset) <=> std::tie(rhs.log_number, rhs.offset);
        }
    };

    std::filesystem::path _repl_path;
    std::filesystem::path _xact_path;
    uint64_t _committed_xid;        ///< Holds the last commited xid.

    /** Interface for reading the replication log. */
    PgMsgStreamReader _repl_reader;
    std::optional<std::filesystem::path> _repl_log; ///< The current replication log file

    /** Map of pg xid -> Position.  Tracks the "active" transactions as we scan the log. */
    std::unordered_map<uint32_t, Position> _active_map;

    /** The end position of the block holding the commit of the last committed transaction. */
    Position _final_committed;

    /** Interface for reading the log messages. */
    std::shared_ptr<PgLogReader> _pg_log_reader;
};

} // namespace springtail::pg_log_mgr
