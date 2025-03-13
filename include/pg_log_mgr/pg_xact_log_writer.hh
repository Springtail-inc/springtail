#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include <pg_repl/pg_repl_msg.hh>

#include <storage/extent.hh>

namespace springtail::pg_log_mgr {

/**
 * @brief Postgres transaction log writer
 */
class PgXactLogWriter {
public:
    static constexpr int PG_XLOG_MIN_FSYNC_MS = 250;

    static constexpr uint8_t PG_XLOG_MAGIC[] = {0xA3, 0xF9, 0x4B};

    /**
     * @brief Construct a new Pg Xact Log Writer object; creates file
     * @param file path dir/name
     */
    PgXactLogWriter(const std::filesystem::path &file);

    ~PgXactLogWriter() { close(); }

    /**
     * Record a committed PG xid to the xact log.  Stores it's Springtail XID, the file offset
     * in the WAL holding the commit, and the WAL file holding the commit.
     */
    void log(uint32_t pg_xid, uint64_t xid);

    /** Close the file */
    void close();

    void rotate(uint64_t timestamp);

private:
    std::shared_mutex _file_mutex;        ///< mutex concurrent access to _file
    std::filesystem::path _file;          ///< current file path
    std::thread _fsync_thread;            ///< The background flushing thread.
    std::atomic<bool> _shutdown = false;  ///< Shutdown flag for the sync thread

    std::mutex _mutex; ///< Protects the extent and fields
    ExtentPtr _extent; ///< The batch of xact log entries
    ExtentSchemaPtr _schema; ///< The schema of the log rows
    MutableFieldPtr _pg_xid_f; ///< Field for the pg xid column
    MutableFieldPtr _xid_f; ///< Field for the springtail xid column

    /** Append an extent to the end of the current xact log file. */
    void _flush_extent(ExtentPtr extent);

    /** fsync thread */
    void _fsync_worker();

    void _flush_current_extent();
};

using PgXactLogWriterPtr = std::shared_ptr<PgXactLogWriter>;

}  // namespace springtail::pg_log_mgr
