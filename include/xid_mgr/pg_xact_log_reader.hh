#pragma once

#include <absl/log/check.h>

#include <common/filesystem.hh>
#include <xid_mgr/pg_xact_log_writer.hh>

namespace springtail::xid_mgr {

/**
 * @brief This class reads from the transaction log file.
 *
 */
class PgXactLogReader {
public:

    /**
     * @brief Construct a new Pg Xact Log Reader Mmap object
     *
     * @param base_dir - directory where the log files are located
     * @param last_xid - last allowed transaction id
     */
    explicit PgXactLogReader(const std::filesystem::path &base_dir)
        : _base_dir(base_dir), _current_xid {0,0,false} {};

    /**
     * @brief Destroy the Pg Xact Log Reader Mmap object
     *
     */
    ~PgXactLogReader();

    /**
     * @brief Begin reading from the first log file
     * @return true if successful, false if no files found or no data in the first file.
     */
    bool begin();

     /**
      * @brief Move to the next row in the log
      * @return true if successful, false if no more rows
      */
    bool next();

    // Accessor methods for fields
    /**
     * @brief Get the pg xid value
     *
     * @return uint32_t
     */
    uint32_t
    get_pg_xid() const
    {
        CHECK(_current_xid.xid != 0);
        return _current_xid.pg_xid;
    }

    /**
     * @brief Get the xid value
     *
     * @return uint64_t
     */
    uint64_t
    get_xid() const
    {
        CHECK(_current_xid.xid != 0);
        return _current_xid.xid;
    }

    /**
     * @brief Get the real value of current xid entry
     *
     * @return true - if it was a real commit
     * @return false - if it was not a real commit
     */
    bool
    get_real_commit() const
    {
        return _current_xid.real_commit;
    }

private:
    std::filesystem::path _base_dir;        ///< full path to transaction logs directory
    std::optional<std::filesystem::path> _current_file;     ///< the name of the current file
    char _read_buffer[PgXactLogWriter::PG_XLOG_PAGE_SIZE];  ///< read buffer memory
    PgXactLogWriter::XidElement _current_xid;     ///< pointer to the next memory location to get the data from
    size_t _current_offset{0};          ///< current memory offset inside the current read segment
    size_t _end_offset{0};              ///< end offset of the current read segment
    int _fd{-1};                        ///< current file descriptor

    /**
     * @brief Load the next page from current file
     *
     * @return true - success
     * @return false - failure
     */
    bool _load_next_page();

    /**
     * @brief Open and load the next file
     * @return true if successful, false if no more files
     */
    bool _open_next_file();

    /**
     * @brief Function for cleaning up internal state.
     *
     */
    void _cleanup();
};
}  // namespace springtail::pg_log_mgr