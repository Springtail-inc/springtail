#pragma once

#include <common/filesystem.hh>
#include <pg_log_mgr/pg_xact_log_writer_mmap.hh>

namespace springtail::pg_log_mgr {

/**
 * @brief This class reads from the transaction log file.
 *
 */
class PgXactLogReaderMmap {
public:

    /**
     * @brief Construct a new Pg Xact Log Reader Mmap object
     *
     * @param base_dir - directory where the log files are located
     */
    explicit PgXactLogReaderMmap(const std::filesystem::path &base_dir);

    /**
     * @brief Destroy the Pg Xact Log Reader Mmap object
     *
     */
    ~PgXactLogReaderMmap();

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
    uint32_t get_pg_xid() const;

    /**
     * @brief Get the xid value
     *
     * @return uint64_t
     */
    uint64_t get_xid() const;

private:
    std::filesystem::path _base_dir;        ///< full path to transaction logs directory
    std::optional<std::filesystem::path> _current_file;     ///< the name of the current file
    char _read_buffer[PgXactLogWriterMmap::PG_XLOG_PAGE_SIZE];  ///< read buffer memory
    PgXactLogWriterMmap::XidElement *_current_xid{nullptr};     ///< pointer to the next memory location to get the data from
    size_t _current_offset{0};          ///< current memory offset inside the current read segment
    int _fd{-1};                        ///< current file descriptor

    /**
    * @brief Load the next extent from current file
    */
    void _load_next_page();

    /**
    * @brief Open and load the next file
    * @return true if successful, false if no more files
    */
    bool _open_next_file();
};
}  // namespace springtail::pg_log_mgr