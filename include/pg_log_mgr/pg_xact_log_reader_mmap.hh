#pragma once

#include <cstddef>
#include <cstdint>
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
     * @param last_xid - last allowed transaction id
     */
    explicit PgXactLogReaderMmap(const std::filesystem::path &base_dir, uint64_t last_xid = std::numeric_limits<uint64_t>::max(), bool archive = false);

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

    /**
     * @brief Function for cleaning up remaining logs after the last transaction id is found
     *
     */
    void cleanup_logs();

private:
    std::filesystem::path _base_dir;        ///< full path to transaction logs directory
    std::optional<std::filesystem::path> _current_file;     ///< the name of the current file
    char _read_buffer[PgXactLogWriterMmap::PG_XLOG_PAGE_SIZE];  ///< read buffer memory
    PgXactLogWriterMmap::XidElement *_current_xid{nullptr};     ///< pointer to the next memory location to get the data from
    size_t _current_offset{0};          ///< current memory offset inside the current read segment
    uint64_t _last_xid;                 ///< last allowed transaction id
    uint32_t _page_count{0};            ///< number of pages in the current file
    int _fd{-1};                        ///< current file descriptor
    bool _last_xid_found{false};        ///< flag indicating that the last transaction id was found
    bool _archive;                      ///< flag that indicates if we need to move files to archive instead of removing them

    /**
    * @brief Load the next extent from current file
    */
    void _load_next_page();

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