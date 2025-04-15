#pragma once

#include <common/filesystem.hh>

namespace springtail::xid_mgr {

/**
 * @brief Xact log writer class using mmap to write to file.
 *
 */
class PgXactLogWriter {
public:
    /** Page size to load at a time */
    static constexpr size_t PG_XLOG_PAGE_SIZE = 4096;

    static constexpr char const * const LOG_PREFIX_XACT = "pg_log_xact_";
    static constexpr char const * const LOG_SUFFIX = ".log";

    /**
     * @brief Storage structure
     *
     */
    struct XidElement {
        uint32_t pg_xid;    ///< Postgress transaction id
        bool real_commit;
        uint64_t xid;       ///< Transaction id associated with this xid
    };

    /**
     * @brief Construct a new Pg Xact Log Writer Mmap object
     *
     * @param base_dir - directory to store the files in
     * @param committed_xid - committed xid to start writing from
     */
    PgXactLogWriter(const std::filesystem::path &base_dir);

    /**
     * @brief Destroy the Pg Xact Log Writer Mmap object
     *
     */
    ~PgXactLogWriter();

    /**
     * @brief Rotate the log files
     *
     * @param timestamp - timestamp to use in the file name
     */
    void rotate(uint64_t timestamp);

    /**
     * @brief Log transaction ids in the log
     *
     * @param pg_xid - Postgress xid
     * @param xid - transaction id associated with this xid
     */
    void log(uint32_t pg_xid, uint64_t xid, bool real_commit);

    void cleanup(uint64_t min_timestamp, bool archive_logs);

    uint64_t get_last_xid() { return _last_stored_xid; }

    static bool set_last_xid_in_storage(std::filesystem::path base_dir, uint64_t last_xid, bool archive);
private:
    std::filesystem::path _base_dir;    ///< full path to file storage directory
    std::filesystem::path _file;        ///< file name of the current log file
    size_t _file_size{0};               ///< file size - multiple of page size
    size_t _mmap_offset{0};             ///< offset of mmap segment
    size_t _last_offset{0};             ///< offset inside the mmap segment, points to the next available memory location
    uint64_t _last_stored_xid{1};
    uint64_t _current_log_timestamp{0};
    XidElement *_file_mem{nullptr};     ///< pointer to the mmaped memory
    int _fd{-1};                        ///< file descriptor of the open file, when set to -1, it means that no file is open
    bool _first_file{true};             ///< first file flag

    /**
     * @brief Flush from memory to file.
     *
     */
    void _flush();

    /**
     * @brief Add the next page to the file by resizing it and moving mmap to the newly added segment.
     *
     */
    void _resize_and_map();

    void _extract_last_xid();
};
} // namespace springtail::pg_log_mgr