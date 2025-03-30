#pragma once

#include <common/filesystem.hh>

namespace springtail::pg_log_mgr {

/**
 * @brief Xact log writer class using mmap to write to file.
 *
 */
class PgXactLogWriterMmap {
public:
    /** Page size to load at a time */
    static constexpr size_t PG_XLOG_PAGE_SIZE = 4096;

    /**
     * @brief Storage structure
     *
     */
    struct XidElement {
        uint32_t pg_xid;    ///< Postgress transaction id
        uint64_t xid;       ///< Transaction id associated with this xid
    };

    /**
     * @brief Construct a new Pg Xact Log Writer Mmap object
     *
     * @param base_dir - directory to store the files in
     * @param committed_xid - committed xid to start writing from
     */
    explicit PgXactLogWriterMmap(const std::filesystem::path &base_dir);

    /**
     * @brief Destroy the Pg Xact Log Writer Mmap object
     *
     */
    ~PgXactLogWriterMmap();

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
    void log(uint32_t pg_xid, uint64_t xid);

private:
    std::filesystem::path _base_dir;    ///< full path to file storage directory
    std::filesystem::path _file;        ///< file name of the current log file
    size_t _file_size{0};               ///< file size - multiple of page size
    size_t _mmap_offset{0};             ///< offset of mmap segment
    size_t _last_offset{0};             ///< offset inside the mmap segment, points to the next available memory location
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
};
} // namespace springtail::pg_log_mgr