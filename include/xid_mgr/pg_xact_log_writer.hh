#pragma once

#include <absl/log/check.h>

#include <common/filesystem.hh>

namespace springtail::xid_mgr {

/**
 * @brief Xact log writer class using mmap to write to file.
 *
 */
class PgXactLogWriter {
public:
    /**
     * @brief Storage structure for xid element read/written to xlog file
     */
    class XidElement {
    public:
        static constexpr int PACKED_SIZE = 8 + 4 + 1;

        uint64_t xid;       ///< Transaction id associated with this xid
        uint32_t pg_xid;    ///< Postgress transaction id
        bool real_commit;

        static int pack(char *buffer, const PgXactLogWriter::XidElement &xid_element)
        {
            DCHECK_NE(buffer, nullptr);
            int offset = 0;
            std::memcpy(buffer, &xid_element.xid, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            std::memcpy(buffer + offset, &xid_element.pg_xid, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            std::memcpy(buffer + offset, &xid_element.real_commit, sizeof(bool));
            offset += sizeof(bool);
            DCHECK_EQ(offset, XidElement::PACKED_SIZE) << "Unpacked size must match packed size";
            return offset;
        }

        static int unpack(const char *buffer, PgXactLogWriter::XidElement &xid_element)
        {
            DCHECK_NE(buffer, nullptr);
            int offset = 0;
            std::memcpy(&xid_element.xid, buffer, sizeof(uint64_t));
            offset += sizeof(uint64_t);
            std::memcpy(&xid_element.pg_xid, buffer + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            std::memcpy(&xid_element.real_commit, buffer + offset, sizeof(bool));
            offset += sizeof(bool);
            DCHECK_EQ(offset, XidElement::PACKED_SIZE) << "Unpacked size must match packed size";
            return offset;
        }
    };

    /** Page size to load at a time, must be divisible by XidElement size */
    static constexpr size_t PG_XLOG_PAGE_SIZE = XidElement::PACKED_SIZE * 256; // 1 KB page size

    /** Xact log prefix and suffix */
    static constexpr char const * const LOG_PREFIX_XACT = "pg_log_xact_";
    static constexpr char const * const LOG_SUFFIX = ".log";



    /**
     * @brief Construct a new Pg Xact Log Writer Mmap object
     *
     * @param base_dir - directory to store the files in
     * @param recovered_xid - last committed xid recovered from storage (0 if none, defaults to 0)
     */
    explicit PgXactLogWriter(const std::filesystem::path &base_dir, uint64_t recovered_xid = 0);

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

    /**
     * @brief Clean up old logs
     *
     * @param min_timestamp - minimum timestamp for remaining logs
     * @param archive_logs - archive logs flag
     */
    void cleanup(uint64_t min_timestamp, bool archive_logs);

    /**
     * @brief Get the last xid stored in the xact log
     *
     * @return uint64_t - xid value
     */
    uint64_t get_last_xid() { return _last_stored_xid; }

    /**
     * @brief Flush from memory to file.
     */
    void flush();

    /**
     * @brief Change existing log file such that existing xid is the last one in storage.
     *
     * @param base_dir - directory where xact log files are stored
     * @param last_xid - last allowed xid
     * @param archive - flag to archive or remove the files with the xids greater than the give one
     * @return uint64_t - the last committed xid found in storage, or 0 if none found
     */
    static uint64_t set_last_xid_in_storage(std::filesystem::path base_dir, uint64_t last_xid, bool archive);



private:
    std::filesystem::path _base_dir;    ///< full path to file storage directory
    std::filesystem::path _file;        ///< file name of the current log file
    size_t _offset{0};                  ///< offset inside write buffer for next entry
    uint64_t _last_stored_xid{1};       ///< value of the last xid stored in xact log
    uint64_t _current_log_timestamp{0}; ///< timestamp of the current log file
    char _write_buffer[PgXactLogWriter::PG_XLOG_PAGE_SIZE];
    int _fd{-1};                        ///< file descriptor of the open file, when set to -1, it means that no file is open
    bool _first_file{true};             ///< first file flag
    bool _can_flush{false};             ///< dirty flag that indicates that the data can be flushed

    /**
     * @brief In the existing file, advance to the position of the last stored xid
     */
    void _extract_last_xid();
};
} // namespace springtail::pg_log_mgr