#pragma once

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <pg_repl/pg_repl_msg.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
#include <storage/io_mgr.hh>

namespace springtail::pg_log_mgr {

/**
 * @brief Postgres transaction log reader
 */
class PgXactLogReader {
public:
    /**
     * @brief Construct a new Pg Xact Log Reader object
     * @param base_dir    base directory for log files
     */
    explicit PgXactLogReader(const std::filesystem::path &base_dir);

    /**
     * @brief Begin reading from the first log file
     * @return true if successful, false if no files found
     */
    bool begin();

    /**
     * @brief Move to the next row in the log
     * @return true if successful, false if no more rows
     */
    bool next();

    // Accessor methods for fields
    uint32_t get_pg_xid() const;
    uint64_t get_xid() const;

private:
    std::filesystem::path _base_dir;
    std::optional<std::filesystem::path> _current_file;

    ExtentPtr _current_extent;
    Extent::Iterator _current_row;
    size_t _current_offset = 0;
    std::shared_ptr<IOHandle> _current_handle;

    ExtentSchemaPtr _schema;  ///< The schema of the log rows
    FieldPtr _pg_xid_f;       ///< Field for the pg xid column
    FieldPtr _xid_f;          ///< Field for the springtail xid column

    /**
     * @brief Load the next extent from current file
     * @return true if successful, false if no more extents
     */
    bool _load_next_extent();

    /**
     * @brief Open and load the next file
     * @return true if successful, false if no more files
     */
    bool _open_next_file();
};

}  // namespace springtail::pg_log_mgr
