#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <fstream>

#include <pg_repl/pg_repl_msg.hh>
#include <storage/extent.hh>
#include <storage/io_mgr.hh>
#include <storage/field.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Postgres transaction log reader
     */
    class PgXactLogReader {
    public:
        /**
         * @brief Construct a new Pg Xact Log Reader object
         * @param base_dir    base directory for log files
         * @param file_prefix file prefix
         * @param file_suffix file suffix
         */
        PgXactLogReader(const std::filesystem::path &base_dir,
                        const std::string &file_prefix,
                        const std::string &file_suffix)
            : _base_dir(base_dir), _file_prefix(file_prefix),
              _file_suffix(file_suffix) {}

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
        std::string _file_prefix;             
        std::string _file_suffix;             
        std::optional<std::filesystem::path> _current_file;  
        
        ExtentPtr _current_extent;
        Extent::Iterator _current_row;
        size_t _current_offset = 0;
        std::shared_ptr<IOHandle> _current_handle;

        // Field accessors (these would match the writer's fields)
        FieldPtr _pg_xid_f;
        FieldPtr _xid_f;

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
} // namespace springtail::pg_log_mgr
