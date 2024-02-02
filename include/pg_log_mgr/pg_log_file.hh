#pragma once

#include <filesystem>
#include <memory>

#include <pg_repl/pg_repl_connection.hh>

namespace springtail {
    class PgLogFile {
    public:
        static constexpr uint32_t PG_LOG_MAGIC=0xDEFC8193;

        PgLogFile(const std::filesystem::path &file);

        /**
         * @brief Add data to log; start of message starts with header.
         * Header contains: PG_LOG_MAGIC 4 B + Msg Length 4B + Starting msg LSN 8B
         * @param data data to add, may be a partial message
         * @return true if full message received
         * @return false if partial message received
         */
        bool log_data(const PgCopyData &data);

        uint64_t offset() const { return _current_offset; }

        /** Close the file */
        void close();

        /**
         * @brief Get file size
         * @return uint64_t size in bytes
         */
        uint64_t size() const { return _current_offset; }

        /**
         * @brief Get file name
         * @return std::filesystem::path& filename
         */
        std::filesystem::path &filename() { return _file; }

    private:
        /** current file path */
        std::filesystem::path _file;

        /** current offset */
        uint64_t _current_offset = 0;

        /** offset of end of current message */
        uint64_t _msg_end_offset = 0;

        /** file descriptor */
        int _fd;
    };
    using PgLogFilePtr = std::shared_ptr<PgLogFile>;
}