#pragma once

#include <filesystem>
#include <memory>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>

namespace springtail {
    /**
     * Postgres log writer class.  Receives data from Pg replication stream in messages.
     * A message may contain multiple Postgres operations.  A header is written for each
     * message chunk received.  Once a message chunk has been fully written the log_data()
     * call returns true to notify the caller a full message has been written.
    */
    class PgLogWriter {
    public:
        /** Magic number used in log header */
        static constexpr uint32_t PG_LOG_MAGIC=0xDEFC8193;

        /** Size of the log header preceeds each message
         * 4B Magic number; 4B Msg length; 8B starting LSN; 4B protocol version
         */
        static constexpr int PG_LOG_HDR_BYTES=20;

        struct PgLogHeader {
            uint32_t magic;
            uint32_t msg_length;
            LSN_t starting_lsn;
            uint32_t proto_version;
        };

        /**
         * @brief Construct a new Pg Log Writer object
         * @param file file to be writing
         * @param proto_version protocol version
         */
        PgLogWriter(const std::filesystem::path &file, int proto_version);

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

        /**
         * @brief Helper to decode log header
         * @param buffer buffer containing header data of length PG_LOG_HDR_BYTES
         * @param header out | header to be filled in
         */
        static void decode_header(const char * const buffer, PgLogHeader &header) {
            header.magic = recvint32(buffer);
            header.msg_length = recvint32(buffer + 4);
            header.starting_lsn = recvint64(buffer + 8);
            header.proto_version = recvint32(buffer + 16);
        }

    private:
        /** current file path */
        std::filesystem::path _file;

        /** postgres version */
        int _proto_version;

        /** current offset */
        uint64_t _current_offset = 0;

        /** offset of end of current message */
        uint64_t _msg_end_offset = 0;

        /** file descriptor */
        int _fd;
    };
    using PgLogWriterPtr = std::shared_ptr<PgLogWriter>;
}