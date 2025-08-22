#pragma once

#include <filesystem>
#include <fstream>
#include <cassert>

#include <fmt/format.h>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_repl_connection.hh>

namespace springtail {
    /**
     * @brief Msg Stream header
     */
    struct PgMsgStreamHeader {
        LSN_t start_lsn;
        LSN_t end_lsn;
        uint32_t magic;
        uint32_t msg_length;

        /** Magic number used in log header */
        static constexpr uint32_t MAGIC=0xDEFC8193UL;

        /** Size of the log header */
        static constexpr int SIZE = (4 + 4 + 8 + 8);

        PgMsgStreamHeader() = default;

        PgMsgStreamHeader(uint32_t msg_length, LSN_t start_lsn, LSN_t end_lsn)
            : start_lsn(start_lsn), end_lsn(end_lsn), magic(MAGIC),
              msg_length(msg_length)
        {}

        PgMsgStreamHeader(const char * const buffer) {
            magic = recvint32(buffer);
            msg_length = recvint32(buffer + 4);
            start_lsn = recvint64(buffer + 8);
            end_lsn = recvint64(buffer + 16);
            DCHECK_EQ(magic, MAGIC);
        }

        void encode_header(char * const buffer) const {
            sendint32(MAGIC, buffer);
            sendint32(msg_length, buffer + 4);
            sendint64(start_lsn, buffer + 8);
            sendint64(end_lsn, buffer + 16);
        }

        std::string to_string() const {
            return fmt::format("Header: msg_length={}, start LSN={}, end LSN={}",
                                msg_length, start_lsn, end_lsn);
        }
    };

    /**
     * @brief Class for reading messages from a stream of log files
     */
    class PgMsgStreamReader {
    public:
        /** Filter for all message types */
        const std::vector<char> ALL_MESSAGES = {'B', 'C', 'R', 'I', 'U', 'D', 'T',
                                                'O', 'M', 'Y', 'S', 'E', 'c', 'A'};
        explicit PgMsgStreamReader(uint64_t db_id);
        PgMsgStreamReader(const PgMsgStreamReader&) = delete;
        PgMsgStreamReader& operator=(const PgMsgStreamReader&) = delete;
        /**
         * @brief Construct a new Pg Msg Stream Reader object
         * @param db_id The database id
         * @param start_file file to start reading from
         * @param start_offset offset to start reading from (0 = beginning of file)
         */
        PgMsgStreamReader(std::optional<uint64_t> db_id,
                          const std::filesystem::path &start_file,
                          uint64_t start_offset=0);

        /**
         * @brief Set the file and offset to start reading from
         * @param start_file file to start reading from
         * @param start_offset offset to start reading from (0 = beginning of file)
         * @param end_offset offset to stop reading at (-1 = end of file)
         */
        void set_file(const std::filesystem::path &start_file,
                      uint64_t start_offset=0, uint64_t end_offset=-1);

        /**
         * @brief Read next message from stream
         * @param filter optional filter to apply to message types; decodes messages in filter
         * @param eos output | end of stream; true if no more messages
         * @param eob output | end of block; true if no more messages in current block
         * @return PgMsgPtr; nullptr if no more messages or message skipped, check has_more() for EOF across all files
         */
        PgMsgPtr read_message(const std::vector<char> &filter, bool &eos);

        /**
         * @brief Read next message from stream
         * @param filter optional filter to apply to message types; decodes messages in filter
         * @return PgMsgPtr; nullptr if no more messages or message skipped, check has_more() for EOF across all files
         */
        PgMsgPtr read_message(const std::vector<char> &filter);

        /**
         * @brief Get current offset
         * @return uint64_t current offset
         */
        uint64_t offset() const { return _current_offset; }

        /**
         * @brief Get the offset of the message we just read
         * @return uint64_t message offset
         */
        uint64_t message_offset() const { return _message_offset; }

        /**
         * @brief Does the stream contain more data to parse
         * @return true if has more data
         * @return false if no more data
         */
        bool end_of_stream() const
        {
            if (_current_offset >= _file_end_offset || _stream.eof()) {
                return true;
            }
            return false;
        }

        /**
         * @brief Scan the entire log file validating header, return end LSN of last message
         * @param file log file to scan
         * @param truncate if true truncate file to end of last message if eof reached prematurely
         * @return uint64_t end LSN of last message
         */
        static uint64_t scan_log(uint64_t db_id,
                const std::filesystem::path &file,
                bool truncate=false);

        void set_streaming() { _streaming = true; }

        const PgMsgStreamHeader& current_header() const { return _header; }

    protected:
        // Proto V1; message lengths if fixed length; excludes first byte for opcode
        static inline constexpr int LEN_BEGIN    = (8 + 8 + 4);
        static inline constexpr int LEN_COMMIT   = (1 + 8 + 8 + 8);

        // Proto V2
        static inline constexpr int LEN_STREAM_START  = (4 + 1);
        static inline constexpr int LEN_STREAM_STOP   = 0;
        static inline constexpr int LEN_STREAM_COMMIT = (4 + 1 + 8 + 8 + 8);

        // decode messages
        // v1 messages
        PgMsgPtr _decode_begin();
        PgMsgPtr _decode_commit();
        PgMsgPtr _decode_relation();
        PgMsgPtr _decode_insert();
        PgMsgPtr _decode_update();
        PgMsgPtr _decode_delete();
        PgMsgPtr _decode_truncate();
        PgMsgPtr _decode_origin();
        PgMsgPtr _decode_type();
        PgMsgPtr _decode_message();

        // v2 messages
        PgMsgPtr _decode_stream_start();
        PgMsgPtr _decode_stream_stop();
        PgMsgPtr _decode_stream_commit();
        PgMsgPtr _decode_stream_abort();

        // decoded messages
        PgMsgPtr _decode_create_table(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_alter_table(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_drop_table(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_create_namespace(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_alter_namespace(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_drop_namespace(PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_create_index(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_drop_index(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_create_usertype(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_alter_usertype(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_drop_usertype(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_copy_sync(const PgMsgMessage &message, char *buffer, int len);
        PgMsgPtr _decode_attach_partition(const PgMsgMessage &message, const char *buffer, int len);
        PgMsgPtr _decode_detach_partition(const PgMsgMessage &message, const char *buffer, int len);

        // helpers
        void _decode_schema_columns(const nlohmann::json &json, std::vector<PgMsgSchemaColumn> &columns);
        void _decode_tuple(PgMsgTupleData &tuple);
        void _decode_string(std::string &ostring);

        // v1 messages
        void _skip_begin();
        void _skip_commit();
        void _skip_relation();
        void _skip_insert();
        void _skip_update();
        void _skip_delete();
        void _skip_truncate();
        void _skip_origin();
        void _skip_type();
        void _skip_message();

        void _skip_tuple();
        void _skip_string();

        // v2 messages
        void _skip_stream_start();
        void _skip_stream_stop();
        void _skip_stream_commit();
        void _skip_stream_abort();

    private:
        std::optional<uint64_t> _db_id;

        std::fstream _stream;                ///< current file stream

        std::filesystem::path _current_path; ///< current file path

        uint64_t _message_offset = 0;   ///< offset of the message we just read
        uint64_t _current_offset;       ///< current file offset

        uint64_t _xlog_msg_end_offset = 0; ///< ending offset of the xlog message in the file
        uint64_t _file_end_offset = 0;     ///< ending offset of the file, file size

        PgMsgStreamHeader _header;      ///< header of the current xlog data message

        int _proto_version;             ///< protocol version of message block (from header)

        bool _read_hdr = false;         ///< true if header needs to be read (set when opening a new file)
        bool _streaming = false;        ///< true if streaming mode (between stream_start and stream_stop)

        /** Helper to check for eof errors in reading stream */
        void _check_fail() {
            if (_stream.fail()) {
                throw PgMessageEOFError();
            }
        }

        // XXX as written this is somewhat expensive, we should cache the results in a map
        // and invalidate on db_config changes from redis cache. SPR-968
        /** Check if namespace name (schema) is in the include list for filtering from properties */
        bool _is_schema_included(const std::string& schema);

        /**
         * @brief Read message header from stream
         * Fills in _header and sets _xlog_msg_end_offset and _current_offset
         */
        void _read_header();

        /** Helper to seek stream based on current offset */
        void _seek_stream(uint64_t file_offset);

        /** Read stream at current offset, return uint32_t */
        uint32_t _recvint32() {
            uint32_t res = recvint32(_stream);
            _check_fail();
            _current_offset += 4;
            return res;
        }

        /** Read stream at current offset, return uint64_t */
        uint64_t _recvint64() {
            uint64_t res = recvint64(_stream);
            _check_fail();
            _current_offset += 8;
            return res;
        }

        /** Read stream at current offset, return uint16_t */
        uint16_t _recvint16() {
            uint16_t res = recvint16(_stream);
            _check_fail();
            _current_offset += 2;
            return res;
        }

        /** Read stream at current offset, return uint8_t */
        uint8_t _recvint8() {
            uint8_t res = recvint8(_stream);
            _check_fail();
            _current_offset++;
            return res;
        }

        /** Read stream at current offset and copy data into buffer; return false if eof hit, true otherwise */
        bool _read_buffer(char *buffer, int size) {
            if (size + _current_offset > _file_end_offset) {
                // trying to read past end of file
                return false;
            }

            // make sure we don't read past the end of the xlog message
            DCHECK_LE(size + _current_offset, _xlog_msg_end_offset);

            _stream.read(buffer, size);
            _check_fail();
            DCHECK_EQ(_stream.gcount(), size);
            _current_offset += size;
            return true; // no eof
        }

        /** Checks if msg type is in filtered set, if it is returns true */
        bool _is_message_filtered(char msg_type, const std::vector<char> &v) const;

        /** opens a file for reading at given offset */
        void _open_file(const std::filesystem::path &file, uint64_t offset);

        /** Decodes a message of a given type returning a msg ptr */
        PgMsgPtr _decode_msg(char msg_type);

        /** Skips a message of a given type */
        void _skip_msg(char msg_type);

        static void _truncate_file(const std::filesystem::path &path, uint64_t offset);
    };

    class PgMsgStreamWriter {
    public:
        PgMsgStreamWriter(const std::filesystem::path &file);

        ~PgMsgStreamWriter() { close(); }

        /**
         * @brief Write a message to the stream
         * @param data data to add, may be a partial message
         * @return uint64_t offset after message chunk written
         */
        uint64_t write_message(const PgCopyData &data);

        /**
         * @brief Write header to the stream
         * @param header header to write
         * @return uint64_t offset after header written
         */
        uint64_t write_header(const PgMsgStreamHeader &header);

        /**
         * @brief Perform fsync of data
         */
        void sync();

        /**
         * @brief Close file
         */
        void close();

        /**
         * @brief Get current write offset
         * @return uint64_t offset
         */
        uint64_t offset() const { return _current_offset; }

    private:
        int _fd=-1;     ///< file descriptor; use C fd for fsync

        std::filesystem::path _file;

        uint64_t _current_offset = 0;
    };
} // namespace springtail
