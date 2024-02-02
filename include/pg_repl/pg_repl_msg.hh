#pragma once

#include <optional>
#include <pg_repl/pg_types.hh>
#include <nlohmann/json.hpp>

namespace springtail
{
    /* Postgres message types, see decode functions in
     * PgRep PgMsg.cc for formats */
    struct PgMsgTupleDataColumn {
        char type;  // 'n' NULL, 'u' unchanged TOAST, 't' text formatted, 'b' binary
        int32_t data_len;
        const char *data;
    };

    struct PgMsgTupleData {
        int16_t num_columns;
        std::vector<PgMsgTupleDataColumn> tuple_data;
    };

    struct PgMsgKeepAlive {
        LSN_t wal_end;
        bool response_requested;
    };

    struct PgMsgBegin {
        int32_t xid;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct PgMsgCommit {
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct PgMsgOrigin {
        LSN_t commit_lsn;
        const char *name_str;
    };

    struct PgMsgCopy {
        LSN_t wal_start;
        LSN_t wal_end;
    };

    struct PgMsgTruncate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t num_rels;
        int8_t options; // 1 for cascade; 2 for restart identity
        std::vector<int32_t> rel_ids;
    };

    struct PgMsgType {
        int32_t xid;   // proto vers 2+ only if streaming
        uint32_t oid;
        const char *namespace_str;
        const char *data_type_str;
    };

    struct PgMsgMessage {
        int32_t xid;   // proto vers 2+ only if streaming
        int8_t flags;
        LSN_t lsn;
        const char *prefix_str;    // null terminated string
        int32_t data_len;
        const char *data;
    };

    struct PgMsgDelete {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        PgMsgTupleData tuple;
    };

    struct PgMsgInsert {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char new_type;
        PgMsgTupleData new_tuple;
    };

    struct PgMsgUpdate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char old_type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        PgMsgTupleData old_tuple;
        char new_type; // 'N' tuple data is new
        PgMsgTupleData new_tuple;
    };

    struct PgMsgRelColumn {
        int8_t flags;  // 0 or 1 - key
        const char *column_name;
        uint32_t oid;   // oid from pg_types table
        int32_t type_modifier; // pg_attribute.atttypmod type specific data; default -1
    };

    struct PgMsgRelation {
        int32_t xid;      // proto vers 2+ only if streaming
        int32_t rel_id;
        const char *namespace_str;
        const char *rel_name_str;
        int8_t identity;      // replica identit; pg_class.relreplident
        int16_t num_columns;
        std::vector<PgMsgRelColumn> columns;
    };

    // stream ops in proto vers 2+ only
    struct PgMsgStreamStart {
        int32_t xid;
        bool first;
    };

    struct PgMsgStreamStop {
        // empty
    };

    struct PgMsgStreamCommit {
        int32_t xid;
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct PgMsgStreamAbort {
        int32_t xid;
        int32_t sub_xid;
        LSN_t   abort_lsn;  // proto vers 4+
        int64_t abort_ts;   // proto vers 4+
    };

    struct PgMsgSchemaColumn {
        std::string column_name;
        std::string udt_type;
        std::optional<std::string> default_value;
        int position;        // position is maintained if column is renamed
        bool is_nullable;
        bool is_pkey;        // is primary key
    };

    struct PgMsgTable { // used by both create table and alter table
        uint32_t oid;
        LSN_t lsn;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string schema;
        std::string table;
        std::vector<PgMsgSchemaColumn> columns;
    };

    struct PgMsgDropTable {
        uint32_t oid;
        LSN_t lsn;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string schema;
        std::string table;
    };


    /** Message types */
    enum PgReplMsgType {
        INVALID, COPY_HDR, KEEP_ALIVE, BEGIN, COMMIT, RELATION, INSERT, DELETE, UPDATE, TRUNCATE,
        ORIGIN, MESSAGE, TYPE,
        // version 2
        STREAM_START, STREAM_STOP, STREAM_COMMIT, STREAM_ABORT,
        // decoded messages
        CREATE_TABLE, ALTER_TABLE, DROP_TABLE
    };

    /**
     * @brief Decoded replication message
     * @details Contains union of messages with the type
     *          specified by the Pgmsg_type
     */
    struct PgReplMsgDecoded {
        std::variant<
         PgMsgKeepAlive,
         PgMsgBegin,
         PgMsgCommit,
         PgMsgOrigin,
         PgMsgCopy,
         PgMsgMessage,
         PgMsgType,
         PgMsgTruncate,
         PgMsgDelete,
         PgMsgUpdate,
         PgMsgInsert,
         PgMsgRelation,
         PgMsgStreamStart,
         PgMsgStreamStop,
         PgMsgStreamCommit,
         PgMsgStreamAbort,
         PgMsgTable,
         PgMsgDropTable
        > msg;
        PgReplMsgType msg_type;    // type defining union member
        int proto_version;         // which protocol version
    };

    /**
     * @brief Class for decoding postgres replication messages
     */
    class PgReplMsg
    {
    protected:
        // Proto V1; message flags, first byte
        static inline constexpr char MSG_BEGIN    = 'B';
        static inline constexpr char MSG_COMMIT   = 'C';
        static inline constexpr char MSG_RELATION = 'R';
        static inline constexpr char MSG_INSERT   = 'I';
        static inline constexpr char MSG_UPDATE   = 'U';
        static inline constexpr char MSG_DELETE   = 'D';
        static inline constexpr char MSG_TRUNCATE = 'T';
        static inline constexpr char MSG_ORIGIN   = 'O';
        static inline constexpr char MSG_MESSAGE  = 'M';
        static inline constexpr char MSG_TYPE     = 'Y';
        // Proto V2
        static inline constexpr char MSG_STREAM_START  = 'S';
        static inline constexpr char MSG_STREAM_STOP   = 'E';
        static inline constexpr char MSG_STREAM_COMMIT = 'c';
        static inline constexpr char MSG_STREAM_ABORT  = 'A';


        // Message prefixes
        static inline constexpr char MSG_PREFIX_CREATE_TABLE[] = "springtail CREATE TABLE";
        static inline constexpr char MSG_PREFIX_ALTER_TABLE[] = "springtail ALTER TABLE";
        static inline constexpr char MSG_PREFIX_DROP_TABLE[] = "springtail DROP TABLE";

        /** Protocol version */
        int _proto_version;

        /** In streaming mode, set true after Stream Start, set false after Stream Stop */
        bool _streaming = false;

        /** Internal buffer to decode messages out of, set by setBuffer() */
        const char *_buffer = nullptr;
        int _buffer_length = 0;

        /** Message struct used for returning decoded message */
        PgReplMsgDecoded _decoded_msg;

        /**
         * @brief Initialize message to empty/invalid message
         */
        inline void init_msg() {
            _decoded_msg.msg_type = PgReplMsgType::INVALID;
            _decoded_msg.proto_version = _proto_version;
        }

        // v1 messages
        int decode_begin();
        int decode_commit();
        int decode_relation();
        int decode_insert();
        int decode_update();
        int decode_delete();
        int decode_truncate();
        int decode_origin();
        int decode_message();
        int decode_type();

        // v2 messages
        int decode_stream_start();
        int decode_stream_stop();
        int decode_stream_commit();
        int decode_stream_abort();

        // decoded messages
        bool decode_create_table(PgMsgMessage &message);
        bool decode_alter_table(PgMsgMessage &message);
        bool decode_drop_table(PgMsgMessage &message);
        void decode_schema_columns(nlohmann::json &json, std::vector<PgMsgSchemaColumn> &columns);

        // helpers
        static int decode_tuple(const char *buffer, int length, PgMsgTupleData &tuple);
        static int decode_string(const char *buffer, int length, const char** str_out);

        static void dump_tuple(const PgMsgTupleData &tuple, std::stringstream &ss) noexcept;

    public:
        /**
         * @brief Constructor
         *
         * @param proto_version Postgres replication protocol version
         */
        PgReplMsg(int proto_version) noexcept;

        /**
         * @brief Set the internal buffer based on buffer passed in
         *
         * @param buffer pointer to buffer containing undecoded Pgmsg data
         * @param length length of buffer
         */
        void set_buffer(const char *buffer, int length) noexcept;

        /**
         * @brief Is there additional data that can be decoded
         *        within internal buffer
         * @return true if additional data exists; false otherwise
         */
        bool has_next_msg() noexcept;

        /**
         * @brief Retrieve next message from internal buffer
         * @return reference to internal decoded message
         * @throws PgMessageTooSmallError
         * @throws PgUnexpectedDataError
         * @throws PgUnknownMessageError
         */
        const PgReplMsgDecoded &decode_next_msg();

        /**
         * @brief convert a message to a printable string
         *
         * @param Pgmsg refernece to message to convert
         * @return readable string of Pgmsg
         */
        std::string dump_msg(const PgReplMsgDecoded &msg);

        /**
         * @brief Convert LSN to string of format XXX/XXX
         *
         * @param lsn LSN to convert
         * @return string of LSN in format: "XXX/XXX"
         */
        static std::string lsn_to_str(LSN_t lsn) noexcept;

        /**
         * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
         *
         * @param lsn_str string of LSN in format XXX/XXX
         * @return LSN_t
         */
        static LSN_t str_to_LSN(const char *lsn_str) noexcept;
    };

    class PgReplMsgStream : public PgReplMsg {
    public:
        struct PgTransaction {
            std::filesystem::path begin_path;
            std::filesystem::path commit_path;
            uint64_t begin_offset;
            uint64_t commit_offset;
            LSN_t xact_lsn;
            uint32_t xid;
        };
        using PgTransactionPtr = std::shared_ptr<PgTransaction>;

        /**
         * @brief Construct a new Pg Repl Msg Stream object
         * @param protoversion version of protocol to support
         */
        PgReplMsgStream(int protoversion) : PgReplMsg(protoversion) {}

        /**
         * @brief Set the log file, offset and size for parsing
         * @param path path of log file
         * @param offset offset of messages from start of file
         * @param size size of message chunk to parse
         */
        void set_log_file(std::filesystem::path path, uint64_t offset, uint64_t size);

        /**
         * @brief Scan the log specific from previous set_log_file()
         * @return std::vector<PgTransactionPtr> a list of transaction entries
         */
        std::vector<PgTransactionPtr> scan_log();

    private:

        // Proto V1; message lengths if fixed length; excludes first byte for opcode
        static inline constexpr int LEN_BEGIN    = (8 + 8 + 4);
        static inline constexpr int LEN_COMMIT   = (1 + 8 + 8 + 8);

        // Proto V2
        static inline constexpr int LEN_STREAM_START  = (4 + 1);
        static inline constexpr int LEN_STREAM_STOP   = 0;
        static inline constexpr int LEN_STREAM_COMMIT = (4 + 1 + 8 + 8 + 8);
        static inline constexpr int LEN_STREAM_ABORT  = (4 + 4 + 8 + 8);

        /** Helper to decode/skip a single message */
        void _scan_message();

        // skip messages
        void _skip_tuple();
        void _skip_string();
        void _skip_relation();
        void _skip_insert();
        void _skip_update();
        void _skip_delete();
        void _skip_truncate();
        void _skip_type();
        void _skip_origin();
        void _skip_message();

        /** Helper to seek stream based on current offset */
        void _seek_stream() {
            _stream.seekg(_current_offset, std::fstream::beg);
        }

        /** Read stream at current offset, return uint32_t */
        uint32_t _recvint32() {
            _seek_stream();
            uint32_t res = recvint32(_stream);
            _current_offset += 4;
            return res;
        }

        /** Read stream at current offset, return uint64_t */
        uint64_t _recvint64() {
            _seek_stream();
            uint64_t res = recvint64(_stream);
            _current_offset += 8;
            return res;
        }

        /** Read stream at current offset, return uint16_t */
        uint16_t _recvint16() {
            _seek_stream();
            uint16_t res = recvint16(_stream);
            _current_offset += 2;
            return res;
        }

        /** Read stream at current offset, return uint8_t */
        uint8_t _recvint8() {
            _seek_stream();
            uint8_t res = recvint8(_stream);
            _current_offset++;
            return res;
        }

        /** Read stream at current offset and copy data into buffer */
        void _read_buffer(char *buffer, int size) {
            _seek_stream();
            _stream.read(buffer, size);
            _current_offset += size;
        }

        /** Underlying file stream */
        std::fstream _stream;
        /** Underlying file path for stream */
        std::filesystem::path _current_path;
        /** Current offset within stream (from beginning) */
        uint64_t _current_offset;
        /** End offset of message block relative to beginning of stream */
        uint64_t _end_offset;
        /** Current message transaction if in non-streaming mode */
        PgTransactionPtr _current_xact = nullptr;
        /** Map of in progress transactions if in streaming mode */
        std::map<uint64_t, PgTransactionPtr> _xact_map;
        /** list of transactions encountered so far */
        std::vector<PgTransactionPtr> _committed_xacts;
    };
}