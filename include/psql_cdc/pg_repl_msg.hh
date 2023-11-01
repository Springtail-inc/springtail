#pragma once

#include <optional>
#include <psql_cdc/pg_types.hh>
#include <nlohmann/json.hpp>

namespace springtail
{
    /* Postgres message types, see decode functions in
     * PgReplMsg.cc for formats */
    struct MsgTupleDataColumn {
        char type;  // 'n' NULL, 'u' unchanged TOAST, 't' text formatted, 'b' binary
        int32_t data_len;
        const char *data;
    };

    struct MsgTupleData {
        int16_t num_columns;
        std::vector<MsgTupleDataColumn> tuple_data;
    };

    struct MsgKeepAlive {
        LSN_t wal_end;
        bool response_requested;
    };

    struct MsgBegin {
        int32_t xid;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct MsgCommit {
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct MsgOrigin {
        LSN_t commit_lsn;
        const char *name_str;
    };

    struct MsgCopy {
        LSN_t wal_start;
        LSN_t wal_end;
    };

    struct MsgTruncate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t num_rels;
        int8_t options; // 1 for cascade; 2 for restart identity
        std::vector<int32_t> rel_ids;
    };

    struct MsgType {
        int32_t xid;   // proto vers 2+ only if streaming
        uint32_t oid;
        const char *namespace_str;
        const char *data_type_str;
    };

    struct MsgMessage {
        int32_t xid;   // proto vers 2+ only if streaming
        int8_t flags;
        LSN_t lsn;
        const char *prefix_str;    // null terminated string
        int32_t data_len;
        const char *data;
    };

    struct MsgDelete {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        MsgTupleData tuple;
    };

    struct MsgInsert {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char new_type;
        MsgTupleData new_tuple;
    };

    struct MsgUpdate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char old_type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        MsgTupleData old_tuple;
        char new_type; // 'N' tuple data is new
        MsgTupleData new_tuple;
    };

    struct MsgRelColumn {
        int8_t flags;  // 0 or 1 - key
        const char *column_name;
        uint32_t oid;   // oid from pg_types table
        int32_t type_modifier; // pg_attribute.atttypmod type specific data; default -1
    };

    struct MsgRelation {
        int32_t xid;      // proto vers 2+ only if streaming
        int32_t rel_id;
        const char *namespace_str;
        const char *rel_name_str;
        int8_t identity;      // replica identit; pg_class.relreplident
        int16_t num_columns;
        std::vector<MsgRelColumn> columns;
    };

    // stream ops in proto vers 2+ only
    struct MsgStreamStart {
        int32_t xid;
        bool first;
    };

    struct MsgStreamStop {
        // empty
    };

    struct MsgStreamCommit {
        int32_t xid;
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    struct MsgStreamAbort {
        int32_t xid;
        int32_t sub_xid;
        LSN_t   abort_lsn;  // proto vers 4+
        int64_t abort_ts;   // proto vers 4+
    };

    struct MsgSchemaColumn {
        std::string column_name;
        std::string udt_type;
        std::optional<std::string> default_value;
        int position;        // position is maintained if column is renamed
        bool is_nullable;
        bool is_pkey;        // is primary key
    };

    struct MsgTable { // used by both create table and alter table
        uint32_t oid;
        LSN_t lsn;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string schema;
        std::string table;
        std::vector<MsgSchemaColumn> columns;
    };

    struct MsgDropTable {
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
     *          specified by the msg_type
     */
    struct PgReplMsgDecoded {
        std::variant<
            MsgKeepAlive,
            MsgBegin,
            MsgCommit,
            MsgOrigin,
            MsgCopy,
            MsgMessage,
            MsgType,
            MsgTruncate,
            MsgDelete,
            MsgUpdate,
            MsgInsert,
            MsgRelation,
            MsgStreamStart,
            MsgStreamStop,
            MsgStreamCommit,
            MsgStreamAbort,
            MsgTable,
            MsgDropTable
        > msg;
        PgReplMsgType msg_type;    // type defining union member
        int proto_version;         // which protocol version
    };

    /**
     * @brief Class for decoding postgres replication messages
     */
    class PgReplMsg
    {

    private:
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
        bool decode_create_table(MsgMessage &message);
        bool decode_alter_table(MsgMessage &message);
        bool decode_drop_table(MsgMessage &message);
        void decode_schema_columns(nlohmann::json &json, std::vector<MsgSchemaColumn> &columns);

        // helpers
        static int decode_tuple(const char *buffer, int length, MsgTupleData &tuple);
        static int decode_string(const char *buffer, int length, const char** str_out);

        static void dump_tuple(const MsgTupleData &tuple, std::stringstream &ss) noexcept;

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
         * @param buffer pointer to buffer containing undecoded msg data
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
         * @param msg refernece to message to convert
         * @return readable string of msg
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
}