#pragma once

#include <psql_cdc/pg_types.hh>

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
        int32_t xid;   // proto vers 2+ only
        int32_t num_rels;
        int8_t options; // 1 for cascade; 2 for restart identity
        std::vector<int32_t> rel_ids;
    };

    struct MsgType {
        int32_t xid;   // proto vers 2+ only
        int32_t oid;
        const char *namespace_str;
        const char *data_type_str;
    };

    struct MsgMessage {
        int32_t xid;   // proto vers 2+ only
        int8_t flags;
        LSN_t lsn;
        const char *prefix_str;    // null terminated string
        int32_t data_len;
        const char *data;
    };

    struct MsgDelete {
        int32_t xid;   // proto vers 2+ only
        int32_t rel_id;
        char type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        MsgTupleData tuple;
    };

    struct MsgInsert {
        int32_t xid;   // proto vers 2+ only
        int32_t rel_id;
        char new_type;
        MsgTupleData new_tuple;
    };

    struct MsgUpdate {
        int32_t xid;   // proto vers 2+ only
        int32_t rel_id;
        char old_type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        MsgTupleData old_tuple;
        char new_type; // 'N' tuple data is new
        MsgTupleData new_tuple;
    };

    struct MsgRelColumn {
        int8_t flags;  // 0 or 1 - key
        const char *column_name;
        int32_t oid;   // oid from pg_types table
        int32_t type_modifier; // pg_attribute.atttypmod type specific data; default -1
    };

    struct MsgRelation {
        int32_t xid;      // proto vers 2+ only
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


    /** Message types */
    enum PgReplMsgType {
        INVALID, COPY_HDR, KEEP_ALIVE, BEGIN, COMMIT, RELATION, INSERT, DELETE, UPDATE, TRUNCATE,
        ORIGIN, MESSAGE, TYPE,
        // version 2
        STREAM_START, STREAM_STOP, STREAM_COMMIT, STREAM_ABORT
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
            MsgStreamAbort
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

        /** Protocol version */
        int _proto_version;

        /** Internal buffer to decode messages out of, set by setBuffer() */
        const char *_buffer = nullptr;
        int _buffer_length = 0;

        /** Message struct used for returning decoded message */
        PgReplMsgDecoded _decoded_msg;

        /**
         * @brief Initialize message to empty/invalid message
         */
        inline void initMsg() {
            _decoded_msg.msg_type = PgReplMsgType::INVALID;
            _decoded_msg.proto_version = _proto_version;
        }

        // v1 messages
        int decodeBegin(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeCommit(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeRelation(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeInsert(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeUpdate(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeDelete(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeTruncate(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeOrigin(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeMessage(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeType(const char *buffer, int length, PgReplMsgDecoded &msg);

        // v2 messages
        int decodeStreamStart(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeStreamStop(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeStreamCommit(const char *buffer, int length, PgReplMsgDecoded &msg);
        int decodeStreamAbort(const char *buffer, int length, PgReplMsgDecoded &msg);

        // helpers
        static int decodeTuple(const char *buffer, int length, MsgTupleData &tuple);
        static int decodeString(const char *buffer, int length, const char** str_out);

        static void dumpTuple(const MsgTupleData &tuple, std::stringstream &ss);

    public:
        /**
         * @brief Constructor
         *
         * @param proto_version Postgres replication protocol version
         */
        PgReplMsg(int proto_version);

        /**
         * @brief Set the internal buffer based on buffer passed in
         *
         * @param buffer pointer to buffer containing undecoded msg data
         * @param length length of buffer
         */
        void setBuffer(const char *buffer, int length);

        /**
         * @brief Is there additional data that can be decoded
         *        within internal buffer
         * @return true if additional data exists; false otherwise
         */
        bool hasNextMsg();

        /**
         * @brief Retrieve next message from internal buffer
         * @return reference to internal decoded message
         */
        const PgReplMsgDecoded &decodeNextMsg();

        /**
         * @brief convert a message to a printable string
         *
         * @param msg refernece to message to convert
         * @return readable string of msg
         */
        static std::string dumpMsg(const PgReplMsgDecoded &msg);

        /**
         * @brief Convert LSN to string of format XXX/XXX
         *
         * @param lsn LSN to convert
         * @return string of LSN in format: "XXX/XXX"
         */
        static std::string lsnToStr(LSN_t lsn);

        /**
         * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
         *
         * @param lsn_str string of LSN in format XXX/XXX
         * @return LSN_t
         */
        static LSN_t strToLSN(const char *lsn_str);
    };
}