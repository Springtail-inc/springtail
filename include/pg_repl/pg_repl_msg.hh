#pragma once

#include <optional>
#include <variant>
#include <set>

#include <pg_repl/pg_types.hh>
#include <nlohmann/json.hpp>

namespace springtail
{
    /* Postgres message types, see decode functions in
     * PgRep PgMsg.cc for formats */
    struct PgMsgTupleDataColumn {
        char type;  // 'n' NULL, 'u' unchanged TOAST, 't' text formatted, 'b' binary
        std::vector<char> data;
    };

    struct PgMsgTupleData {
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
        std::string name_str;
    };

    struct PgMsgCopy {
        LSN_t wal_start;
        LSN_t wal_end;
    };

    struct PgMsgTruncate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t num_rels;
        std::vector<int32_t> rel_ids;
        int8_t options; // 1 for cascade; 2 for restart identity
    };

    struct PgMsgType {
        int32_t xid;   // proto vers 2+ only if streaming
        uint32_t oid;
        std::string namespace_str;
        std::string data_type_str;
    };

    struct PgMsgMessage {
        LSN_t lsn;
        int32_t xid;   // proto vers 2+ only if streaming
        int8_t flags;
        std::string prefix_str;    // null terminated string
        // don't store data as we wouldn't know how to interpret it
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
        char new_type; // 'N' tuple data is new
        PgMsgTupleData old_tuple;
        PgMsgTupleData new_tuple;
    };

    struct PgMsgRelColumn {
        uint32_t oid;   // oid from pg_types table
        int32_t type_modifier; // pg_attribute.atttypmod type specific data; default -1
        int8_t flags;  // 0 or 1 - key
        std::string column_name;
    };

    struct PgMsgRelation {
        int32_t xid;      // proto vers 2+ only if streaming
        int32_t rel_id;
        int8_t identity;      // replica identit; pg_class.relreplident
        std::string namespace_str;
        std::string rel_name_str;
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
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
        int32_t xid;
    };

    struct PgMsgStreamAbort {
        LSN_t   abort_lsn;  // proto vers 4+
        int64_t abort_ts;   // proto vers 4+
        int32_t xid;
        int32_t sub_xid;
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
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string schema;
        std::string table;
        std::vector<PgMsgSchemaColumn> columns;
    };

    struct PgMsgDropTable {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string schema;
        std::string table;
    };


    /** Message types */
    enum PgMsgEnum {
        INVALID=-1, COPY_HDR, KEEP_ALIVE, BEGIN, COMMIT, RELATION, INSERT, DELETE, UPDATE, TRUNCATE,
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
    struct PgMsg {
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

        PgMsgEnum msg_type;    // type defining union member
        int proto_version;     // which protocol version
        bool is_streaming;     // is this a streaming message

        PgMsg(PgMsgEnum type=PgMsgEnum::INVALID)
            : msg_type(type) {}
    };
    using PgMsgPtr = std::shared_ptr<PgMsg>;

    /**
     * @brief Start end offset/file pairs for a transaction
     * Also includes set of oids changed within the transaction
     */
    struct PgTransaction {
        enum : uint8_t {
            TYPE_COMMIT = 0,
            TYPE_STREAM_START = 1,
            TYPE_STREAM_ABORT = 2
        };

        uint64_t begin_offset;   ///< offset to start of block header
        uint64_t commit_offset;  ///< offset to end of commit msg
        uint64_t springtail_xid; ///< springtail xid
        LSN_t xact_lsn;
        uint8_t type;
        uint32_t xid;
        std::set<uint64_t> oids;
        std::set<uint32_t> aborted_xids; // stream subxacts that aborted
        std::filesystem::path begin_path;
        std::filesystem::path commit_path;
    };
    using PgTransactionPtr = std::shared_ptr<PgTransaction>;

    /**
     * @brief Class for decoding postgres replication messages
     */
    namespace pg_msg {

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
        static inline constexpr char MSG_PREFIX_SPRINGTAIL[] = "springtail:";
        static inline constexpr char MSG_PREFIX_CREATE_TABLE[] = "springtail:CREATE TABLE";
        static inline constexpr char MSG_PREFIX_ALTER_TABLE[] = "springtail:ALTER TABLE";
        static inline constexpr char MSG_PREFIX_DROP_TABLE[] = "springtail:DROP TABLE";


        /**
         * @brief convert a message to a printable string
         *
         * @param Pgmsg refernece to message to convert
         * @return readable string of Pgmsg
         */
        std::string dump_msg(const PgMsg &msg);

        /**
         * @brief Convert LSN to string of format XXX/XXX
         *
         * @param lsn LSN to convert
         * @return string of LSN in format: "XXX/XXX"
         */
        std::string lsn_to_str(LSN_t lsn) noexcept;

        /**
         * @brief Convert LSN in string format XXX/XXX to LSN_t (uint64_t)
         *
         * @param lsn_str string of LSN in format XXX/XXX
         * @return LSN_t
         */
        LSN_t str_to_LSN(const char *lsn_str) noexcept;
    };
}
