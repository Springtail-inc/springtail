#pragma once

#include <optional>
#include <variant>

#include <pg_repl/pg_types.hh>
#include <nlohmann/json.hpp>
#include <storage/schema_type.hh>

namespace springtail
{
    /* Postgres message types, see decode functions in
     * PgRep PgMsg.cc for formats */

    /** Tuple data for a single column */
    struct PgMsgTupleDataColumn {
        char type;  // 'n' NULL, 'u' unchanged TOAST, 't' text formatted, 'b' binary
        std::vector<char> data;
    };

    /** Tuple data for a row (set of columns) */
    struct PgMsgTupleData {
        std::vector<PgMsgTupleDataColumn> tuple_data;
    };

    /** Keep alive message */
    struct PgMsgKeepAlive {
        LSN_t wal_end;
        bool response_requested;
    };

    /** Begin transaction message, not streaming */
    struct PgMsgBegin {
        int32_t xid;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    /** Commit message not streaming */
    struct PgMsgCommit {
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
    };

    /** Origin message */
    struct PgMsgOrigin {
        LSN_t commit_lsn;
        std::string name_str;
    };

    /** Copy message */
    struct PgMsgCopy {
        LSN_t wal_start;
        LSN_t wal_end;
    };

    /** Truncate message */
    struct PgMsgTruncate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t num_rels;
        std::vector<int32_t> rel_ids;
        int8_t options; // 1 for cascade; 2 for restart identity
    };

    /** Type message for an OID */
    struct PgMsgType {
        int32_t xid;   // proto vers 2+ only if streaming
        uint32_t oid;
        std::string namespace_str;
        std::string data_type_str;
    };

    /** Message message -- shouldn't be used; DDL messages are transformed into other types */
    struct PgMsgMessage {
        LSN_t lsn;
        int32_t xid;   // proto vers 2+ only if streaming
        int8_t flags;
        std::string prefix_str;    // null terminated string
        // don't store data as we wouldn't know how to interpret it
    };

    /** Delete row message */
    struct PgMsgDelete {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        PgMsgTupleData tuple;
    };

    /** Insert row message */
    struct PgMsgInsert {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char new_type;
        PgMsgTupleData new_tuple;
    };

    /** Update row message */
    struct PgMsgUpdate {
        int32_t xid;   // proto vers 2+ only if streaming
        int32_t rel_id;
        char old_type; // 'K' tuple data is a key, 'O' tuple data is an old tuple
        char new_type; // 'N' tuple data is new
        PgMsgTupleData old_tuple;
        PgMsgTupleData new_tuple;
    };

    /** Column relation message -- defines a single column */
    struct PgMsgRelColumn {
        uint32_t oid;   // oid from pg_types table
        int32_t type_modifier; // pg_attribute.atttypmod type specific data; default -1
        int8_t flags;  // 0 or 1 - key
        std::string column_name;
    };

    /** Relation message for a table contains a set of columns */
    struct PgMsgRelation {
        int32_t xid;      // proto vers 2+ only if streaming
        int32_t rel_id;
        int8_t identity;      // replica identit; pg_class.relreplident
        std::string namespace_str;
        std::string rel_name_str;
        std::vector<PgMsgRelColumn> columns;
    };

    /** Stream start -- in proto vers 2+ only */
    struct PgMsgStreamStart {
        int32_t xid;
        bool first;
    };

    /** Stream stop -- in proto vers 2+ only */
    struct PgMsgStreamStop {
        // empty
    };

    /** Stream commit -- in proto vers 2+ only */
    struct PgMsgStreamCommit {
        LSN_t commit_lsn;
        LSN_t xact_lsn;
        int64_t commit_ts;
        int32_t xid;
    };

    /** Stream abort message -- in proto vers 2+ only --
     * may be for a subtransaction (if xid != sub_xid)
     */
    struct PgMsgStreamAbort {
        LSN_t   abort_lsn;  // proto vers 4+
        int64_t abort_ts;   // proto vers 4+
        int32_t xid;
        int32_t sub_xid;
    };

    /** Column schema for a single column used by Create table and Alter table */
    struct PgMsgSchemaColumn {
        std::string name;
        uint8_t type; // springtail schema type
        int32_t pg_type; // postgres type oid
        std::optional<std::string> default_value;
        int position;        // position is maintained if column is renamed
        int pk_position;     // position in primary key, if is_pkey
        bool is_nullable;
        bool is_pkey;        // is primary key
        bool is_generated;  // is this a generated column
        bool is_non_standard_collation;
        bool is_user_defined_type;
        std::string type_name;
        std::string collation;
    };

    /** Create table/alter table message decoded */
    struct PgMsgTable {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string namespace_name;
        std::string table;
        std::vector<PgMsgSchemaColumn> columns;
    };

    /** Drop table message decoded */
    struct PgMsgDropTable {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string namespace_name;
        std::string table;
    };

    /** Column schema for a single column used by Create index */
    struct PgMsgSchemaIndexColumn {
        std::string name;
        int position;        // position is maintained if column is renamed
        int idx_position;    // position in the index
    };

    struct PgMsgIndex {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string index;
        bool is_unique;
        uint32_t table_oid;
        std::string table_name;
        std::string namespace_name;
        std::vector<PgMsgSchemaIndexColumn> columns;
    };

    struct PgMsgDropIndex {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid;        // proto vers 2+ only if streaming
        std::string index;
        std::string namespace_name;
    };

    struct PgMsgNamespace {
        LSN_t lsn;
        uint32_t oid;
        int32_t xid; // pg xid; proto vers 2+ only if streaming
        std::string name;
    };

    struct PgMsgCopySync {
        int64_t target_xid;
        int32_t pg_xid;
    };

    struct PgMsgReconcileIndex{
        uint64_t db_id;
        uint64_t reconcile_xid;
    };

    /** Message types */
    enum PgMsgEnum {
        INVALID=-1, COPY_HDR, KEEP_ALIVE, BEGIN, COMMIT, RELATION, INSERT, DELETE, UPDATE, TRUNCATE,
        ORIGIN, MESSAGE, TYPE,
        // version 2
        STREAM_START, STREAM_STOP, STREAM_COMMIT, STREAM_ABORT,
        // decoded messages
        CREATE_TABLE, ALTER_TABLE, DROP_TABLE,
        CREATE_NAMESPACE, ALTER_NAMESPACE, DROP_NAMESPACE,
        CREATE_INDEX, DROP_INDEX, COPY_SYNC,
        RECONCILE_INDEX, // Custom committer notifiers
        ALTER_RESYNC // generated when an invalid table becomes valid due to an ALTER TABLE
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
         PgMsgDropTable,
         PgMsgIndex,
         PgMsgDropIndex,
         PgMsgNamespace,
         PgMsgCopySync,
         PgMsgReconcileIndex
        > msg;                 ///< message data

        /** timestamp id of the current Postgres log file -- will be zero for internal messages */
        uint64_t pg_log_timestamp{0};
        PgMsgEnum msg_type;    ///< type defining union member
        int proto_version;     ///< which protocol version
        bool is_streaming;     ///< is this a streaming message

        PgMsg(PgMsgEnum type=PgMsgEnum::INVALID)
            : msg_type(type) {}
    };
    using PgMsgPtr = std::shared_ptr<PgMsg>;

    /**
     * @brief Start end offset/file pairs for a transaction
     * Also includes set of oids changed within the transaction
     */
    struct PgTransaction {
        /** Transaction record type */
        enum : uint8_t {
            TYPE_COMMIT = 0,        ///< normal commit or stream commit
            TYPE_STREAM_START = 1,  ///< stream start
            TYPE_STREAM_ABORT = 2,  ///< stream abort
            TYPE_PIPELINE_STALL = 3 ///< stalls the pipeline for table sync
        };

        LSN_t xact_lsn;  ///< xact lsn (final lsn of xact; begin_msg.xact_lsn)
        uint8_t type;    ///< transaction record type (see enum above)
        uint32_t xid;    ///< postgres xid

        PgTransaction(uint8_t type) : type(type) {}

        PgTransaction() = default;
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
        static inline constexpr char MSG_PREFIX_CREATE_INDEX[] = "springtail:CREATE INDEX";
        static inline constexpr char MSG_PREFIX_DROP_INDEX[] = "springtail:DROP INDEX";
        static inline constexpr char MSG_PREFIX_COPY_SYNC[] = "springtail:COPY SYNC";
        static inline constexpr char MSG_PREFIX_CREATE_NAMESPACE[] = "springtail:CREATE SCHEMA";
        static inline constexpr char MSG_PREFIX_ALTER_NAMESPACE[] = "springtail:ALTER SCHEMA";
        static inline constexpr char MSG_PREFIX_DROP_NAMESPACE[] = "springtail:DROP SCHEMA";

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

        /**
         * @brief Convert a PG type to a Springtail type.
         * @param pg_type The PG column type OID.
         * @return A springtail SchemaType.
         */
        SchemaType convert_pg_type(int32_t pg_type);
    };
}
