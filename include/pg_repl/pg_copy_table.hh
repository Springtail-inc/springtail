#pragma once

#include <cstdio>
#include <string>
#include <memory>
#include <vector>
#include <optional>

#include <nlohmann/json.hpp>

#include <common/common.hh>

#include <pg_repl/libpq_connection.hh>

#include <storage/field.hh>
#include <storage/xid.hh>

namespace springtail
{
    /** Stores the result of a copy operation */
    struct PgCopyResult {
        uint64_t target_xid;         ///< target xid

        // see: https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT
        uint32_t xmin;               ///< xmin; lowest xid still active
        uint32_t xmax;               ///< xmax; one past highest completed xid
        uint32_t xmin_epoch;
        uint32_t xmax_epoch;
        uint32_t pg_xid;
        uint32_t pg_epoch;
        std::vector<int32_t> tids;   ///< table ids
        std::vector<uint32_t> xips;  ///< transactions in progress: xmin <= X < xmax
        std::string pg_xids;         ///< pg_current_snapshot(); xmin:xmax:xids

        explicit PgCopyResult(uint64_t target_xid) : target_xid(target_xid) {}

        /**
         * @brief Add table to result
         * @param tid table id
         */
        void add_table(int32_t tid)
        {
            tids.push_back(tid);
        }

        /**
         * @brief Set snapshot information
         * @param pg_xid8 pg_xid
         * @param pg_xids xmin:xmax:xids
         */
        void set_snapshot(uint64_t pg_xid8, const std::string &pg_xids)
        {
            this->pg_xids = pg_xids;

            // parse pg_xid
            this->pg_xid = static_cast<uint32_t>(pg_xid8 & 0xFFFFFFFFLL);
            this->pg_epoch = static_cast<uint32_t>(pg_xid8 >> 32);

            // parse xids: xmin:xmax:xids
            std::vector<std::string> xid_parts;
            common::split_string(":", pg_xids, xid_parts);

            // parse xid parts
            // xids are xid8 which are 64 bit values, bottom 32 bits are xid, top 32 bits are epoch
            uint64_t xmin8 = std::strtoull(xid_parts[0].c_str(), nullptr, 10);
            uint64_t xmax8 = std::strtoull(xid_parts[1].c_str(), nullptr, 10);
            xmin = static_cast<uint32_t>(xmin8 & 0xFFFFFFFFLL);
            xmax = static_cast<uint32_t>(xmax8 & 0xFFFFFFFFLL);
            xmin_epoch = static_cast<uint32_t>(xmin8 >> 32);
            xmax_epoch = static_cast<uint32_t>(xmax8 >> 32);

            // parse xips (in progress xids)
            if (xid_parts.size() > 2 && !xid_parts[2].empty()) {
                std::vector<std::string> xip_parts;
                common::split_string(",", xid_parts[2], xip_parts);
                for (const auto &xip : xip_parts) {
                    uint64_t xip8 = std::strtoull(xip.c_str(), nullptr, 10);
                    xips.push_back(static_cast<uint32_t>(xip8 & 0xFFFFFFFFLL));
                }
            }
        }
    };
    using PgCopyResultPtr = std::shared_ptr<PgCopyResult>;

    /** Stores the table schema for table being copied */
    struct PgTableSchema {
        std::string db_name;
        std::string schema_name;
        std::string table_name;
        std::string xids;                // pg_current_snapshot(); xmin:xmax:xids
        uint64_t table_oid;
        uint64_t schema_oid;
        std::vector<SchemaColumn> columns;
        std::vector<std::string> pkeys;  // primary keys as columns
        std::vector<Index> secondary_keys;  // secondary keys as columns
    };

    /**
     * @brief Serialize data for a single table from a remote
     * Postgres server to a file, and de-serialize from a file
     */
    class PgCopyTable {

    private:
        /** header of copy data signature */
        static inline constexpr char COPY_SIGNATURE[] = "PGCOPY\n\377\r\n\0";

        /** number of worker threads; XXX hardcoded for now */
        static constexpr int WORKER_THREADS = 4;

        /** Request to worker thread to process table */
        struct CopyRequest {
            std::string table_name;
            std::string schema_name;
            int32_t table_oid;
            int32_t schema_oid;
        };
        using CopyRequestPtr = std::shared_ptr<CopyRequest>;
        using CopyQueue = ConcurrentQueue<CopyRequest>;
        using CopyQueuePtr = std::shared_ptr<CopyQueue>;

        /** Struct for holding table metadata */
        struct TableMetadata {
            std::string namespace_name;
            std::string table_name;
            uint32_t namespace_oid;
            uint32_t table_oid;

            TableMetadata(std::string_view n_name,
                          std::string_view t_name,
                          uint32_t n_pgoid,
                          uint32_t t_pgoid)
                : namespace_name(n_name),
                  table_name(t_name),
                  namespace_oid(n_pgoid),
                  table_oid(t_pgoid)
            { }
            TableMetadata() = default;

            // Operator to sort the table metadata by table oid
            bool operator<(const TableMetadata &table_metadata) const{
                return table_oid < table_metadata.table_oid;
            }
        };

        LibPqConnection _connection;
        std::string _db_name;
        std::string _schema_name;
        std::string _table_name;

        bool _oid_flag = false;

        PgTableSchema _schema;

        /**
         * @brief Extract schema from table and store in internal _schema object
         * @details Uses atttypid from pg_attribute table for identifier of the type.
         *          Saves the column name, ordinal position, default value (as string), column type
         *          and is_nullable flag for each table column.  Requires getTableOid() first.
         *
         */
        void _set_schema(const std::string &table_name,
                         const std::string &schema_name,
                         uint64_t table_oid,
                         uint64_t schema_oid);

        /**
         * @brief Get transaction ids for current transaction snapshot
         * @details calls: SELECT pg_current_xact_id(), pg_current_snapshot()
         *          to get current transaction pg_xid and
         *          "xid_min:xid_max:xid,xid,xid" showing set of
         *          transactions overlapping with current transaction
         * @return std::pair<uint64_t, std::string> pg_xid, xid string from pg_current_snapshot()
         * @throws PgQueryError
         */
        std::pair<uint64_t, std::string> _get_xact_xids();

        /**
         * @brief Get secondary index columns for table by oid
         */
        void _get_secondary_indexes();

        /**
         * @brief Execute copy query
         */
        void _prepare_copy();

        /**
         * @brief Get copy data from connection using copy buffer
         * Copy buffer should be released with _release_data()
         * @return std::optional<std::string_view> buffer containing data
         */
        std::optional<std::string_view> _get_next_data();

        /**
         * @brief Free the copy buffer from _get_next_data()
         */
        void _release_data();

        /**
         * @brief Parse row received from copy table command
         * @param row input row (copy buffer)
         * @param pos position in row to start parsing (in/out)
         */
        FieldArrayPtr _parse_row(const std::string_view &row, size_t &pos);

        /**
         * Validate copy header
         * @details Header contents:
         *          11B signature starts with COPY_SIGNATURE
         *           4B flags; bit 16 oid flag
         *           4B header extension length
         */
        int32_t _verify_copy_header(const std::string_view &header);

        /**
         * @brief Get table oids based on query passed in
         * @param query query to get table oids
         * @param table_oids output: table name, schema name, oid
         */
        void _get_table_oids(const std::string &query,
                             std::vector<TableMetadata> &table_oids);

        /**
         * @brief Get table oids based on json specifying schema and table includes
         * @param include_json json object specifying schema and table includes
         * @param table_oids output: table name, schema name, oid
         */
        void _get_table_oids(const nlohmann::json &include_json,
                             std::vector<TableMetadata> &table_oids);

        /**
         * @brief Copy table from remote system
         */
        void _copy_table(uint64_t db_id,
                         springtail::XidLsn &xid,
                         const std::string &table_name,
                         const std::string &schema_name,
                         uint64_t table_oid,
                         uint64_t schema_oid);

        /**
         * @brief End the copy, commit the transaction
         */
        void _end_copy();

        /**
         * @brief Send sync replication message
         */
        void _send_sync_msg(PgCopyResultPtr result);

        /**
         * @brief Worker thread for copying tables
         * Listens on copy_queue for table copy requests
         * @param db_id database id
         * @param target_xid target xid
         * @param copy_queue queue of copy requests
         * @param result copy result (output)
         */
        void _worker(uint64_t db_id,
                     uint64_t target_xid,
                     CopyQueuePtr copy_queue,
                     PgCopyResultPtr result);

        /**
         * @brief Get namespaces, returns a pair of namespace name and oid
         * @param db_id database id
         * @param xid xid
         */
        std::vector<std::pair<uint64_t, std::string>> _get_namespaces(uint64_t db_id, uint64_t xid);

        /**
         * @brief Internall helper called from copy_db, copy_schema, copy_table
         * @param db_id database id
         * @param target_xid target xid
         * @param schema_name schema name (optional)
         * @param table_oid table oid (optional)
         * @return PgCopyResultPtrs
         */
        static std::vector<PgCopyResultPtr> _internal_copy(uint64_t db_id,
            uint64_t target_xid,
            std::optional<std::string> schema_name = std::nullopt,
            std::optional<std::pair<std::string, std::string>> table_name = std::nullopt,
            std::optional<std::vector<uint32_t>> table_oids = std::nullopt,
            std::optional<nlohmann::json> include_json = std::nullopt);

    public:

        /**
         * @brief Constructor for copying table from remote system
         */
        PgCopyTable() {}

        /**
         * @brief Constructor for copying table from remote system
         * @param db_name name of the database
         */
        PgCopyTable(const std::string &db_name) : _db_name(db_name) {}

        ~PgCopyTable()
        {
            // release underlying connection if connected
            _connection.disconnect();
        }

        /**
         * @brief Connect to database; call prior to copyToFile
         * @param hostname DB hostname
         * @param username DB username
         * @param password DB password
         * @param port     DB port
         */
        void connect(const std::string &hostname,
                     const std::string &username,
                     const std::string &password,
                     const int port);

        /**
         * @brief Connect to database, get config from Properties
         * @param db_id database id
         */
        void connect(uint64_t db_id);

        /**
         * @brief Disconnect connection; should be done after copy is finished
         */
        void disconnect();

        /**
         * @brief Copy all tables from remote system
         * @param db_id database id
         * @param xid target xid
         * @return PgCopyResultPtr
         */
        static std::vector<PgCopyResultPtr>
            copy_db(uint64_t db_id, uint64_t xid);

        /**
         * @brief Create namespaces
         * @param db_id database id
         * @param xid xid
         */
        static void create_namespaces(uint64_t db_id, uint64_t xid);

        /**
         * @brief Copy all tables in single schema from remote system
         * @param db_id database id
         * @param xid target xid
         * @param table_oid table oid
         * @return PgCopyResultPtr
         */
        static std::vector<PgCopyResultPtr>
            copy_schema(uint64_t db_id, uint64_t xid,
                        const std::string &schema_name);

        /**
         * @brief Copy a single table from remote system
         * @param db_id database id
         * @param xid target xid
         * @param table_oid table oids (vector)
         * @return PgCopyResultPtr
         */
        static std::vector<PgCopyResultPtr>
            copy_tables(uint64_t db_id, uint64_t xid,
                        std::vector<uint32_t> table_oids);

        /**
         * @brief Copy a single table from remote system
         * @param db_id database id
         * @param xid target xid
         * @param schema_name schema name
         * @param table_name table name
         * @return PgCopyResultPtr
         */
        static std::vector<PgCopyResultPtr>
            copy_table(uint64_t db_id, uint64_t xid,
                       const std::string &schema_name,
                       const std::string &table_name);

        /**
         * @brief Copy tables/schemas based on json specifying
         *        schema and table includes
         * @param db_id database id
         * @param xid target xid
         * @param include_json json object specifying schema and table includes
         * @return std::vector<PgCopyResultPtr>
         */
        static std::vector<PgCopyResultPtr>
            copy_table(uint64_t db_id, uint64_t xid,
                       const nlohmann::json &include_json);
    };
}
