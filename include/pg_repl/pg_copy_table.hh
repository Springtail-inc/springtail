#pragma once

#include <cstdio>
#include <string>
#include <memory>
#include <vector>
#include <optional>

#include <pg_repl/libpq_connection.hh>

#include <storage/field.hh>

namespace springtail
{
    /** Stores the result of a copy operation */
    struct PgCopyResult {
        std::vector<int32_t> tids;  ///< table ids
        std::string pg_xids;        ///< xids from pg_current_snapshot()
        uint64_t target_xid;        ///< target xid

        PgCopyResult(const std::vector<int32_t> &tids,
                     const std::string &pg_xids,
                     uint64_t target_xid) :
            tids(tids), pg_xids(pg_xids), target_xid(target_xid) {}
    };
    using PgCopyResultPtr = std::shared_ptr<PgCopyResult>;

    /** Stores the column schema for the table being copied */
    struct PgColumn {
        std::string name;
        int32_t pg_type;
        std::optional<std::string> default_value;
        int32_t position;
        bool is_nullable;
        bool is_pkey;
    };

    /** Stores the table schema for table being copied */
    struct PgTableSchema {
        std::string db_name;
        std::string schema_name;
        std::string table_name;
        std::string xids;                // pg_current_snapshot(); xmin:xmax:xids
        int32_t table_oid;
        std::vector<PgColumn> columns;
        std::vector<std::string> pkeys;  // primary keys as columns
        std::vector<std::vector<std::string>> secondary_keys;  // secondary keys as columns
    };

    /**
     * @brief Serialize data for a single table from a remote
     * Postgres server to a file, and de-serialize from a file
     */
    class PgCopyTable {

    private:
        /** header of copy data signature */
        static inline constexpr char COPY_SIGNATURE[] = "PGCOPY\n\377\r\n\0";

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
                         uint64_t table_oid);

        /**
         * @brief Get transaction ids for current transaction snapshot
         * @details calls: SELECT txid_current_snapshot() returns string
         *          in format: "xid_min:xid_max:xid,xid,xid" showing set of
         *          transactions overlapping with current transaction
         * @return std::string xid string from pg_current_snapshot()
         * @throws PgQueryError
         */
        std::string _get_xact_xids();

        /**
         * @brief Get table's oid based on schema / table, store in schema
         */
        void _get_table_oid();

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
         * @brief Convert pg columns to internal pg msg schema columns
         * @param pg_columns input pg columns
         * @param pkeys primary keys
         * @return std::vector<PgMsgSchemaColumn>
         */
        std::vector<PgMsgSchemaColumn> _map_to_pg_msg(const std::vector<PgColumn> pg_columns,
                                                      const std::vector<std::string> pkeys);

        /**
         * find element in vector and get distance from begin iterator
         * used to find primary key position
         */
        int _get_vec_pos(const std::vector<std::string> vec, const std::string element);

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
                             std::vector<std::tuple<std::string, std::string, int32_t>> &table_oids);

        /**
         * @brief Copy table from remote system
         */
        void _copy_table(uint64_t db_id,
                         springtail::XidLsn &xid,
                         const std::string &table_name,
                         const std::string &schema_name,
                         uint64_t table_oid);

        /**
         * @brief Internall helper called from copy_db, copy_schema, copy_table
         * @param db_id database id
         * @param schema_name schema name (optional)
         * @param table_oid table oid (optional)
         * @return PgCopyResultPtr
         */
        static PgCopyResultPtr _internal_copy(uint64_t db_id,
                                              std::optional<std::string> schema_name,
                                              std::optional<std::pair<std::string, std::string>> table_name,
                                              std::optional<uint32_t> table_oid);

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
         * @param table_oid table oid
         * @return PgCopyResultPtr
         */
        static PgCopyResultPtr copy_db(uint64_t db_id);

        /**
         * @brief Copy all tables in single schema from remote system
         * @param db_id database id
         * @param table_oid table oid
         * @return PgCopyResultPtr
         */
        static PgCopyResultPtr copy_schema(uint64_t db_id,
                                           const std::string &schema_name);

        /**
         * @brief Copy a single table from remote system
         * @param db_id database id
         * @param table_oid table oid
         * @return PgCopyResultPtr
         */
        static PgCopyResultPtr copy_table(uint64_t db_id,
                                          uint32_t table_oid);

        /**
         * @brief Copy a single table from remote system
         * @param db_id database id
         * @param schema_name schema name
         * @param table_name table name
         * @return PgCopyResultPtr
         */
        static PgCopyResultPtr copy_table(uint64_t db_id,
                                          const std::string &schema_name,
                                          const std::string &table_name);
    };
}
