#pragma once

#include <filesystem>
#include <map>

#include <sys_tbl_mgr/table.hh>


namespace springtail::test {
    class FSCheck
    {
    public:
        FSCheck();
        ~FSCheck() = default;

        /**
         * @brief Check all databases
         *
         */
        void
        check_dbs();

        /**
         * @brief Check given database
         *
         * @param db_id - database id
         */
        void
        check_db(uint64_t db_id);

        /**
         * @brief Check given table in the given database
         *
         * @param db_id - database id
         * @param table_id - table id
         */
        void
        check_db_table(uint64_t db_id, uint64_t table_id);

    private:
        std::map<uint64_t, std::string> _databases;     ///< map of database id to database name
        std::filesystem::path _table_base;              ///< directory where all the tables are stored

        struct FSNamespace
        {
            uint64_t ns_id;
            std::string ns_name;
            uint64_t xid;
            uint64_t lsn;
            bool exists;
        };

        struct FSRoot
        {
            uint64_t xid;
            uint64_t extent_id;
            uint64_t snapshot_xid;
        };

        struct FSStats
        {
            uint64_t xid;
            uint64_t row_count;
            uint64_t end_offset;
        };

        struct FSIndex
        {
            uint64_t xid;
            uint64_t lsn;
            Index index;
        };


        struct FSTable
        {
            uint64_t ns_id;
            std::string name;
            uint64_t table_id;
            uint64_t xid;
            uint64_t lsn;
            bool exists;
            std::map<uint64_t, SchemaColumn> pos_to_column;
            std::map<uint64_t, FSIndex> id_to_index;
            std::map<std::pair<uint64_t, uint64_t>, FSRoot> index_xid_to_root;
            std::map<uint64_t, FSStats> xid_to_stats;
        };

        std::map<std::pair<uint64_t, uint64_t>, FSNamespace> _db_ns_id_map;
        std::map<std::pair<uint64_t, uint64_t>, FSTable> _db_tbl_id_map;

        /**
         * @brief Read all namespaces for the database
         *
         * @param db_id - database id
         */
        void
        _read_namespaces(uint64_t db_id);

        /**
         * @brief Read all table data for the given database
         *
         * @param db_id
         */
        void
        _read_tables(uint64_t db_id);

        /**
         * @brief Validate primary key index
         *
         * @param table - table
         * @param table_schema - table schema
         */
        void
        _validate_primary_extent(std::shared_ptr<Table> table, ExtentSchemaPtr table_schema);

        /**
         * @brief Validate secondary indexes
         *
         * @param table - table
         * @param table_schema - schema
         */
        void
        _validate_secondary_extents(std::shared_ptr<Table> table, ExtentSchemaPtr table_schema);

        /**
         * @brief Internal function for checking specific database.
         *
         * @param db_id - database id
         * @param db_name - database name
         */
        void
        _check_db(uint64_t db_id, const std::string &db_name);

        /**
         * @brief Internal function to reading specific database table
         *
         * @param db_id - database id
         * @param db_name - database name
         * @param table_id - table id
         * @param table_fields - fields of TableNames system table
         * @param row - row in TableNames system table
         */
        void
        _check_db_table(uint64_t db_id, const std::string &db_name, const FSTable &table);

        /**
         * @brief Get system table with the fields
         *
         * @tparam Tbl - id of system table
         * @param db_id - database id
         * @return std::pair<TablePtr, std::shared_ptr<std::vector<FieldPtr>>> - pair of table pointer and fields vector
         */
        template<typename Tbl>
        std::pair<TablePtr, std::shared_ptr<std::vector<FieldPtr>>>
        _get_table_and_fields(uint64_t db_id);
    };

} // springtail::test