#pragma once

#include <filesystem>
#include <map>

#include <sys_tbl_mgr/system_table.hh>


namespace springtail::test {
    class FSCheck
    {
    public:
        explicit FSCheck(uint64_t max_xid = constant::LATEST_XID, bool all_xids = false);
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
         * @param all_xids - check all available xids
         */
        void
        check_db_table(uint64_t db_id, uint64_t table_id, bool all_xids = false);

    private:
        std::map<uint64_t, std::string> _databases;     ///< map of database id to database name
        std::map<uint64_t, uint64_t> _db_id_to_cutoff_xid;  ///< cuttoff xid per database
        std::filesystem::path _table_base;              ///< directory where all the tables are stored
        uint64_t _max_xid;                              ///< maximum xid
        uint64_t _max_recorded_xid{0};                  ///< maximum xid found in system tables
        bool _all_xids;                                 ///< iterate over all xids

        /**
         * @brief Storage for namespace data
         *
         */
        struct FSNamespace
        {
            uint64_t ns_id;
            std::string ns_name;
            uint64_t xid;
            uint64_t lsn;
            bool exists;
        };

        /**
         * @brief Storage for root data
         *
         */
        struct FSRoot
        {
            uint64_t xid;
            uint64_t extent_id;
            uint64_t snapshot_xid;
        };

        /**
         * @brief Storage for stats data
         *
         */
        struct FSStats
        {
            uint64_t xid;
            uint64_t row_count;
            uint64_t end_offset;
        };

        /**
         * @brief Storage for index data
         *
         */
        struct FSIndex
        {
            uint64_t xid;
            uint64_t lsn;
            Index index;
        };

        /**
         * @brief Storage for table data
         *
         */
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

        /**
         * @brief Map of database id and namespace id namespace data
         *
         */
        std::map<std::pair<uint64_t, uint64_t>, FSNamespace> _db_ns_id_map;

        /**
         * @brief Map of database id and table id to table data
         *
         */
        std::map<std::pair<uint64_t, uint64_t>, FSTable> _db_tbl_id_map;

        /**
         * @brief Read all namespaces for the database
         *
         * @param db_id - database id
         * @param max_xid - maximum xid that limits data scan
         */
        void
        _read_namespaces(uint64_t db_id, uint64_t max_xid);

        /**
         * @brief Read all table data for the given database
         *
         * @param db_id - database id
         * @param max_xid - maximum xid that limits data scan
         */
        void
        _read_tables(uint64_t db_id, uint64_t max_xid);

        /**
         * @brief Read all information for the given database from the system tables.
         *
         * @param db_id - database id
         * @param max_xid - maximum xid that limits data scan
         */
        void
        _read_database_info(uint64_t db_id, uint64_t max_xid);

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
         * @param first_xid - first xid
         * @param cuttoff_xid - cutoff xid
         */
        void
        _check_db(uint64_t db_id, uint64_t first_xid, uint64_t cutoff_xid);

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

        /**
         * @brief Save the next max recorded xid if applicable
         *
         * @param xid - xid value
         */
        void
        _record_max_xid(uint64_t xid);
    };

} // springtail::test