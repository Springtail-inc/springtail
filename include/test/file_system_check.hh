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
        std::map<uint64_t, std::string> _databases;
        std::filesystem::path _table_base;

        void
        _check_db(uint64_t db_id, const std::string &db_name);

        void
        _check_db_table(uint64_t db_id, const std::string &db_name, uint64_t table_id, std::shared_ptr<std::vector<FieldPtr>> table_fields, const Extent::Row &row);

        template<typename Tbl>
        std::pair<TablePtr, std::shared_ptr<std::vector<FieldPtr>>>
        _get_table_and_fields(uint64_t db_id);

        std::vector<SchemaColumn>
        _read_schema_columns(uint64_t db_id, uint64_t table_id, XidLsn &access_start, XidLsn &access_end);

        std::shared_ptr<const SchemaMetadata>
        _get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid_lsn);

        std::shared_ptr<ExtentSchema>
        _get_extent_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        TableMetadataPtr
        _get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

        TablePtr
        _get_table(uint64_t db_id, uint64_t table_id, uint64_t xid);
    };

} // springtail::test