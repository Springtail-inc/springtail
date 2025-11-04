#pragma once

#include <boost/thread.hpp>
#include <filesystem>

#include <sys_tbl_mgr/table.hh>

namespace springtail {
    class TableMgrBase {
    public:
        /**
         * @brief Construct a new Table Mgr Base object
         *
         */
        TableMgrBase();
        virtual ~TableMgrBase() = default;

        /**
         * @brief Read the table metadata for the requested table ID.
         *
         * @param db_id - database id
         * @param table_id - table id
         * @param xid - transaction id
         * @return TablePtr - pointer to the table object
         */
        virtual TablePtr
        get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, const ExtensionCallback &extension_callback = {}) = 0;

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        virtual std::map<uint32_t, SchemaColumn>
        get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid) = 0;

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param db_id The database ID of the table.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        virtual std::shared_ptr<Schema>
        get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid) = 0;

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent.  This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param db_id The database ID of the table.
         * @param table_id The table we need the schema for.
         * @param xid The XID/LSN that we need the schema at. Defaults to the MAX_LSN, providing the
         *            schema at the point after all changes in the XID have been applied.
         */
        virtual std::shared_ptr<ExtentSchema>
        get_extent_schema(uint64_t db_id, uint64_t table_id,
                          const XidLsn &xid, const ExtensionCallback &extension_callback = {},
                          bool allow_undefined = false) = 0;

        /**
         * @brief Get the base path for the tables storage location
         *
         * @return std::filesystem::path file path
         */
        std::filesystem::path get_table_base() const { return _table_base; }

    protected:
        /** Helper to convert schema column to map */
        static std::map<uint32_t, SchemaColumn>
        _convert_columns(const std::vector<SchemaColumn> &columns);

        std::filesystem::path _table_base; ///< The base directory for individual table directories.
    };
} // springtail
