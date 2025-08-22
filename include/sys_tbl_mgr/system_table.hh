#pragma once

#include <sys_tbl_mgr/system_table_mgr.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {
    class SystemTable : public Table, public std::enable_shared_from_this<SystemTable> {
    public:
        /**
         * UserTable constructor.
         */
        SystemTable(uint64_t db_id,
                  uint64_t table_id,
                  uint64_t xid,
                  const std::filesystem::path &table_base,
                  const std::vector<std::string> &primary_key,
                  const std::vector<Index> &secondary,
                  const TableMetadata &metadata,
                  ExtentSchemaPtr schema) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema) {}

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual ExtentSchemaPtr extent_schema() const
        {
            return SystemTableMgr::get_instance()->get_extent_schema(_id);
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const
        {
            return SystemTableMgr::get_instance()->get_schema(_id);
        }
    };
} // springtail
