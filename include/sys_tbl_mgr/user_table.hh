#pragma once

#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {
    class UserTable : public Table, public std::enable_shared_from_this<UserTable> {
    public:
        /**
         * UserTable constructor.
         */
        UserTable(uint64_t db_id,
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
        virtual ExtentSchemaPtr extent_schema() const override
        {
            return SchemaMgr::get_instance()->get_extent_schema(_db_id, _id, XidLsn(_xid));
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const override
        {
            return SchemaMgr::get_instance()->get_schema(_db_id, _id, XidLsn(extent_xid), XidLsn(_xid));
        }

    };
} // springtail