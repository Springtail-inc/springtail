#pragma once

#include <sys_tbl_mgr/system_table_mgr.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail {
    class SystemTable : public Table,
                        public std::enable_shared_from_this<SystemTable> {
    public:
        using Table::schema;

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
        virtual ExtentSchemaPtr extent_schema() const override
        {
            return SystemTableMgr::get_instance()->_get_extent_schema(_id);
        }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const override
        {
            return SystemTableMgr::get_instance()->_get_schema(_id);
        }
    };

    class SystemMutableTable: public MutableTable,
                              public std::enable_shared_from_this<SystemMutableTable> {

    public:
        /**
         * System mutable table constructor.
         */
        SystemMutableTable(uint64_t db_id,
                           uint64_t table_id,
                           uint64_t access_xid,
                           uint64_t target_xid,
                           const std::filesystem::path &table_base,
                           const std::vector<std::string> &primary_key,
                           const std::vector<Index> &secondary,
                           const TableMetadata &metadata,
                           ExtentSchemaPtr schema,
                           bool for_gc = false) :
            MutableTable(db_id, table_id, access_xid, target_xid, table_base, primary_key,
                         secondary, metadata, schema, for_gc, false) {}

        /**
         * Truncate function call is empty as we are not going to truncate system tables.
         *
         */
        virtual void truncate() override
        {
            // no truncation for system tables
            LOG_WARN("Truncate operation on system table is not supported.");
            DCHECK(false);
        }
    };
} // springtail
