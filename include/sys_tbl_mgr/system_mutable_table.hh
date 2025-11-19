#pragma once

#include <sys_tbl_mgr/system_table.hh>
#include <sys_tbl_mgr/mutable_table.hh>

namespace springtail {

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
                           ExtentSchemaPtr schema);

        /**
         * Truncate function call is empty as we are not going to truncate system tables.
         */
        virtual void truncate() override;
    };

} // namespace springtail
