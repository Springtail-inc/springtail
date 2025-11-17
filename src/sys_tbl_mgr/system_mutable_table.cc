#include <sys_tbl_mgr/system_mutable_table.hh>

namespace springtail {

SystemMutableTable::SystemMutableTable(uint64_t db_id,
                                       uint64_t table_id,
                                       uint64_t access_xid,
                                       uint64_t target_xid,
                                       const std::filesystem::path &table_base,
                                       const std::vector<std::string> &primary_key,
                                       const std::vector<Index> &secondary,
                                       const TableMetadata &metadata,
                                       ExtentSchemaPtr schema)
    : MutableTable(db_id, table_id, access_xid, target_xid, table_base, primary_key,
                   secondary, metadata, schema)
{
}

void SystemMutableTable::truncate()
{
    // no truncation for system tables
    LOG_WARN("Truncate operation on system table is not supported.");
    DCHECK(false);
}

} // namespace springtail
