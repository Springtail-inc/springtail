#include <sys_tbl_mgr/system_table_mgr_server.hh>
#include <sys_tbl_mgr/system_mutable_table.hh>
#include <sys_tbl_mgr/system_tables.hh>

using namespace springtail;

SystemTableMgrServer::SystemTableMgrServer()
    : Singleton<SystemTableMgrServer>(ServiceId::SystemTableMgrServerId),
      SystemTableMgr()
{
}

MutableTablePtr
SystemTableMgrServer::get_mutable_system_table(uint64_t db_id, uint64_t table_id,
                                               uint64_t access_xid, uint64_t target_xid)
{
    DCHECK(table_id < constant::MAX_SYSTEM_TABLE_ID);
    // initialize the system tables using the look-aside root files
    std::vector<Index> secondary_keys;

    TableMetadata tbl_meta{};
    tbl_meta.snapshot_xid = 1;

    auto schema = _get_extent_schema(table_id);

    // XXX note that the table stats are currently broken for system tables... would need a way to bootstrap


    switch (table_id) {
    case sys_tbl::TableNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::TableNames>();

        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableNames::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::TableRoots::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableRoots::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::Indexes::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::Indexes::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::Schemas::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::Schemas::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::TableStats::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableStats::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::IndexNames::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::IndexNames::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::NamespaceNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::NamespaceNames>();
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::NamespaceNames::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    case sys_tbl::UserTypes::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::UserTypes::Primary::KEY, secondary_keys,
                                                    tbl_meta, schema);
    }
    default:
        LOG_ERROR("Unable to find the requested system table: {}", table_id);
        throw SchemaError();
    }
}
