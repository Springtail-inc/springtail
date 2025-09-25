#include <common/json.hh>

#include <sys_tbl_mgr/system_table_mgr.hh>
#include <sys_tbl_mgr/system_table.hh>
#include <sys_tbl_mgr/system_tables.hh>

using namespace springtail;

SystemTableMgr::SystemTableMgr() : Singleton<SystemTableMgr>(ServiceId::SystemTableMgrId), TableMgrBase()
{
    // TableNames
    _system_cache[{ sys_tbl::TableNames::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::TableNames::Data::SCHEMA);

    // TableRoots
    _system_cache[{ sys_tbl::TableRoots::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::TableRoots::Data::SCHEMA);

    // Indexes
    _system_cache[{ sys_tbl::Indexes::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::Indexes::Data::SCHEMA);

    // Schemas
    _system_cache[{ sys_tbl::Schemas::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::Schemas::Data::SCHEMA);

    // TableStats
    _system_cache[{ sys_tbl::TableStats::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::TableStats::Data::SCHEMA);

    // IndexNames
    _system_cache[{ sys_tbl::IndexNames::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::IndexNames::Data::SCHEMA);

    // NamespaceNames
    _system_cache[{ sys_tbl::NamespaceNames::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::NamespaceNames::Data::SCHEMA);

    // UserTypes
    _system_cache[{ sys_tbl::UserTypes::ID, constant::INDEX_DATA, true }] = std::make_shared<ExtentSchema>(sys_tbl::UserTypes::Data::SCHEMA);
}

std::shared_ptr<Schema>
SystemTableMgr::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid)
{
    return _get_schema(table_id);
}

std::shared_ptr<ExtentSchema>
SystemTableMgr::get_extent_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid, bool allow_undefined)
{
    return _get_extent_schema(table_id);
}

std::shared_ptr<Schema>
SystemTableMgr::_get_schema(uint64_t table_id)
{
    // first check if it's an immutable system table schema
    auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
    CHECK(system_i != _system_cache.end());
    return system_i->second;
}

std::shared_ptr<ExtentSchema>
SystemTableMgr::_get_extent_schema(uint64_t table_id)
{
    // first check if it's an immutable system table schema
    auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
    CHECK(system_i != _system_cache.end());
    return system_i->second;
}

template<typename Table>
std::vector<Index>
SystemTableMgr::_get_secondary_keys() {
    std::vector<Index> keys;
    Index idx;
    idx.id = 1;
    idx.table_id = Table::ID;
    idx.is_unique = false;
    idx.state = static_cast<uint8_t>(sys_tbl::IndexNames::State::READY);

    uint32_t idx_position = 0;
    for (auto const& col: Table::Secondary::KEY) {
        // find the
        auto it = std::ranges::find_if(Table::Data::SCHEMA, [&](auto const& v)
                {
                return col == v.name;
                }
                );
        assert(it != Table::Data::SCHEMA.end());
        idx.columns.emplace_back(idx_position, it->position);
        ++idx_position;
    }
    keys.push_back(idx);
    return keys;
}

TablePtr
SystemTableMgr::get_table(uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    DCHECK(table_id < constant::MAX_SYSTEM_TABLE_ID);
    // initialize the system tables using the look-aside root files
    // XXX should we change this to use the table_roots and table_stats?
    std::vector<Index> secondary_keys;

    // construct generic table metadata for the system tables
    TableMetadata tbl_meta;
    tbl_meta.snapshot_xid = 1;

    std::shared_ptr<TableMetadata> tbl_meta_ptr(&tbl_meta, [](TableMetadata*) {
        // no-op deleter: do nothing
    });

    // get the table's schema
    auto schema = _get_extent_schema(table_id);

    switch (table_id) {
    case sys_tbl::TableNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::TableNames>();
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::TableNames::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::TableRoots::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::TableRoots::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::Indexes::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::Indexes::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::Schemas::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::Schemas::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::TableStats::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::TableStats::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::IndexNames::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::IndexNames::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::NamespaceNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::NamespaceNames>();
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::NamespaceNames::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    case sys_tbl::UserTypes::ID: {
        return std::make_shared<SystemTable>(db_id, table_id, xid, _table_base,
                                             sys_tbl::UserTypes::Primary::KEY,
                                             secondary_keys, tbl_meta_ptr, schema);
    }
    default:
        CHECK(0);
    }
}

std::map<uint32_t, SchemaColumn>
SystemTableMgr::get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    switch (table_id) {
    case sys_tbl::TableNames::ID:
        return _convert_columns(sys_tbl::TableNames::Data::SCHEMA);
    case sys_tbl::TableRoots::ID:
        return _convert_columns(sys_tbl::TableRoots::Data::SCHEMA);
    case sys_tbl::Indexes::ID:
        return _convert_columns(sys_tbl::Indexes::Data::SCHEMA);
    case sys_tbl::Schemas::ID:
        return _convert_columns(sys_tbl::Schemas::Data::SCHEMA);
    case sys_tbl::TableStats::ID:
        return _convert_columns(sys_tbl::TableStats::Data::SCHEMA);
    case sys_tbl::IndexNames::ID:
        return _convert_columns(sys_tbl::IndexNames::Data::SCHEMA);
    case sys_tbl::NamespaceNames::ID:
        return _convert_columns(sys_tbl::NamespaceNames::Data::SCHEMA);
    case sys_tbl::UserTypes::ID:
        return _convert_columns(sys_tbl::UserTypes::Data::SCHEMA);
    default:
        CHECK(false);
        break;
    }
}

MutableTablePtr
SystemTableMgr::get_mutable_system_table(uint64_t db_id, uint64_t table_id,
                                         uint64_t access_xid, uint64_t target_xid)
{
    DCHECK(table_id < constant::MAX_SYSTEM_TABLE_ID);
    // initialize the system tables using the look-aside root files
    std::vector<Index> secondary_keys;

    TableMetadata tbl_meta;
    tbl_meta.snapshot_xid = 1;

    std::shared_ptr<TableMetadata> tbl_meta_ptr(&tbl_meta, [](TableMetadata*) {
        // no-op deleter: do nothing
    });

    auto schema = _get_extent_schema(table_id);

    // XXX note that the table stats are currently broken for system tables... would need a way to bootstrap


    switch (table_id) {
    case sys_tbl::TableNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::TableNames>();

        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableNames::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::TableRoots::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableRoots::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::Indexes::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::Indexes::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::Schemas::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::Schemas::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::TableStats::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::TableStats::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::IndexNames::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::IndexNames::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::NamespaceNames::ID: {
        secondary_keys = _get_secondary_keys<sys_tbl::NamespaceNames>();
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::NamespaceNames::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    case sys_tbl::UserTypes::ID: {
        return std::make_shared<SystemMutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                    sys_tbl::UserTypes::Primary::KEY, secondary_keys,
                                                    tbl_meta_ptr, schema);
    }
    default:
        LOG_ERROR("Unable to find the requested system table: {}", table_id);
        throw SchemaError();
    }
}

