#include <common/constants.hh>

#include <storage/exception.hh>
#include <storage/schema.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>
#include <sys_tbl_mgr/table_mgr.hh>

namespace springtail {

    SchemaMgr::SchemaMgr() : Singleton<SchemaMgr>(ServiceId::SchemaMgrId)
    {}

    std::map<uint32_t, SchemaColumn>
    SchemaMgr::_convert_columns(const std::vector<SchemaColumn> &columns)
    {
        std::map<uint32_t, SchemaColumn> column_map;
        for (auto &&column : columns) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "col_pos={} col_name={}",
                        column.position, column.name);
            column_map.insert({column.position, column});
        }
        return column_map;
    }

    std::map<uint32_t, SchemaColumn>
    SchemaMgr::get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
    {
        // handle system tables
        if (table_id <= constant::MAX_SYSTEM_TABLE_ID) {
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
                    assert(false);
                    break;
            }
        }

        // non-system tables
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);
        return _convert_columns(meta->columns);
    }

    std::shared_ptr<Schema>
    SchemaMgr::get_schema(uint64_t db_id,
                          uint64_t table_id,
                          const XidLsn &access_xid,
                          const XidLsn &target_xid)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->_get_schema(table_id);
        }

        // XXX keep some kind of local cache?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_target_schema(db_id, table_id, access_xid, target_xid);

        // construct the schema object
        if (meta->history.empty()) {
            return std::make_shared<ExtentSchema>(meta->columns);
        }

        return std::make_shared<VirtualSchema>(*meta);
    }

    std::shared_ptr<ExtentSchema>
    SchemaMgr::get_extent_schema(uint64_t db_id,
                                 uint64_t table_id,
                                 const XidLsn &xid,
                                 bool allow_undefined)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->_get_extent_schema(table_id);
        }

        // XXX keep some kind of local cache?  how to keep it valid given the XID progression?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);

        // construct the schema from the provided schema metadata
        return std::make_shared<ExtentSchema>(meta->columns, allow_undefined);
    }

    std::shared_ptr<UserType>
    SchemaMgr::get_usertype(uint64_t db_id,
                            uint64_t type_id,
                            const XidLsn &xid)
    {
        // call into the SysTblMgr to get the user_type at the given XID/LSN
        return sys_tbl_mgr::Client::get_instance()->get_usertype(db_id, type_id, xid);
    }
}
