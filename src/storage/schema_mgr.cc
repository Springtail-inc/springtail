#include <common/constants.hh>
#include <storage/exception.hh>
#include <storage/schema.hh>
#include <storage/schema_mgr.hh>
#include <storage/system_tables.hh>
#include <storage/table_mgr.hh>
#include <sys_tbl_mgr/client.hh>

namespace springtail {
    /* static member initialization must happen outside of class */
    SchemaMgr* SchemaMgr::_instance {nullptr};
    boost::mutex SchemaMgr::_instance_mutex;

    SchemaMgr *
    SchemaMgr::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new SchemaMgr();
        }

        return _instance;
    }

    void
    SchemaMgr::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    SchemaMgr::SchemaMgr()
    {
        // note: don't need a valid sql_type for the internal nodes since they aren't exposed
        SchemaColumn child(constant::BTREE_CHILD_FIELD, 0, SchemaType::UINT64, 0, false);

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
    }

    std::map<uint32_t, SchemaColumn>
    SchemaMgr::_convert_columns(const std::vector<SchemaColumn> &columns)
    {
        std::map<uint32_t, SchemaColumn> column_map;
        for (auto &&column : columns) {
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
                default:
                    assert(false);
                    break;
            }
        }

        // non-system tables
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);
        return _convert_columns(meta.columns);
    }

    std::shared_ptr<Schema>
    SchemaMgr::get_schema(uint64_t db_id,
                          uint64_t table_id,
                          const XidLsn &access_xid,
                          const XidLsn &target_xid)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
        if (system_i != _system_cache.end()) {
            return system_i->second;
        }

        // XXX keep some kind of local cache?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_target_schema(db_id, table_id, access_xid, target_xid);

        // construct the schema object
        if (meta.history.empty()) {
            return std::make_shared<ExtentSchema>(meta.columns);
        }

        return std::make_shared<VirtualSchema>(meta);
    }

    std::shared_ptr<ExtentSchema>
    SchemaMgr::get_extent_schema(uint64_t db_id,
                                 uint64_t table_id,
                                 const XidLsn &xid)
    {
        // first check if it's an immutable system table schema
        auto &&system_i = _system_cache.find({ table_id, constant::INDEX_DATA, true });
        if (system_i != _system_cache.end()) {
            return system_i->second;
        }

        // XXX keep some kind of local cache?  how to keep it valid given the XID progression?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);

        // construct the schema from the provided schema metadata
        return std::make_shared<ExtentSchema>(meta.columns);
    }

}
