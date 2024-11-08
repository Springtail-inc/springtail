#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>

namespace springtail {

    /* static member initialization must happen outside of class */
    TableMgr* TableMgr::_instance {nullptr};
    boost::mutex TableMgr::_instance_mutex;

    TableMgr *
    TableMgr::get_instance()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new TableMgr();
        }

        return _instance;
    }

    void
    TableMgr::shutdown()
    {
        boost::unique_lock lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    TableMgr::TableMgr()
    {
        // get the base directory for table data
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", _table_base);
        _table_base = Properties::make_absolute_path(_table_base);
    }

    TablePtr
    TableMgr::get_table(uint64_t db_id,
                        uint64_t table_id,
                        uint64_t xid)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return _get_system_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);

        // construct the table and return it
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id,
                                                                   {xid, constant::MAX_LSN});
        return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                       schema->get_sort_keys(), std::vector<std::vector<std::string>>{},
                                       tbl_meta, schema);
    }

    bool
    TableMgr::exists(uint64_t db_id,
                     uint64_t table_id,
                     uint64_t xid,
                     uint64_t lsn)
    {
        boost::shared_lock lock(_mutex);
        return sys_tbl_mgr::Client::get_instance()->exists(db_id, table_id, { xid, lsn });
    }

    MutableTablePtr
    TableMgr::get_mutable_table(uint64_t db_id,
                                uint64_t table_id,
                                uint64_t access_xid,
                                uint64_t target_xid,
                                bool for_gc)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return _get_mutable_system_table(db_id, table_id, access_xid, target_xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, access_xid);

        // construct the mutable table and return it
        XidLsn xid(target_xid);
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id, xid);

        return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid,
                                              _table_base, schema->get_sort_keys(),
                                              std::vector<std::vector<std::string>>{},
                                              tbl_meta, schema, for_gc);
    }

    MutableTablePtr
    TableMgr::get_snapshot_table(uint64_t db_id,
                                 uint64_t table_id,
                                 uint64_t snapshot_xid,
                                 ExtentSchemaPtr schema)
    {
        TableMetadata tbl_meta;
        tbl_meta.snapshot_xid = snapshot_xid;

        // construct an empty mutable table with the provided snapshot XID and return it
        return std::make_shared<MutableTable>(db_id, table_id, snapshot_xid, snapshot_xid,
                                              _table_base, schema->get_sort_keys(),
                                              std::vector<std::vector<std::string>>{},
                                              tbl_meta, schema, false);
    }

    void
    TableMgr::create_table(uint64_t db_id,
                           const XidLsn &xid,
                           const PgMsgTable &msg)
    {
        sys_tbl_mgr::Client::get_instance()->create_table(db_id, xid, msg);
    }

    void
    TableMgr::alter_table(uint64_t db_id,
                          const XidLsn &xid,
                          const PgMsgTable &msg)
    {
        sys_tbl_mgr::Client::get_instance()->alter_table(db_id, xid, msg);
    }

    void
    TableMgr::drop_table(uint64_t db_id,
                         const XidLsn &xid,
                         const PgMsgDropTable &msg)
    {
        sys_tbl_mgr::Client::get_instance()->drop_table(db_id, xid, msg);
    }

    void
    TableMgr::finalize_metadata(uint64_t db_id,
                                uint64_t xid)
    {
        sys_tbl_mgr::Client::get_instance()->finalize(db_id, xid);
    }

    TablePtr
    TableMgr::_get_system_table(uint64_t db_id,
                                uint64_t table_id,
                                uint64_t xid)
    {
        // initialize the system tables using the look-aside root files
        // XXX should we change this to use the table_roots and table_stats?
        std::vector<std::vector<std::string>> secondary_keys;

        // construct generic table metadata for the system tables
        TableMetadata tbl_meta;
        tbl_meta.snapshot_xid = 1;

        // get the table's schema
        XidLsn access_xid(xid);
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id, access_xid);

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);

            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::TableNames::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::TableRoots::ID): {
            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::TableRoots::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::Indexes::ID): {
            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::Indexes::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::Schemas::ID): {
            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::Schemas::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::TableStats::ID): {
            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::TableStats::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::IndexNames::ID): {
            return std::make_shared<Table>(db_id, table_id, xid, _table_base,
                                           sys_tbl::IndexNames::Primary::KEY,
                                           secondary_keys, tbl_meta, schema);
        }
        default:
            assert(0);
        }
    }

    void
    TableMgr::update_roots(uint64_t db_id,
                           uint64_t table_id,
                           uint64_t target_xid,
                           const TableMetadata &metadata)
    {
        sys_tbl_mgr::Client::get_instance()->update_roots(db_id, table_id, target_xid, metadata);
    }

    MutableTablePtr
    TableMgr::_get_mutable_system_table(uint64_t db_id,
                                        uint64_t table_id,
                                        uint64_t access_xid,
                                        uint64_t target_xid)
    {
        // initialize the system tables using the look-aside root files
        std::vector<std::vector<std::string>> secondary_keys;

        TableMetadata tbl_meta;
        tbl_meta.snapshot_xid = 1;

        XidLsn xid(access_xid);
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id, xid);

        // XXX note that the table stats are currently broken for system tables... would need a way to bootstrap

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);

            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::TableNames::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::TableRoots::ID): {
            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::TableRoots::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::Indexes::ID): {
            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::Indexes::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::Schemas::ID): {
            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::Schemas::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::TableStats::ID): {
            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::TableStats::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        case (sys_tbl::IndexNames::ID): {
            return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, _table_base,
                                                  sys_tbl::IndexNames::Primary::KEY,
                                                  secondary_keys, tbl_meta, schema);
        }
        default:
            SPDLOG_ERROR("Unable to find the requested system table: {}", table_id);
            throw SchemaError();
        }
    }

}
