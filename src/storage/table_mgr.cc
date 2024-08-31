#include <common/json.hh>
#include <common/properties.hh>

#include <storage/table_mgr.hh>
#include <storage/system_tables.hh>

#include <sys_tbl_mgr/client.hh>

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

        // make sure that the base directory for tables exists
        std::filesystem::create_directories(_table_base);
    }

    TablePtr
    TableMgr::get_table(uint64_t db_id,
                        uint64_t table_id,
                        uint64_t xid,
                        uint64_t lsn)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return _get_system_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);

        // construct the table and return it
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id, {xid, lsn});
        return std::make_shared<Table>(db_id, table_id, xid, _table_base / std::to_string(db_id) / std::to_string(table_id),
                                       schema->get_sort_keys(), std::vector<std::vector<std::string>>{},
                                       tbl_meta.roots, schema, tbl_meta.stats);
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

        return std::make_shared<MutableTable>(db_id, table_id, access_xid, target_xid, tbl_meta.roots,
                                              _table_base / std::to_string(db_id) / std::to_string(table_id),
                                              schema->get_sort_keys(),
                                              std::vector<std::vector<std::string>>{},
                                              schema, tbl_meta.stats, for_gc);
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
        // XXX should change this to use the table_roots and table_stats
        std::vector<uint64_t> roots;
        std::vector<std::vector<std::string>> secondary_keys;
        ExtentSchemaPtr schema;
        std::filesystem::path table_path;
        XidLsn access_xid(xid);

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableNames::ID, access_xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableNames::ID);

            return std::make_shared<Table>(db_id,
                                           sys_tbl::TableNames::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::TableNames::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           TableStats());
        }
        case (sys_tbl::TableRoots::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableRoots::ID, access_xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableRoots::ID);

            return std::make_shared<Table>(db_id,
                                           sys_tbl::TableRoots::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::TableRoots::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           TableStats());
        }
        case (sys_tbl::Indexes::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::Indexes::ID, access_xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::Indexes::ID);

            return std::make_shared<Table>(db_id,
                                           sys_tbl::Indexes::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::Indexes::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           TableStats());
        }
        case (sys_tbl::Schemas::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::Schemas::ID, access_xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::Schemas::ID);

            return std::make_shared<Table>(db_id,
                                           sys_tbl::Schemas::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::Schemas::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           TableStats());
        }
        case (sys_tbl::TableStats::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableStats::ID, access_xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableStats::ID);

            return std::make_shared<Table>(db_id,
                                           sys_tbl::TableStats::ID,
                                           xid,
                                           table_path,
                                           sys_tbl::TableStats::Primary::KEY,
                                           secondary_keys,
                                           roots,
                                           schema,
                                           TableStats());
        }
        default:
            return nullptr;
        }
    }

    void
    TableMgr::update_roots(uint64_t db_id,
                           uint64_t table_id,
                           uint64_t target_xid,
                           const std::vector<uint64_t> &roots,
                           const TableStats &stats)
    {
        sys_tbl_mgr::Client::get_instance()->update_roots(db_id, table_id, target_xid, roots, stats.row_count);
    }

    MutableTablePtr
    TableMgr::_get_mutable_system_table(uint64_t db_id,
                                        uint64_t table_id,
                                        uint64_t access_xid,
                                        uint64_t target_xid)
    {
        // initialize the system tables using the look-aside root files
        std::vector<uint64_t> roots;
        std::vector<std::vector<std::string>> secondary_keys;
        ExtentSchemaPtr schema;
        std::filesystem::path table_path;
        XidLsn xid(access_xid);

        // XXX note that the table stats are currently broken for system tables... need a way to bootstrap

        MutableTablePtr sys_table;

        switch (table_id) {
        case (sys_tbl::TableNames::ID): {
            secondary_keys.push_back(sys_tbl::TableNames::Secondary::KEY);
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableNames::ID, xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableNames::ID);

            sys_table = std::make_shared<MutableTable>(db_id,
                                                       sys_tbl::TableNames::ID,
                                                       access_xid,
                                                       target_xid,
                                                       roots,
                                                       table_path,
                                                       sys_tbl::TableNames::Primary::KEY,
                                                       secondary_keys,
                                                       schema,
                                                       TableStats());
            break;
        }
        case (sys_tbl::TableRoots::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableRoots::ID, xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableRoots::ID);

            sys_table = std::make_shared<MutableTable>(db_id,
                                                       sys_tbl::TableRoots::ID,
                                                       access_xid,
                                                       target_xid,
                                                       roots,
                                                       table_path,
                                                       sys_tbl::TableRoots::Primary::KEY,
                                                       secondary_keys,
                                                       schema,
                                                       TableStats());
            break;
        }
        case (sys_tbl::Indexes::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::Indexes::ID, xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::Indexes::ID);

            sys_table = std::make_shared<MutableTable>(db_id,
                                                       sys_tbl::Indexes::ID,
                                                       access_xid,
                                                       target_xid,
                                                       roots,
                                                       table_path,
                                                       sys_tbl::Indexes::Primary::KEY,
                                                       secondary_keys,
                                                       schema,
                                                       TableStats());
            break;
        }
        case (sys_tbl::Schemas::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::Schemas::ID, xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::Schemas::ID);

            sys_table = std::make_shared<MutableTable>(db_id,
                                                       sys_tbl::Schemas::ID,
                                                       access_xid,
                                                       target_xid,
                                                       roots,
                                                       table_path,
                                                       sys_tbl::Schemas::Primary::KEY,
                                                       secondary_keys,
                                                       schema,
                                                       TableStats());
            break;
        }
        case (sys_tbl::TableStats::ID): {
            schema = SchemaMgr::get_instance()->get_extent_schema(db_id, sys_tbl::TableStats::ID, xid);
            table_path = _table_base / std::to_string(db_id) / std::to_string(sys_tbl::TableStats::ID);

            sys_table = std::make_shared<MutableTable>(db_id,
                                                       sys_tbl::TableStats::ID,
                                                       access_xid,
                                                       target_xid,
                                                       roots,
                                                       table_path,
                                                       sys_tbl::TableStats::Primary::KEY,
                                                       secondary_keys,
                                                       schema,
                                                       TableStats());
            break;
        }
        default:
            SPDLOG_ERROR("Unable to find the requested system table: {}", table_id);
            throw SchemaError();
        }

        return sys_table;
    }
}
