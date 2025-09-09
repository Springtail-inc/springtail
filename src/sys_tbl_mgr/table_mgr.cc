#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>
#include <sys_tbl_mgr/user_table.hh>

#include <storage/vacuumer.hh>

namespace springtail {

    TableMgr::TableMgr() : Singleton<TableMgr>(ServiceId::TableMgrId)
    {
        // get the base directory for table data
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", _table_base);
        _table_base = Properties::make_absolute_path(_table_base);
    }

    TablePtr
    TableMgr::get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, ComparatorFunc comparator_func)
    {
        boost::shared_lock lock(_mutex);

        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_system_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);

        // construct the table and return it
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id,
                                                                {xid, constant::MAX_LSN},
                                                                comparator_func);

        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        return std::make_shared<UserTable>(db_id, table_id, xid, _table_base,
                                        schema->get_sort_keys(), secondary_indexes,
                                        *tbl_meta, schema, comparator_func);
    }

    std::filesystem::path
    TableMgr::get_table_data_dir(uint64_t db_id, uint64_t table_id, uint64_t xid)
    {
        auto&& table_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);
        return table_helpers::get_table_dir(_table_base, db_id, table_id, table_meta->snapshot_xid);
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
            return SystemTableMgr::get_instance()->get_mutable_system_table(db_id, table_id, access_xid, target_xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, access_xid);

        // construct the mutable table and return it
        XidLsn xid(target_xid);
        auto schema = SchemaMgr::get_instance()->get_extent_schema(db_id, table_id, xid);

        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        LOG_DEBUG(LOG_BTREE, "Get mutable table: table {}, access_xid {}", table_id, access_xid);

#ifdef DEBUG
        for (auto &root : tbl_meta->roots) {
            LOG_DEBUG(LOG_BTREE, "Get mutable table: index {}, root {}", root.index_id, root.extent_id);
        }
#endif

        return std::make_shared<UserMutableTable>(db_id, table_id, access_xid, target_xid,
                                                  _table_base, schema->get_sort_keys(), secondary_indexes,
                                                  *tbl_meta, schema, for_gc);
    }

    MutableTablePtr
    TableMgr::get_snapshot_table(uint64_t db_id,
                                 uint64_t table_id,
                                 uint64_t snapshot_xid,
                                 ExtentSchemaPtr schema,
                                 const std::vector<Index>& secondary_keys,
                                 ComparatorFunc comparator_func)
    {
        TableMetadata tbl_meta;
        tbl_meta.snapshot_xid = snapshot_xid;

        // note: in the case of a failure, there may be a partially copied table already present in
        //       the directory structure, so we need to make sure to delete it before we try to
        //       create it below
        auto table_dir = table_helpers::get_table_dir(_table_base, db_id, table_id, snapshot_xid);
        if (std::filesystem::exists(table_dir)) {
            std::filesystem::remove_all(table_dir);
        }

        // construct an empty mutable table with the provided snapshot XID and return it
        return std::make_shared<UserMutableTable>(db_id, table_id, snapshot_xid, snapshot_xid,
                                                  _table_base, schema->get_sort_keys(), secondary_keys,
                                                  tbl_meta, schema, false, comparator_func);
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

    void
    TableMgr::update_roots(uint64_t db_id,
                           uint64_t table_id,
                           uint64_t target_xid,
                           const TableMetadata &metadata)
    {
        sys_tbl_mgr::Client::get_instance()->update_roots(db_id, table_id, target_xid, metadata);
    }
}
