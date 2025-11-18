#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>

namespace springtail {

    TablePtr
    TableMgr::get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, const ExtensionCallback &extension_callback)
    {
        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Server::get_instance()->get_roots(db_id, table_id, xid);

        // construct the table and return it
        auto schema = get_extent_schema(db_id, table_id, {xid, constant::MAX_LSN}, extension_callback);

        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY && v.id != constant::INDEX_LOOK_ASIDE; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        return std::make_shared<UserTable>(db_id, table_id, xid, _table_base,
                                        schema->get_sort_keys(), secondary_indexes,
                                        *tbl_meta, schema, extension_callback);
    }

    std::optional<std::filesystem::path>
    TableMgr::get_table_data_dir(uint64_t db_id, uint64_t table_id, uint64_t xid)
    {
        auto&& table_meta = sys_tbl_mgr::Server::get_instance()->get_roots(db_id, table_id, xid);
        if (table_meta == nullptr) {
            return std::nullopt;
        }
        return table_helpers::get_table_dir(_table_base, db_id, table_id, table_meta->snapshot_xid);
    }

    MutableTablePtr
    TableMgr::get_mutable_table(uint64_t db_id,
                                uint64_t table_id,
                                uint64_t access_xid,
                                uint64_t target_xid,
                                const ExtensionCallback &extension_callback)
    {
        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_mutable_system_table(db_id, table_id, access_xid, target_xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Server::get_instance()->get_roots(db_id, table_id, access_xid);
        if (tbl_meta == nullptr) {
            tbl_meta = std::make_shared<TableMetadata>();
        }

        // construct the mutable table and return it
        XidLsn xid(target_xid);
        auto schema = get_extent_schema(db_id, table_id, xid, extension_callback);
        auto schema_without_row_id = get_extent_schema(db_id, table_id, xid, extension_callback, false, false);

        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY && v.id != constant::INDEX_LOOK_ASIDE; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        LOG_DEBUG(LOG_BTREE, LOG_LEVEL_DEBUG1, "Get mutable table: table {}, access_xid {}", table_id, access_xid);

#ifdef DEBUG
        for (auto &root : tbl_meta->roots) {
            LOG_DEBUG(LOG_BTREE, "Get mutable table: index {}, root {}", root.index_id, root.extent_id);
        }
#endif

        return std::make_shared<UserMutableTable>(db_id, table_id, access_xid, target_xid,
                                                  _table_base, schema->get_sort_keys(), secondary_indexes,
                                                  *tbl_meta, schema, schema_without_row_id, extension_callback);
    }

    MutableTablePtr
    TableMgr::get_snapshot_table(uint64_t db_id,
                                 uint64_t table_id,
                                 uint64_t snapshot_xid,
                                 ExtentSchemaPtr schema,
                                 const std::vector<Index>& secondary_keys,
                                 const ExtensionCallback &extension_callback,
                                 const OpClassHandler &opclass_handler
                                )
    {
        TableMetadata tbl_meta{};
        tbl_meta.snapshot_xid = snapshot_xid;

        // NOTE: in the case of a failure, there may be a partially copied table already present in
        //       the directory structure, so we need to make sure to delete it before we try to
        //       create it below
        auto table_dir = table_helpers::get_table_dir(_table_base, db_id, table_id, snapshot_xid);
        if (std::filesystem::exists(table_dir)) {
            std::filesystem::remove_all(table_dir);
        }

        // Create schema with internal row ID field
        auto schema_with_row_id = schema->create_schema(schema->column_order(), {}, schema->get_sort_keys(), extension_callback);

        // construct an empty mutable table with the provided snapshot XID and return it
        return std::make_shared<UserMutableTable>(db_id, table_id, snapshot_xid, snapshot_xid,
                                                  _table_base, schema_with_row_id->get_sort_keys(), secondary_keys,
                                                  tbl_meta, schema_with_row_id, schema, extension_callback, opclass_handler);
    }

    std::map<uint32_t, SchemaColumn>
    TableMgr::get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
    {
        // handle system tables
        if (table_id <= constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_columns(db_id, table_id, xid);
        }

        // non-system tables
        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(db_id, table_id, xid);
        return _convert_columns(meta->columns);
    }

    std::shared_ptr<Schema>
    TableMgr::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_schema(db_id, table_id, access_xid, target_xid);
        }

        // XXX keep some kind of local cache?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_target_schema(db_id, table_id, access_xid, target_xid);

        // construct the schema object
        if (meta->history.empty()) {
            return std::make_shared<ExtentSchema>(meta->columns);
        }

        return std::make_shared<VirtualSchema>(*meta);
    }

    std::shared_ptr<ExtentSchema>
    TableMgr::get_extent_schema(uint64_t db_id, uint64_t table_id,
                                const XidLsn &xid, const ExtensionCallback &extension_callback, bool allow_undefined,
                                bool include_internal_row_id)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_extent_schema(db_id, table_id, xid, extension_callback, allow_undefined);
        }

        // XXX keep some kind of local cache?  how to keep it valid given the XID progression?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Server::get_instance()->get_schema(db_id, table_id, xid);

        // construct the schema from the provided schema metadata
        return std::make_shared<ExtentSchema>(meta->columns, extension_callback, allow_undefined, include_internal_row_id);
    }

    void
    UserMutableTable::truncate()
    {
        // remove any dirty cached pages for this table since they don't need to be written
        StorageCache::get_instance()->drop_for_truncate(_data_file);

        // clear the indexes
        TableMetadata metadata;
        metadata.snapshot_xid = _snapshot_xid;
        metadata.roots = {{ constant::INDEX_PRIMARY, constant::UNKNOWN_EXTENT }};
        _primary_index->truncate();
        for (auto& [index_id, idx]: _secondary_indexes) {
            idx.first->truncate();
            metadata.roots.emplace_back(index_id, constant::UNKNOWN_EXTENT);
        }

        // update the roots and stats
        sys_tbl_mgr::Server::get_instance()->update_roots(_db_id, _id, _target_xid, metadata);

        // Smart vacuum if data exists
        if (std::filesystem::exists(_data_file)) {
            Vacuumer::get_instance()->expire_extent(_data_file, 0, std::filesystem::file_size(_data_file), _target_xid);
        } else {
            LOG_INFO("TRUNCATE: File: {} doesn't exist to report to vacuum", _data_file);
        }
    }
}
