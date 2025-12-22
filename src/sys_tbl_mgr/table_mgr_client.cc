#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_table_mgr_client.hh>
#include <sys_tbl_mgr/table_mgr_client.hh>

namespace springtail {
    TablePtr
    TableMgrClient::get_table(uint64_t db_id,
                              uint64_t table_id,
                              uint64_t xid,
                              const ExtensionCallback &extension_callback)
    {
        // check the system tables
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgrClient::get_instance()->get_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table first
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);

        // Check the cache for an existing Table object
        TableCacheKey cache_key = {db_id, table_id};
        {
            std::lock_guard<std::mutex> lock(_cache_mutex);
            auto cached_table = _table_cache.get(cache_key);
            if (cached_table != nullptr) {
                // Found cached table. Check if we need to advance it to a new XID.
                if (cached_table->get_xid() == xid) {
                    // Exact match - return the cached table
                    return cached_table;
                } else if (cached_table->get_xid() < xid) {
                    // Table needs to be advanced to a newer XID
                    // Advance the cached table to the new XID
                    cached_table->advance_to_xid(xid, tbl_meta->roots);
                    return cached_table;
                }
                // If cached_table->get_xid() > xid, this shouldn't happen per user requirements
                // but if it does, we'll fall through and create a new table
            }
        }

        // Cache miss or new schema - construct a new Table
        auto schema = get_extent_schema(db_id, table_id, {xid, constant::MAX_LSN}, extension_callback);
        auto schema_without_row_id = get_extent_schema(db_id, table_id, {xid, constant::MAX_LSN}, extension_callback, false, false);

        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY && v.id != constant::INDEX_LOOK_ASIDE; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        auto new_table = std::make_shared<UserClientTable>(db_id, table_id, xid, _table_base,
                                                           schema->get_sort_keys(), secondary_indexes,
                                                           *tbl_meta, schema, schema_without_row_id, extension_callback);

        // Cache the new table
        {
            std::lock_guard<std::mutex> lock(_cache_mutex);
            _table_cache.insert(cache_key, new_table);
        }

        return new_table;
    }

    std::map<uint32_t, SchemaColumn>
    TableMgrClient::get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
    {
        // handle system tables
        if (table_id <= constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgrClient::get_instance()->get_columns(db_id, table_id, xid);
        }

        // non-system tables
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);
        return _convert_columns(meta->columns);
    }

    std::shared_ptr<Schema>
    TableMgrClient::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgrClient::get_instance()->get_schema(db_id, table_id, access_xid, target_xid);
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
    TableMgrClient::get_extent_schema(uint64_t db_id, uint64_t table_id,
                                      const XidLsn &xid, const ExtensionCallback &extension_callback,
                                      bool allow_undefined, bool include_internal_row_id)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgrClient::get_instance()->get_extent_schema(db_id, table_id, xid, extension_callback, allow_undefined);
        }

        // XXX keep some kind of local cache?  how to keep it valid given the XID progression?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);

        // construct the schema from the provided schema metadata
        return std::make_shared<ExtentSchema>(meta->columns, extension_callback, allow_undefined, include_internal_row_id);
    }

    void TableMgrClient::invalidate_table_cache_on_schema_change()
    {
        std::lock_guard<std::mutex> lock(_cache_mutex);
        _table_cache.clear();
    }

} // namespace
