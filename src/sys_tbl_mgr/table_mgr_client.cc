#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>
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
            return SystemTableMgr::get_instance()->get_table(db_id, table_id, xid);
        }

        // retrieve the roots and stats of the table
        auto &&tbl_meta = sys_tbl_mgr::Client::get_instance()->get_roots(db_id, table_id, xid);

        // construct the table and return it
        auto schema = get_extent_schema(db_id, table_id, {xid, constant::MAX_LSN}, extension_callback);

        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, XidLsn{xid});

        // pass secondary indexes only
        auto filtered = std::views::filter(meta->indexes, [](auto const& v) { return v.id != constant::INDEX_PRIMARY; });
        std::vector<Index> secondary_indexes(filtered.begin(), filtered.end());

        return std::make_shared<UserClientTable>(db_id, table_id, xid, _table_base,
                                                 schema->get_sort_keys(), secondary_indexes,
                                                 *tbl_meta, schema, extension_callback);
    }

    std::map<uint32_t, SchemaColumn>
    TableMgrClient::get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
    {
        // handle system tables
        if (table_id <= constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_columns(db_id, table_id, xid);
        }

        // non-system tables
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);
        return _convert_columns(meta->columns);
    }

    std::shared_ptr<Schema>
    TableMgrClient::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_schema(db_id, table_id, access_xid, target_xid);
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
                                      const XidLsn &xid, const ExtensionCallback &extension_callback, bool allow_undefined)
    {
        if (table_id < constant::MAX_SYSTEM_TABLE_ID) {
            return SystemTableMgr::get_instance()->get_extent_schema(db_id, table_id, xid, extension_callback, allow_undefined);
        }

        // XXX keep some kind of local cache?  how to keep it valid given the XID progression?

        // call into the SysTblMgr to get the schema at the given XID/LSN
        auto &&meta = sys_tbl_mgr::Client::get_instance()->get_schema(db_id, table_id, xid);

        // construct the schema from the provided schema metadata
        return std::make_shared<ExtentSchema>(meta->columns, extension_callback, allow_undefined);
    }

} // namespace
