#pragma once

#include <common/init.hh>
#include <common/object_cache.hh>
#include <sys_tbl_mgr/table_mgr_base.hh>
#include <mutex>

namespace springtail {
    class TableMgrClient : public Singleton<TableMgrClient>, public TableMgrBase
    {
        friend class Singleton<TableMgrClient>;
    public:
        /**
         * Read the table metadata for the requested table ID.  Note that Table objects's are always
         * constructed at lsn == MAX_LSN within the provided xid.
         * Cached Table objects are reused across calls when appropriate and advanced to new XIDs
         * without full reconstruction when possible.
         */
        virtual TablePtr
        get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, const ExtensionCallback &extension_callback = {}) override;

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        virtual std::map<uint32_t, SchemaColumn>
        get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid) override;

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual std::shared_ptr<Schema>
        get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid) override;

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual std::shared_ptr<ExtentSchema>
        get_extent_schema(uint64_t db_id, uint64_t table_id,
                          const XidLsn &xid, const ExtensionCallback &extension_callback,
                          bool allow_undefined = false, bool include_internal_row_id = true) override;

        /**
         * Invalidate the cached Table object when schema_xid changes.
         * This clears the entire cache so that Tables will be reconstructed with new schemas.
         */
        void invalidate_table_cache_on_schema_change();

    private:
        /**
         * @brief Construct a new Table Mgr Client singleton object
         *
         */
        TableMgrClient() : Singleton<TableMgrClient>(ServiceId::TableMgrClientId),
                          _table_cache(128) {}  // Cache up to 128 Table objects
        ~TableMgrClient() override = default;

        /**
         * Cache key for Table objects: (db_id, table_id)
         */
        using TableCacheKey = std::pair<uint64_t, uint64_t>;

        struct TableCacheKeyHash {
            std::size_t operator()(const TableCacheKey &k) const {
                return boost::hash<uint64_t>()(k.first) ^ (boost::hash<uint64_t>()(k.second) << 1);
            }
        };

        LruObjectCache<TableCacheKey, Table, TableCacheKeyHash> _table_cache;  ///< LRU cache for Table objects
        std::mutex _cache_mutex;                                               ///< Protects concurrent access to cache
    };

    /**
     * @brief This class is for representing user tables on the client side. It will use TableMgrClient
     *      to get schema and extent schema. In turn TblMgrClient will use system table manager client calls
     *      to obtain information for constructing the table.
     *
     */
    class UserClientTable : public Table, public std::enable_shared_from_this<UserClientTable> {
    public:
        /**
         * UserTable constructor.
         */
        UserClientTable(uint64_t db_id,
                        uint64_t table_id,
                        uint64_t xid,
                        const std::filesystem::path &table_base,
                        const std::vector<std::string> &primary_key,
                        const std::vector<Index> &secondary,
                        const TableMetadata &metadata,
                        ExtentSchemaPtr schema,
                        ExtentSchemaPtr schema_without_row_id,
                        const ExtensionCallback &extension_callback) :
            Table(db_id, table_id, xid, table_base, primary_key, secondary, metadata, schema, schema_without_row_id, extension_callback) {}

    };

} // springtail
