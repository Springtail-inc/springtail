#pragma once

#include <common/init.hh>

#include <storage/schema.hh>
#include <sys_tbl_mgr/table_mgr_base.hh>

namespace springtail {

    /**
     * Base class for system table management. Provides read-only access to system tables.
     * Use SystemTableMgrClient on client side or SystemTableMgrServer on server side.
     */
    class SystemTableMgr : public TableMgrBase
    {
    public:
        /**
         * Construct a system table.
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
                          const XidLsn &xid, const ExtensionCallback &extension_callback = {},
                          bool allow_undefined = false, bool include_internal_row_id = true) override;

    protected:
        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param table_id The ID of the table being requested.
         */
        std::shared_ptr<Schema>
        _get_schema(uint64_t table_id);

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent. This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param table_id The table we need the schema for.
         */
        std::shared_ptr<ExtentSchema>
        _get_extent_schema(uint64_t table_id);

        SystemTableMgr();
        ~SystemTableMgr() override = default;

        template<typename Table>
        static std::vector<Index> _get_secondary_keys();

        /**
         * A key for the system schema cache.
         */
        struct SystemKey {
            uint64_t table_id;
            uint64_t index_id;
            bool is_leaf;

            SystemKey(uint64_t t, uint64_t i, bool l)
                : table_id(t),
                  index_id(i),
                  is_leaf(l)
            { }

            bool operator==(const SystemKey &other) const = default;

            friend std::size_t hash_value(const SystemKey &k)
            {
                std::size_t seed = 0;

                boost::hash_combine(seed, k.table_id);
                boost::hash_combine(seed, k.index_id);
                boost::hash_combine(seed, k.is_leaf);

                return seed;
            }
        };

        /** A map of fixed system schemas.  Maps from System Table ID to the ExtentSchema for that table. */
        std::unordered_map<SystemKey, std::shared_ptr<ExtentSchema>, boost::hash<SystemKey>> _system_cache;
    };

} // springtail
