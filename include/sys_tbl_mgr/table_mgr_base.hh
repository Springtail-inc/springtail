#pragma once

#include <boost/thread.hpp>
#include <filesystem>
#include <list>
#include <mutex>
#include <unordered_map>

#include <sys_tbl_mgr/table.hh>

namespace springtail {

    /** Type of ExtentSchema being cached */
    enum class ExtentSchemaType : uint8_t {
        TABLE,           // Base table schema from get_extent_schema()
        INDEX,           // Secondary index schema (has index_id)
        PRIMARY_INDEX,   // Primary index schema (index_id = 0)
        LOOK_ASIDE,      // Look-aside index schema (maps internal_row_id to extent_id/row_id)
        BTREE_BRANCH,    // Branch schema for MutableBTree internal nodes
        PG_LOG_BATCH,    // PgLogReader batch schema (with __springtail_op/lsn)
        CUSTOM           // Other derived schemas from create_schema()
    };

    /** Key for looking up cached ExtentSchema objects (without XID since entries store version ranges) */
    struct ExtentSchemaCacheKey {
        uint64_t db_id;
        uint64_t table_id;
        ExtentSchemaType type;
        uint64_t type_param;     // index_id for INDEX types, 0 otherwise

        bool operator==(const ExtentSchemaCacheKey& other) const {
            return db_id == other.db_id && table_id == other.table_id &&
                   type == other.type && type_param == other.type_param;
        }

        size_t hash() const noexcept {
            size_t h1 = std::hash<uint64_t>{}(db_id);
            size_t h2 = std::hash<uint64_t>{}(table_id);
            size_t h3 = std::hash<uint8_t>{}(static_cast<uint8_t>(type));
            size_t h4 = std::hash<uint64_t>{}(type_param);
            return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
        }
    };

} // springtail

// Hash specialization for ExtentSchemaCacheKey - must be defined before use in unordered_map
namespace std {
    template<>
    struct hash<springtail::ExtentSchemaCacheKey> {
        size_t operator()(const springtail::ExtentSchemaCacheKey& k) const noexcept {
            return k.hash();
        }
    };
}

namespace springtail {

    class TableMgrBase {
    public:
        /**
         * @brief Construct a new Table Mgr Base object
         *
         */
        TableMgrBase();
        virtual ~TableMgrBase() = default;

        /**
         * @brief Read the table metadata for the requested table ID.
         *
         * @param db_id - database id
         * @param table_id - table id
         * @param xid - transaction id
         * @return TablePtr - pointer to the table object
         */
        virtual TablePtr
        get_table(uint64_t db_id, uint64_t table_id, uint64_t xid, const ExtensionCallback &extension_callback = {}) = 0;

        /**
         * Retrieve the column metadata for a given table at a given XID/LSN.
         * Map from column ID/column position to column metadata.
         */
        virtual std::map<uint32_t, SchemaColumn>
        get_columns(uint64_t db_id, uint64_t table_id, const XidLsn &xid) = 0;

        /**
         * Retrieve the schema for a given table at a given point in time.
         * @param db_id The database ID of the table.
         * @param table_id The ID of the table being requested.
         * @param extent_xid The XID of the extent being processed.
         * @param target_xid The XID that the query is executing at.
         * @param lsn An optional LSN (logical sequence number) which tells you which schema changes within a given XID to apply up through.
         */
        virtual std::shared_ptr<Schema>
        get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid) = 0;

        /**
         * Retrieve an ExtentSchema for a given table at a given XID that can be used for writing /
         * updating the extent.  This function assumes we are retrieving the schema of the table's
         * underlying data.
         * @param db_id The database ID of the table.
         * @param table_id The table we need the schema for.
         * @param xid The XID/LSN that we need the schema at. Defaults to the MAX_LSN, providing the
         *            schema at the point after all changes in the XID have been applied.
         */
        virtual std::shared_ptr<ExtentSchema>
        get_extent_schema(uint64_t db_id, uint64_t table_id,
                          const XidLsn &xid, const ExtensionCallback &extension_callback = {},
                          bool allow_undefined = false, bool include_internal_row_id = true) = 0;

        /**
         * @brief Get the base path for the tables storage location
         *
         * @return std::filesystem::path file path
         */
        std::filesystem::path get_table_base() const { return _table_base; }

    protected:
        /** Helper to convert schema column to map */
        static std::map<uint32_t, SchemaColumn>
        _convert_columns(const std::vector<SchemaColumn> &columns);

        std::filesystem::path _table_base; ///< The base directory for individual table directories.

        /** Cache entry holding multiple versions of ExtentSchema with validity ranges */
        struct ExtentSchemaCacheEntry {
            // Multiple versions of the schema, ordered by valid_from XID
            std::map<XidLsn, ExtentSchemaPtr> _versions;  // key = valid_from

            // Validity range for each version: valid_from -> valid_until
            std::map<XidLsn, XidLsn> _version_validity;

            // Iterator into LRU list for O(1) removal
            std::list<ExtentSchemaCacheKey>::iterator _lru_iterator;

            /** Find schema valid at given XID */
            ExtentSchemaPtr find_schema_at(const XidLsn& xid) const {
                // Find the first version where valid_from > xid
                auto it = _versions.upper_bound(xid);
                if (it == _versions.begin()) {
                    return nullptr; // No version is old enough
                }
                --it; // Go back to the version where valid_from <= xid

                // Check if this version is still valid at xid
                auto valid_until_it = _version_validity.find(it->first);
                if (valid_until_it != _version_validity.end() && xid < valid_until_it->second) {
                    return it->second;
                }
                return nullptr;
            }
        };

        /** ExtentSchema cache with LRU eviction and XID-based invalidation */
        struct ExtentSchemaCache {
            // Primary storage: map from cache key to schema entry (which holds multiple versions)
            std::unordered_map<ExtentSchemaCacheKey, ExtentSchemaCacheEntry> _entries;

            // LRU tracking: list of keys, front = least recently used, back = most recently used
            std::list<ExtentSchemaCacheKey> _lru_list;

            // XID tracking: map from (db_id, table_id) to set of XIDs with schema changes
            std::map<std::pair<uint64_t, uint64_t>, std::set<XidLsn>> _schema_change_xids;

            // Statistics
            uint64_t _hits = 0;
            uint64_t _misses = 0;
            uint64_t _evictions = 0;

            size_t _max_entries = 10000;    // Configurable via property

            mutable std::mutex _mutex;  // Simple mutex since reads modify LRU
        };

        ExtentSchemaCache _extent_schema_cache;

        // Cache interface methods
        ExtentSchemaPtr _lookup_cached_schema(const ExtentSchemaCacheKey& key, const XidLsn& xid);
        void _cache_schema(const ExtentSchemaCacheKey& key, ExtentSchemaPtr schema, const XidLsn& valid_from);
        void _evict_lru_entries(size_t count);
        void _touch_entry(ExtentSchemaCacheEntry& entry);

    public:
        /** Record that a schema change occurred at the given XID */
        void record_schema_change(uint64_t db_id, uint64_t table_id, const XidLsn& xid);

        /** Invalidate cached schemas that are no longer valid after XID commit */
        void on_xid_committed(uint64_t committed_xid);
    };
} // springtail
