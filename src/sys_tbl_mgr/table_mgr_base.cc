#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>

#include <sys_tbl_mgr/table_mgr_base.hh>

namespace springtail {
    TableMgrBase::TableMgrBase()
    {
        // get the base directory for table data
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", _table_base);
        _table_base = Properties::make_absolute_path(_table_base);
    }

    std::map<uint32_t, SchemaColumn>
    TableMgrBase::_convert_columns(const std::vector<SchemaColumn> &columns)
    {
        std::map<uint32_t, SchemaColumn> column_map;
        for (auto &&column : columns) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "col_pos={} col_name={}",
                        column.position, column.name);
            column_map.emplace(column.position, column);
        }
        return column_map;
    }

    ExtentSchemaPtr
    TableMgrBase::_lookup_cached_schema(const ExtentSchemaCacheKey& key, const XidLsn& xid)
    {
        std::lock_guard lock(_extent_schema_cache._mutex);

        auto it = _extent_schema_cache._entries.find(key);
        if (it == _extent_schema_cache._entries.end()) {
            _extent_schema_cache._misses++;
            return nullptr;
        }

        // Find the schema version valid at the requested XID
        ExtentSchemaPtr schema = it->second.find_schema_at(xid);
        if (!schema) {
            _extent_schema_cache._misses++;
            return nullptr;
        }

        // Found - update LRU and return
        _extent_schema_cache._hits++;
        _touch_entry(it->second);

        return schema;
    }

    void
    TableMgrBase::_cache_schema(const ExtentSchemaCacheKey& key,
                                 ExtentSchemaPtr schema,
                                 const XidLsn& valid_from)
    {
        std::lock_guard lock(_extent_schema_cache._mutex);

        auto it = _extent_schema_cache._entries.find(key);
        if (it == _extent_schema_cache._entries.end()) {
            // New entry - check if we need to evict
            if (_extent_schema_cache._entries.size() >= _extent_schema_cache._max_entries) {
                _evict_lru_entries(_extent_schema_cache._max_entries / 10);  // Evict 10%
            }

            // Add to LRU list (most recently used = back)
            _extent_schema_cache._lru_list.push_back(key);
            auto lru_it = std::prev(_extent_schema_cache._lru_list.end());

            // Create new entry
            ExtentSchemaCacheEntry entry;
            entry._lru_iterator = lru_it;
            entry._versions[valid_from] = schema;
            entry._version_validity[valid_from] = XidLsn(constant::LATEST_XID);

            _extent_schema_cache._entries.emplace(key, std::move(entry));
        } else {
            // Existing entry - add new version
            it->second._versions[valid_from] = schema;
            it->second._version_validity[valid_from] = XidLsn(constant::LATEST_XID);

            // Update LRU
            _touch_entry(it->second);
        }
    }

    void
    TableMgrBase::_touch_entry(ExtentSchemaCacheEntry& entry)
    {
        // Caller must hold lock on _extent_schema_cache._mutex

        // Get the key from the iterator
        const ExtentSchemaCacheKey& key = *entry._lru_iterator;

        // Remove from current position in LRU list
        _extent_schema_cache._lru_list.erase(entry._lru_iterator);

        // Add to back (most recently used)
        _extent_schema_cache._lru_list.push_back(key);

        // Update iterator in entry
        entry._lru_iterator = std::prev(_extent_schema_cache._lru_list.end());
    }

    void
    TableMgrBase::_evict_lru_entries(size_t count)
    {
        // Caller must hold lock on _extent_schema_cache._mutex

        auto it = _extent_schema_cache._lru_list.begin();
        for (size_t i = 0; i < count && it != _extent_schema_cache._lru_list.end(); ++i) {
            const ExtentSchemaCacheKey& key = *it;

            // Remove from entries map
            _extent_schema_cache._entries.erase(key);

            // Remove from LRU list
            it = _extent_schema_cache._lru_list.erase(it);

            _extent_schema_cache._evictions++;
        }
    }

    void
    TableMgrBase::record_schema_change(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
    {
        std::lock_guard lock(_extent_schema_cache._mutex);
        _extent_schema_cache._schema_change_xids[{db_id, table_id}].insert(xid);

        // Update valid_until for entries that are now superseded
        for (auto& [key, entry] : _extent_schema_cache._entries) {
            if (key.db_id == db_id && key.table_id == table_id) {
                // Find all versions that need their valid_until updated
                for (auto& [valid_from, valid_until] : entry._version_validity) {
                    if (valid_until == XidLsn(constant::LATEST_XID) && valid_from < xid) {
                        entry._version_validity[valid_from] = xid;
                    }
                }
            }
        }
    }

    void
    TableMgrBase::on_xid_committed(uint64_t committed_xid)
    {
        std::lock_guard lock(_extent_schema_cache._mutex);

        // Remove all schema versions that are no longer valid after this XID
        auto it = _extent_schema_cache._entries.begin();
        while (it != _extent_schema_cache._entries.end()) {
            // Remove expired versions from this entry
            auto version_it = it->second._versions.begin();
            while (version_it != it->second._versions.end()) {
                auto valid_until_it = it->second._version_validity.find(version_it->first);
                if (valid_until_it != it->second._version_validity.end() &&
                    valid_until_it->second.xid < committed_xid) {
                    // This version is no longer valid
                    it->second._version_validity.erase(valid_until_it);
                    version_it = it->second._versions.erase(version_it);
                    _extent_schema_cache._evictions++;
                } else {
                    ++version_it;
                }
            }

            // If entry has no more versions, remove the entire entry
            if (it->second._versions.empty()) {
                // Remove from LRU list
                _extent_schema_cache._lru_list.erase(it->second._lru_iterator);

                // Remove from entries map
                it = _extent_schema_cache._entries.erase(it);
            } else {
                ++it;
            }
        }
    }

} // springtail