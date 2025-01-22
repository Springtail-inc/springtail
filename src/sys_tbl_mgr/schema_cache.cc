#include <sys_tbl_mgr/schema_cache.hh>

namespace springtail::sys_tbl_mgr {
    SchemaMetadataPtr
    SchemaCache::get(uint64_t db,
                     uint64_t tid,
                     const XidLsn &xid,
                     PopulateFn populate)
    {
        std::unique_lock lock(_mutex);
        auto key = std::make_pair(db, tid);

        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "db {}, table {}, xid {}:{}", db, tid, xid.xid, xid.lsn);

        while (true) {
            // check for the schema at the access key
            auto schema_i = _schema_map.find(key);
            if (schema_i != _schema_map.end()) {
                auto entry = schema_i->second;

                // found a matching entry, check if it's being fetched
                if (entry->fetching) {
                    // block and try again once complete
                    entry->cond.wait(lock, [&entry]() {
                        return !entry->fetching;
                    });

                    // note: we perform the lookup again in case it was evicted between the
                    //       fetch and when this thread recieved control
                    continue; 
                }

                // not fetching, check if it covers the requested XID
                if (xid < entry->start_xid) {
                    // the requested XID is not the latest, so fetch, but don't cache
                    lock.unlock();
                    return populate(db, tid, xid);
                }

                // found the entry and it's valid, update the LRU position
                _lru.erase(entry->lru_i);
                entry->lru_i = _lru.insert(_lru.end(), schema_i);

                return entry->schema;
            }

            // not in the cache, retrieve it and try to cache it
            auto entry = _fetch_locked(lock, db, tid, xid, populate);

            // record the mapping of index OIDs in the cache to table OIDs to properly
            // invalidate on drop_index()
            if (!entry->invalidated) {
                for (const auto &index : entry->schema->indexes) {
                    if (index.id == constant::INDEX_PRIMARY) {
                        continue; // skip
                    }
                    auto key = std::make_pair(db, index.id);
                    _index_map.try_emplace(key, tid);
                }
            }

            return entry->schema;
        }
    }

    SchemaCache::SchemaEntryPtr
    SchemaCache::_fetch_locked(std::unique_lock<std::mutex> &lock,
                               uint64_t db,
                               uint64_t tid,
                               const XidLsn &xid,
                               PopulateFn populate)
    {
        SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "db {}, table {}, xid {}:{}", db, tid, xid.xid, xid.lsn);

        // create a dummy entry in the fetching state
        auto key = std::make_pair(db, tid);
        auto entry = std::make_shared<SchemaEntry>();
        auto &&result = _schema_map.try_emplace(key, entry);
        CHECK(result.second);

        // call the populate function for the cache
        SchemaMetadataPtr metadata;
        {
            lock.unlock();

            // must release the lock while calling this potentially-blocking function
            metadata = populate(db, tid, xid);

            SPDLOG_DEBUG_MODULE(LOG_SCHEMA, "db {}, table {}, start_xid {}:{}, end_xid {}:{}",
                                db, tid,
                                metadata->access_range.start.xid, metadata->access_range.start.lsn,
                                metadata->access_range.end.xid, metadata->access_range.end.lsn);


            lock.lock();
        }

        // if the entry wasn't invalidated during fetch, perform bookkeeping
        if (!entry->invalidated) {
            // check if the entry is still the latest known entry
            if (metadata->access_range.end != XidLsn(constant::LATEST_XID)) {
                // not the latest, can return it for this request, but don't cache it
                _schema_map.erase(result.first);
                entry->invalidated = true;

                if (_latest_xid[db] < metadata->access_range.end) {
                    _latest_xid[db] = metadata->access_range.end;
                }
            } else {
                // make space for the entry
                _make_space_locked();

                // place the entry on the back of the LRU list
                entry->lru_i = _lru.insert(_lru.end(), result.first);
            }
        }

        // populate the entry
        entry->schema = metadata;
        entry->start_xid = metadata->access_range.start;

        // notify anyone waiting for the entry fetch
        entry->fetching = false;
        entry->cond.notify_all();

        // return the entry
        return entry;
    }

    void
    SchemaCache::invalidate_table(uint64_t db,
                                  uint64_t tid,
                                  const XidLsn &xid)
    {
        std::unique_lock lock(_mutex);
        _invalidate_locked(db, tid, xid);
    }

    void
    SchemaCache::invalidate_db(uint64_t db,
                               const XidLsn &xid)
    {
        std::unique_lock lock(_mutex);

        // update the latest known XID with a schema change
        if (_latest_xid[db] < xid) {
            _latest_xid[db] = xid;
        }

        // generate a search key
        uint64_t tid = 0;
        auto key = std::make_pair(db, tid);

        auto schema_i = _schema_map.lower_bound(key);
        while (schema_i != _schema_map.end() && schema_i->first.first == db) {
            // clear the associated entry
            _remove_locked(schema_i);

            // move to the next entry
            schema_i = _schema_map.lower_bound(key);
        }
    }

    void
    SchemaCache::invalidate_by_index(uint64_t db,
                                     uint64_t index_id,
                                     const XidLsn &xid)
    {
        std::unique_lock lock(_mutex);

        // update the latest known XID with a schema change
        if (_latest_xid[db] < xid) {
            _latest_xid[db] = xid;
        }

        // find the table from the index map
        auto key = std::make_pair(db, index_id);
        auto index_i = _index_map.find(key);
        if (index_i == _index_map.end()) {
            return;
        }
        
        // perform the invalidation
        _invalidate_locked(db, index_i->second, xid);
    }

    void
    SchemaCache::_invalidate_locked(uint64_t db,
                                    uint64_t tid,
                                    const XidLsn &xid)
    {
        // update the latest known XID with a schema change
        if (_latest_xid[db] < xid) {
            _latest_xid[db] = xid;
        }

        // find the schema entry
        auto key = std::make_pair(db, tid);
        auto entry_i = _schema_map.find(key);
        if (entry_i == _schema_map.end()) {
            return;
        }

        // remove the entry
        _remove_locked(entry_i);
    }

    void
    SchemaCache::_remove_locked(SchemaMap::iterator schema_i)
    {
        if (!schema_i->second->fetching) {
            // clear the LRU entry
            _lru.erase(schema_i->second->lru_i);

            // clear any associated entries from the index map
            for (const auto &index : schema_i->second->schema->indexes) {
                if (index.id == constant::INDEX_PRIMARY) {
                    continue; // skip
                }

                auto key = std::make_pair(schema_i->first.first, index.id);
                CHECK_EQ(_index_map.erase(key), 1);
            }
        }

        // mark the entry as invalidated
        schema_i->second->invalidated = true;

        // clear the entry from the schema map
        _schema_map.erase(schema_i);
    }

    void
    SchemaCache::_make_space_locked()
    {
        // check if we have space in the map already
        if (_lru.size() < _capacity) {
            return;
        }

        // get the LRU entry
        auto schema_i = _lru.front();

        // remove the entry 
        _remove_locked(schema_i);
    }
}
