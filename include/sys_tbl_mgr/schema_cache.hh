#include <list>

#include <sys_tbl_mgr/table.hh>

namespace springtail::sys_tbl_mgr {
    /**
     * A class for holding schema metadata from the sys_tbl_mgr::Service.  It caches both the full
     * schema metadata as well as mutation history entries.  History entries are attached to a
     * schema entry and represent all of the changes required to get the provided entry to either
     * the next cached entry, or to the latest xid.  To evict history entries, we must also evict
     * the earliest cached schema for a given db/table, which will allow us to free the associated
     * history entries.
     */
    class SchemaCache {
    public:
        struct Key {
            uint64_t db = 0; ///< The database this entry covers
            uint64_t tid = 0; ///< The table this entry covers

            /** In the cache, this is the ending XID/LSN of the range this entry covers, not
             *  inclusive.  When used for lookup it is the requested XID/LSN position. */
            XidLsn xid;

            Key() = default;
            Key(uint64_t db, uint64_t tid)
                : db(db), tid(tid)
            { }
            Key(uint64_t db, uint64_t tid, const XidLsn &xid)
                : db(db), tid(tid), xid(xid)
            { }

            void set_max_end() {
                xid = XidLsn{constant::LATEST_XID};
            }

            void set_end(const XidLsn &end_xid) {
                xid = end_xid;
            }

            bool verify_table(const Key &key) const {
                return (db == key.db && tid == key.tid);
            }

            bool operator<(const Key &rhs) const {
                return (db < rhs.db || (db == rhs.db &&
                                        (tid < rhs.tid || (tid == rhs.tid &&
                                                           xid < rhs.xid))));
            }
        };

        using PopulateFn = std::function<SchemaMetadataPtr(const Key &)>;
        using PopulateRangeFn = std::function<SchemaMetadataPtr(const Key &, const Key &)>;

    public:
        SchemaCache(int schema_max, int history_max)
            : _schema_max(schema_max),
              _history_max(history_max)
        { }

        SchemaMetadataPtr
        get(const Key &access_key,
            PopulateFn populate)
        {
            std::unique_lock lock(_mutex);

            while (true) {
                // check for the schema at the access key
                auto entry_i = _schema_map.upper_bound(access_key);
                if (entry_i == _schema_map.end() ||
                    !entry_i->first.verify_table(access_key) ||
                    !entry_i->second->verify_xid(access_key)) {
                    // note: we could check if there's an earlier schema and then use the history of
                    //       mutations to roll-forward to the requested access schema

                    // note: there are two cases here:
                    // 1) the search key is prior to an existing entry, in which case we can use the
                    //    start of the next entry as the key of the entry while fetching
                    // 2) there is no following entry for this key, in which case we can use the
                    //    LATEST_XID as the key of the entry while fetching
                    // either way, we will need to re-insert once we have the true ending key, meaning
                    // anyone waiting for the fetch may find that the retrieved entry doesn't actually
                    // meet their needs
                    Key ending_key(access_key.db, access_key.tid);
                    if (entry_i == _schema_map.end() ||
                        !entry_i->first.verify_table(access_key)) {
                        ending_key.set_max_end();
                    } else {
                        ending_key.set_end(entry_i->first.xid);
                    }

                    // missing the schema, need to populate it
                    return _populate_get(access_key, ending_key, populate);
                } else {
                    if (entry_i->second->fetching) {
                        // wait until the fetch is complete, then check again in case the fetched
                        // entry doesn't actually cover the range including your requested XID
                        entry_i->second->cond.wait(lock, [&entry_i]() {
                            return !entry_i->second->fetching;
                        });
                    } else {
                        // update the entry's position in the LRU list
                        _lru.erase(entry_i->second->lru_i);
                        entry_i->second->lru_i = _lru.insert(_lru.end(), entry_i);

                        // return the value
                        auto metadata = std::make_shared<SchemaMetadata>();
                        metadata->columns = entry_i->second->schema;
                        return metadata;
                    }
                }
            }
        }

        /**
         * Internal helper to populate the cache using the provided populate function.
         */
        SchemaMetadataPtr
        _populate_get(const Key &access_key,
                      const Key &ending_key,
                      PopulateFn populate)
        {
            // make space for the entry
            _make_schema_space();

            // create a dummy entry in the fetching state
            auto &&result = _schema_map.try_emplace(ending_key, std::make_shared<SchemaEntry>());
            if (!result.second) {
                throw Error();
            }
            auto entry_i = result.first;

            // call the populate function for the cache
            SchemaMetadataPtr metadata;
            {
                std::unique_lock lock(_mutex, std::adopt_lock);
                lock.unlock();

                // must release the lock while calling this potentially-blocking function
                metadata = populate(access_key);

                lock.lock();
                lock.release();
            }

            // populate the entry
            entry_i->second->schema = metadata->columns;
            entry_i->second->start_xid = metadata->access_range.start;

            // place the entry on the back of the LRU list
            entry_i->second->lru_i = _lru.insert(_lru.end(), entry_i);

            // notify anyone waiting for the entry fetch
            entry_i->second->fetching = false;
            entry_i->second->cond.notify_all();
            
            // reinsert the entry into the cache with the correct ending XID
            Key corrected_key;
            corrected_key.db = access_key.db;
            corrected_key.tid = access_key.tid;
            corrected_key.xid = metadata->access_range.end;

            auto value = entry_i->second;
            _schema_map.erase(entry_i);
            result = _schema_map.try_emplace(corrected_key, value);
            if (!result.second) {
                throw Error();
            }

            // return the value
            return metadata;
        }

        SchemaMetadataPtr
        get_range(const Key &access_key,
                  const Key &target_key,
                  PopulateRangeFn populate)
        {
            std::unique_lock lock(_mutex);

            // check for the schema at the access key
            auto entry_i = _schema_map.upper_bound(access_key);
            if (entry_i == _schema_map.end() ||
                !entry_i->first.verify_table(access_key) ||
                !entry_i->second->verify_xid(access_key)) {
                // note: we could check if there's an earlier schema and then use the history of
                //       mutations to roll-forward to the requested access schema

                Key ending_key(access_key.db, access_key.tid);
                if (entry_i == _schema_map.end() ||
                    !entry_i->first.verify_table(access_key)) {
                    ending_key.set_max_end();
                } else {
                    ending_key.set_end(entry_i->first.xid);
                }

                // missing the schema, need to populate it
                return _populate_get_range(access_key, ending_key, target_key, populate);
            }

            // we found the access schema, now collect history until we have covered the target key
            auto metadata = std::make_shared<SchemaMetadata>();
            metadata->columns = entry_i->second->schema;
            auto current_xid = access_key.xid;
            while (current_xid < target_key.xid &&
                   entry_i != _schema_map.end() &&
                   entry_i->first.verify_table(access_key)) {
                for (const auto &history : entry_i->second->history) {
                    current_xid = XidLsn(history.xid, history.lsn);
                    if (current_xid > target_key.xid) {
                        break;
                    }
                    metadata->history.push_back(history);
                }

                // move to the next schema in the history
                ++entry_i;
            }

            return metadata;
        }

        /**
         * Internal helper to populate the cache using the provided populate function.
         */
        SchemaMetadataPtr
        _populate_get_range(const Key &access_key,
                            const Key &ending_key,
                            const Key &target_key,
                            PopulateRangeFn populate)
        {
            // make space for the entry
            _make_schema_space();

            // create a dummy entry in the fetching state
            auto &&result = _schema_map.try_emplace(ending_key, std::make_shared<SchemaEntry>());
            if (!result.second) {
                throw Error();
            }
            auto entry_i = result.first;

            // call the populate function for the cache
            SchemaMetadataPtr metadata;
            {
                std::unique_lock lock(_mutex, std::adopt_lock);
                lock.unlock();

                // must release the lock while calling this potentially-blocking function
                metadata = populate(access_key, target_key);

                lock.lock();
                lock.release();
            }

            // make space for the history
            _make_history_space(metadata->history.size());

            // populate the entry
            entry_i->second->schema = metadata->columns;
            entry_i->second->start_xid = metadata->access_range.start;

            // populate the history for this entry
            for (const auto &change : metadata->history) {
                XidLsn change_xid(change.xid, change.lsn);
                if (change_xid > ending_key.xid) {
                    break;
                }

                entry_i->second->history.push_back(change);
            }

            // place the entry on the back of the LRU list
            entry_i->second->lru_i = _lru.insert(_lru.end(), entry_i);

            // notify anyone waiting for the entry fetch
            entry_i->second->fetching = false;
            entry_i->second->cond.notify_all();
            
            // reinsert the entry into the cache with the correct ending XID
            Key corrected_key;
            corrected_key.db = access_key.db;
            corrected_key.tid = access_key.tid;
            corrected_key.xid = metadata->access_range.end;

            auto value = entry_i->second;
            _schema_map.erase(entry_i);
            result = _schema_map.try_emplace(corrected_key, value);
            if (!result.second) {
                throw Error();
            }

            // return the value
            return metadata;
        }

        /**
         * Checks for an entry that contains the key and re-indexes it with the key as the ending
         * position for the entry.  Used to mark the "current" entry for a schema or root as ending
         * at the given key.
         */
        void
        reinsert(const Key &key)
        {
            std::unique_lock lock(_mutex);

            // check if the key exists in the cache
            auto entry_i = _schema_map.upper_bound(key);
            if (entry_i == _schema_map.end() ||
                !entry_i->first.verify_table(key) ||
                !entry_i->second->verify_xid(key)) {
                return;
            }

            // insert the value using the new key
            auto &&result = _schema_map.try_emplace(key, entry_i->second);
            if (!result.second) {
                throw Error();
            }

            // update the LRU list pointer to the new map entry
            *(entry_i->second->lru_i) = result.first;

            // remove the old entry from the map
            _schema_map.erase(entry_i);
        }

    private:
        void
        _make_schema_space()
        {
            // check if we have space in the map already
            if (_schema_map.size() < _schema_max) {
                return;
            }

            // get the LRU entry
            auto schema_i = _lru.front();
            _lru.pop_front();

            // if this is not the first entry for the table, keep the change history
            if (schema_i != _schema_map.begin()) {
                auto prior_i = schema_i;
                --prior_i;

                if (prior_i->first.verify_table(schema_i->first)) {
                    // move all of the history entries to the end of the prior entry
                    prior_i->second->history.splice(prior_i->second->history.end(),
                                                    std::move(schema_i->second->history));
                }
            }

            // clear the entry from the schema map
            _history_size -= schema_i->second->history.size();
            _schema_map.erase(schema_i);
        }

        void
        _make_history_space(int count)
        {
            assert(count <= _history_max); // sanity check

            // check if we have space for the history already
            if (_history_size + count <= _history_max) {
                return;
            }

            // clear historical schemas until it frees enough history space
            // note: there are probably better ways of doing this, but this is easy and probably
            //       close enough to right for now
            while (_history_size + count > _history_max) {
                _make_schema_space();
            }
        }

    private:
        struct SchemaEntry;
        using SchemaEntryPtr = std::shared_ptr<SchemaEntry>;
        using SchemaMap = std::map<Key, SchemaEntryPtr>;
        using LruList = std::list<typename SchemaMap::iterator>;

        struct SchemaEntry {
            XidLsn start_xid;
            std::vector<SchemaColumn> schema;
            std::list<SchemaColumn> history;

            LruList::iterator lru_i;
            bool fetching = true;
            std::condition_variable cond;

            bool verify_xid(const Key &key) const {
                return (start_xid <= key.xid);
            }
        };

        std::mutex _mutex;

        SchemaMap _schema_map;
        LruList _lru;

        uint64_t _schema_max;

        uint64_t _history_max;
        uint64_t _history_size{0};
    };
}
