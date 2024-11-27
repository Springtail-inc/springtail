namespace springtail::sys_tbl_mgr {
    struct MetadataKey {
        uint64_t db;
        uint64_t tid;
        uint64_t xid;

        bool verify(const MetdataKey &key) {
            return (db == key.db && tid == key.tid);
        }

        bool operator<(const MetadataKey &rhs) {
            return (db < rhs.db ||
                    (db == rhs.db &&
                     (tid < rhs.tid ||
                      (tid == rhs.tid &&
                       xid < rhs.xid))));
        }
    };

    struct MetadataValue {
        uint64_t start_xid;
        std::shared_ptr<TableMetadata> metadata;

        bool verify(const MetadataKey &key) {
            return (start_xid <= key.xid);
        }
    };

    struct SchemaKey {
        uint64_t db;
        uint64_t tid;
        XidLsn access_xid;
        XidLsn target_xid;

        bool verify(const SchemaKey &key) {
            return (db == key.db && tid == key.tid);
        }

        bool operator<(const SchemaKey &rhs) {
            return (db < rhs.db ||
                    (db == rhs.db &&
                     (tid < rhs.tid ||
                      (tid == rhs.tid &&
                       (access_xid < rhs.access_xid ||
                        (access_xid == rhs.access_xid &&
                         target_xid < rhs.target_xid))))));
        }
    };

    template <class TypeT>
    struct SchemaValue {
        XidLsn start_access_xid;
        XidLsn start_target_xid;
        SchemaPtr schema;

        bool verify(const SchemaKey &key) {
            return (start_access_xid <= key.access_xid &&
                    start_target_xid <= key.target_xid);
        }
    };

    template <class KeyT, class ValueT>
    class Cache {
    public:
        Cache(int max)
            : _max_size(max)
        { }

        SchemaValue
        get(const KeyT &key,
            std::function<std::shared_ptr<ValueT>()> populate)
        {
            std::unique_lock lock(_mutex);

            // see if there's any potentially matching entry
            auto entry_i = _lookup.lower_bound(key);
            if (entry_i == _lookup.end() ||
                !entry->first.verify(key) ||
                !entry->second.value.verify(key)) {

                // make space for the entry
                _make_space();

                // create a dummy entry for the entry
                auto &&result = _lookup.try_emplace(key, {});
                if (!result.second) {
                    throw Error();
                }
                entry_i = result.first;
                lock.unlock();

                // call the populate function for the cache
                std::shared_ptr<ValueT> value = populate(key);

                // update the new entry's value
                lock.lock();
                entry_i->second.fetching = false;
                entry_i->second.value = value;
                entry_i->second.cond.notify_all();

            } else {
                if (entry_i->second.fetching) {
                    // wait until the fetch is complete
                    entry_i->second.cond.wait(lock, [&entry_i]() {
                        return !entry_i->second.fetching;
                    });
                } else {
                    // remove the entry from it's current position in the LRU list
                    _lru.erase(entry_i->second.lru_i);
                }
            }

            // place the entry on the back of the LRU list
            entry_i->second.lru_i = _lru.insert(_lru.end(), entry_i);

            // return the value
            return entry_i->second.value;
        }

        /**
         * Checks for an entry that contains the key and re-indexes it with the key as the ending
         * position for the entry.  Used to mark the "current" entry for a schema or root as ending
         * at the given key.
         */
        void
        reinsert(const KeyT &key)
        {
            std::unique_lock lock(_mutex);

            // check if the key exists in the cache
            auto entry_i = _lookup.lower_bound(key);
            if (entry_i == _lookup.end() ||
                !entry->first.verify(key) ||
                !entry->second.value.verify(key)) {
                return;
            }

            // insert the value using the new key
            auto &&result = _lookup.try_emplace(key, entry_i->second);
            if (!result.second) {
                throw Error();
            }

            // update the LRU list pointer to the new map entry
            *(entry_i->lru_i) = result.first;

            // remove the old entry from the map
            _lookup.erase(entry_i);
        }

    private:
        void
        _make_space()
        {
            if (_size < _max_size) {
                ++_size;
            } else {
                auto lru_i = _lru.begin();
                _lookup.erase(*lru_i);
                _lru.erase(lru_i);
            }
        }

        struct Entry;
        using LookupMap = std::map<KeyT, Entry>;
        using LruList = std::list<LookupMap::iterator>;

        struct Entry {
            std::shared_ptr<ValueT> value;
            LruList::iterator lru_i;
            bool fetching = true;
        };

        LookupMap _lookup;
        LruList _lru;
        std::mutex _mutex;
    };
}
