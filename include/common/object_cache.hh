#pragma once

#include <unordered_map>
#include <functional>
#include <list>
#include <iostream>
#include <boost/container_hash/hash.hpp>

namespace springtail {
    /** The template interface for a cache of object pointers that ensures size limit based on provided object sizes. */
    template <class IdType, class EntryType>
    class ObjectCache
    {
    public:
        virtual ~ObjectCache() { }

        virtual void insert(const IdType &id, std::shared_ptr<EntryType> entry, uint64_t size) = 0;

        virtual std::shared_ptr<EntryType> get(const IdType &id) = 0;
    };


    /** A least-recently-used policy object cache. */
    template <class IdType, class EntryType, class Hash=boost::hash<IdType>>
    class LruObjectCache : public ObjectCache<IdType, EntryType>
    {
    private:
        typedef std::tuple<IdType, std::shared_ptr<EntryType>, uint64_t> CacheEntry;

        std::unordered_map<IdType, typename std::list<CacheEntry>::iterator, Hash> _lookup; ///< A map that holds the entries.
        std::list<CacheEntry> _cache; ///< An ordered list of objects for priority removal

        uint64_t _cache_size; ///< The current size of the cache.
        uint64_t _cache_max; ///< The maximum allowed size of the cache.

        std::function<bool(std::shared_ptr<EntryType>)> _callback; ///< callback function, optional

        /**
         * @brief Helper to remove entry
         * @param entry Entry to remove
         */
        inline void
        _remove_entry(CacheEntry &entry)
        {
            // remove from hashmap
            _lookup.erase(std::get<0>(entry));

            // update the size of the cache
            _cache_size -= std::get<2>(entry);
        }


    protected:
        /**
         * @brief Evict least used item (back of _cache list); if callback is set, check callback first
         * to make sure it is evictable (callback returns true)
         */
        bool
        _evict_next() {
            // remove the item from the cache
            if (!_callback) {
                // no callback function, easy
                CacheEntry &entry = _cache.back();
                _cache.pop_back();
                _remove_entry(entry);
                return true;
            }

            // with callback, we need to check if entry is evictable
            // reverse iterate through until we find an evictable entry
            // this is O(n) which isn't great since the lock is held           
            for (auto current = _cache.rbegin(); current != _cache.rend(); current++) {
                CacheEntry &entry = *current;
                if (_callback(std::get<1>(entry)) == true) {
                    // callback returned true so we can evict item
                    // reverse iterator is pointing behind of where we want it
                    _cache.erase(std::next(current).base()); 
                    _remove_entry(entry);
                    return true;
                }
            }

            return false; // nothing evictable
        }

    public:
        /**
         * @brief Construct LRU cache
         * @param max max size of cache, size is based on entry size at insert
         * @param callback optional callback for eviction, return true/false if eviction ok
         */
        LruObjectCache(uint64_t max, std::function<bool(std::shared_ptr<EntryType>)> callback)
            : _cache_max(max), _callback(callback)
        { }

        LruObjectCache(uint64_t max) : _cache_max(max)
        { }

        ~LruObjectCache()
        {
            // evict all of the entries before destruction
            while (!_cache.empty()) {
                _evict_next();
            }
        }

        /**
         * @brief Resize the cache, doesn't evict if too many entries.  
         *        Eviction will happen on next insert.
         * @param size new max size of cache
         */
        void
        resize(int size)
        {
            _cache_max = size;
        }

        /**
         * @brief Insert entry
         *
         * @param id     key for entry
         * @param entry  value for entry
         * @param size   optional size for each entry; a size=1 will use # entries for eviction
         */
        void
        insert(const IdType &id, std::shared_ptr<EntryType> entry, uint64_t size=1)
        {
            // if we need more space, evict entries until we have enough space
            while (_cache_size + size > _cache_max) {
                if (!_evict_next()) {
                    break; // nothing evictable
                }
            }

            // push onto the front of the LRU queue
            _cache.push_front({id, entry, size});
            _lookup.insert_or_assign(id, _cache.begin());
            _cache_size += size;
        }

        /**
         * @brief Get entry from cache based on key ID
         *
         * @param id key to lookup entry
         * @return entry (value)
         */
        std::shared_ptr<EntryType>
        get(const IdType &id)
        {
            // find the entry if it exists
            auto &&i = _lookup.find(id);
            if (i == _lookup.end()) {
                return nullptr;
            }

            // move the entry to the end of the LRU cache
            _cache.splice(_cache.end(), _cache, i->second);

            // return the data
            return std::get<1>(*(i->second));
        }
    };
}
