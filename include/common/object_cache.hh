#pragma once

#include <unordered_map>
#include <functional>
#include <list>
#include <iostream>
#include <boost/container_hash/hash.hpp>
#include <common/logging.hh>

namespace springtail {
    /**
     * @brief LRU object cache
     * @details The template interface for a cache of object pointers that ensures size
     *          limit based on provided object sizes.
     */
    template <class IdType, class EntryType>
    class ObjectCache
    {
    public:
        virtual ~ObjectCache() { }

        /**
         * @brief Insert method
         * @param id     ID (key)
         * @param entry  Value
         * @param size   Size of value (default of 1 to count entries)
         */
        virtual void insert(const IdType &id, std::shared_ptr<EntryType> entry, uint64_t size=1) = 0;

        /**
         * @brief Lookup by ID (key)
         * @param id ID (key)
         * @return std::shared_ptr<EntryType> value result or nullptr if not found
         */
        virtual std::shared_ptr<EntryType> get(const IdType &id) = 0;

        /**
         * @brief Evict an entry from the cache based on ID (key)
         * @param id ID (key)
         * @return std::shared_ptr<EntryType> value of the evicted entry or nullptr if not found
         */
        virtual std::shared_ptr<EntryType> evict(const IdType &id) = 0;

        /**
         * @brief Update the size associated with an entry.  Force eviction if that causes the cache
         *        to exceed it's limits.
         * @param id ID (key)
         */
        virtual void update_size(const IdType &id, uint64_t size) = 0;
    };


    /**
     * @brief A least-recently-used policy object cache.
     */
    template <class IdType, class EntryType, class Hash=boost::hash<IdType>>
    class LruObjectCache : public ObjectCache<IdType, EntryType>
    {
    private:
        /** Cache entry typedef, tuple of key, value and size */
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
         * @return true if an evictable entry is found
         * @return false if no evictable entry is found
         */
        bool
        _evict_next() {
            // remove the item from the cache
            if (!_callback) {
                // no callback function, easy
                CacheEntry &entry = _cache.back();
                _remove_entry(entry);
                _cache.pop_back();
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
                    _remove_entry(entry);
                    _cache.erase(std::next(current).base());
                    return true;
                }
            }

            return false; // nothing evictable
        }

        /**
         * @brief Check the size of the cache and evict entries until it is below the max size.
         * @param size An optional entry size to add to the current cache size.  Allows the caller
         *             to make space for new entry of the given size.
         */
        void
        _check_and_evict(uint64_t size = 0) {
            while (_cache_size + size > _cache_max) {
                if (!_evict_next()) {
                    SPDLOG_WARN("Object cache eviction, nothing evictable");
                    break; // nothing evictable
                }
            }
        }

    public:
        /**
         * @brief Construct a new Lru Object Cache object
         * @param max max size of cache, size is based on entry size at insert
         * @param callback optional callback for eviction, return true/false if eviction ok
         */
        LruObjectCache(uint64_t max, std::function<bool(std::shared_ptr<EntryType>)> callback)
            : _cache_size(0), _cache_max(max), _callback(callback)
        { }

        /**
         * @brief Construct a new Lru Object Cache object
         * @param max max size of cache, size is based on entry size at insert
         */
        LruObjectCache(uint64_t max) : _cache_size(0), _cache_max(max)
        { }

        /**
         * @brief Destroy the Lru Object Cache object; evict all entries
         */
        ~LruObjectCache()
        {
            // evict all of the entries before destruction
            while (!_cache.empty()) {
                _evict_next();
            }
        }

        /**
         * @brief Resize the cache, doesn't evict if too many entries exist (if cache oversize)
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
         * @param id     key for entry
         * @param entry  value for entry
         * @param size   optional size for each entry; a size=1 will use # entries for eviction
         */
        void
        insert(const IdType &id, std::shared_ptr<EntryType> entry, uint64_t size=1)
        {
            // if we need more space, evict entries until we have enough space
            _check_and_evict(size);

            // push onto the front of the LRU queue
            _cache.push_front({id, entry, size});

            // note: if this replaces an existing entry, we aren't evicting the replaced entry; it
            //       will only get evicted via eventual LRU eviction
            _lookup.insert_or_assign(id, _cache.begin());
            _cache_size += size;
        }

        /**
         * @brief Get entry from cache based on key ID
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

        /**
         * @brief Evict an entry from the cache based on ID (key).  Throws an exception if the entry
         *        cannot be evicted due to the callback.
         * @param id ID (key)
         * @return std::shared_ptr<EntryType> value of the evicted entry or nullptr if not found
         */
        std::shared_ptr<EntryType>
        evict(const IdType &id)
        {
            // find the entry if it exists
            auto &&i = _lookup.find(id);
            if (i == _lookup.end()) {
                return nullptr;
            }

            // retrieve the value from the lookup entry
            CacheEntry &entry = *(i->second);
            auto value = std::get<1>(entry);

            // check the callback if one exists
            if (_callback && _callback(value) == false) {
                return nullptr; // if we weren't able to evict, return a nullptr
            }

            // remove the entry from the cache
            _cache_size -= std::get<2>(entry);
            _cache.erase(i->second);
            _lookup.erase(i);

            return value;
        }

        /**
         * @brief Update the size associated with an entry.  Force eviction if that causes the cache
         *        to exceed it's limits.
         * @param id ID (key)
         */
        void
        update_size(const IdType &id, uint64_t size)
        {
            // find the entry
            auto &&i = _lookup.find(id);
            if (i == _lookup.end()) {
                SPDLOG_WARN("Tried to update the size of a non-existant cache entry");
                return;
            }

            // resize the entry
            uint64_t old_size = std::get<2>(*(i->second));
            _cache_size = _cache_size - old_size + size;
            std::get<2>(*(i->second)) = size;

            // evict until the cache is the correct size
            _check_and_evict();
        }
    };
}
