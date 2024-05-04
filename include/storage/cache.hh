#pragma once

#include <filesystem>
#include <memory>

#include <boost/thread.hpp>

#include <storage/constants.hh>
#include <storage/extent.hh>
#include <storage/field.hh>

namespace springtail {
    /**
     * A centralized cache of data extents wrapped by Page objects.  Pages can be acquired for read,
     * providing shared access, or acquired for write providing exclusive access.  Pages can also be
     * upgraded from read to write.  Dirty pages are associated with a target XID.  Once the page is
     * flushed to disk, it's underlying extents are released back to the cache as clean.
     */
    class StorageCache {
    public:
        /**
         * @brief get_instance() of singleton StorageCache; create if it doesn't exist.
         * @return instance of StorageCache
         */
        static StorageCache *get_instance();

        /**
         * @brief Shutdown the StorageCache singleton.
         */
        static void shutdown();

    private:
        static StorageCache *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /** Constructor.  Uses global properties to configure itself. */
        StorageCache();

        // INTERNAL CLASSES

        // Note: currently we place a hard limit on the number of Page and CacheExtent objects that
        // can be allocated to place a soft limit on memory utilization.  However, what we really
        // need to do is place a hard cap on the total memory utilization of the cache.  There are
        // two possible approaches to doing this:
        //
        // 1) We could create pools of resources for each resource type.  We would then embed the
        //    various metadata pointers (list pointers, hash pointers, etc.) within the objects
        //    themselves so that all memory is accounted for within the objects.  The resource pools
        //    could optionally be backed by a larger shared pool that would perform evictions when
        //    space is required.
        //
        // 2) We could create a polymorphic memory resource that can force cache evictions when
        //    memory is fully utilized.  The problem is that we'd have to somehow integrate into an
        //    actual memory allocator to know when sufficient space has actually been created to
        //    perform the allocation.

        class DataCache;
        class PageCache;

        /** Key for cache entries. */
        using CacheKey = std::pair<std::filesystem::path, uint64_t>;

        /**
         * A wrapper around an extent to hold additional information needed by the cache.
         *
         * XXX we should consider merging this functionality into the Extent rather than making this
         *     a sub-class.  Would simplify data manipulation functions like split().
         */
        class CacheExtent : public Extent {
            friend DataCache;

        public:
            enum State {
                CLEAN = 0,
                DIRTY = 1,
                MUTABLE = 2,
                FLUSHING = 3,
                INVALID = 4
            };

        public:
            CacheExtent(const std::vector<std::shared_ptr<std::vector<char>>> &data,
                        const std::filesystem::path &file,
                        uint64_t extent_id)
                : Extent(data),
                  _file(file),
                  _extent_id(extent_id),
                  _use_count(1),
                  _state(State::CLEAN),
                  _cache_id(0)
            { }

            CacheExtent(const ExtentHeader &header,
                        const std::filesystem::path &file)
                : Extent(header),
                  _file(file),
                  _extent_id(constant::UNKNOWN_EXTENT),
                  _use_count(1),
                  _state(State::DIRTY),
                  _cache_id(0)
            { }

            /**
             * Copy constructor.
             */
            CacheExtent(const CacheExtent &extent)
                : Extent(extent),
                  _file(extent._file),
                  _extent_id(constant::UNKNOWN_EXTENT),
                  _use_count(1),
                  _state(State::MUTABLE),
                  _cache_id(0)
            { }

            /**
             * Move construct from an Extent object; used when splitting an extent.
             */
            CacheExtent(Extent &&other, const CacheExtent &original)
                : Extent(std::move(other)),
                  _file(original._file),
                  _extent_id(constant::UNKNOWN_EXTENT),
                  _use_count(1),
                  _state(State::DIRTY),
                  _cache_id(0)
            { }

            /**
             * Returns they cache key of this extent.
             */
            CacheKey key() const {
                return CacheKey(_file, _extent_id);
            }

            /**
             * Flush the extent to disk.
             */
            uint64_t flush() {
                assert(_extent_id == constant::UNKNOWN_EXTENT);

                // write the data to disk
                auto handle = IOMgr::get_instance()->open(_file, IOMgr::IO_MODE::APPEND, true);
                auto response = this->async_flush(handle);
                _extent_id = response.get()->offset;

                return _extent_id;
            }

            uint64_t cache_id() const {
                return _cache_id;
            }

        private:
            std::filesystem::path _file; ///< The file containing this extent.
            uint64_t _extent_id; ///< The extent_id of this extent.

            // note: these are only used by the DataCache and are protected by it's mutex

            std::atomic<uint16_t> _use_count; ///< The number of users of this extent.
            std::list<std::shared_ptr<CacheExtent>>::iterator _pos; ///< The position of this entry on it's global LRU list.  Invalid if use count is non-zero.

            State _state; ///< The current state of this extent.
            std::shared_ptr<boost::condition_variable> _flush_cv; ///< A condition variable used to notify waiters when the extent is no longer FLUSHING.

            uint64_t _cache_id;
        };
        using CacheExtentPtr = std::shared_ptr<CacheExtent>;

        /**
         * Reference to an extent in the DataCache.  First holds the ID of the extent.  If second is
         * true, then the ID is a cache ID, false then an extent ID.
         */
        using ExtentRef = std::pair<uint64_t, bool>;

        using ExtentLru = std::list<CacheExtentPtr>;


        // XXX we could instead use a std::unordered_set<CacheExtentPtr> for the CleanCache if we
        //     created the right comparison overloads, which would reduce key copies
        using CleanCache = std::unordered_map<CacheKey, CacheExtentPtr, boost::hash<CacheKey>>;

        using DirtyCache = std::unordered_map<uint64_t, CacheExtentPtr>;

        /**
         * A cache of Extent objects.  The DataCache constructs CacheExtent objects.  It maintains a
         * maximum number of in-memory extents, shared between clean and dirty pages.
         *
         * Clean extents are kept in a lookup cache as well as stored on an LRU list for eviction
         * selection.
         *
         * Dirty extents are also kept in a separate LRU list, but not in a lookup cache.  These extents
         * are managed by the higher-level Page objects, and can only be found for use via the Page,
         * but eviction selection is handled at this lower layer.
         */
        class DataCache {
        public:
            DataCache(uint64_t max_size)
                : _size(0),
                  _max_size(max_size),
                  _next_cache_id(0)
            { }

            /**
             * Retrieve an extent from the cache based on a file and extent ID.  Must be released
             * back to the cache after use with put().  May block due to IO.
             * 
             * @param file The file to read the extent from.
             * @param extent_id The extent_id (offset) of the extent.
             * @return A pointer to the extent.
             */
            CacheExtentPtr get(const std::filesystem::path &file, uint64_t extent_id);

            /**
             * Retrieve an extent from the cache based on a cache ID.  Cache IDs are generated when
             * an extent is extract()'d from the read cache for use in the write cache.  Must be
             * released back to the cache after use with put().  May block due to IO.
             * 
             * @param cache_id The unique cache ID of the extent.
             * @return A pointer to the extent.
             */
            CacheExtentPtr get(uint64_t cache_id);

            /**
             * Releases an extent back to the cache, reducing it's use count.
             *
             * @param extent The extent to release back to the cache.
             */
            void put(CacheExtentPtr extent);

            /**
             * Removes an extent from the read cache for use by the write cache.  If the extent is
             * in use by multiple callers, then a copy of the extent is returned to maintain thread
             * safety, so you must always use the returned extent after this call.
             *
             * @param extent The extent to extract from the cache.
             * @return The extent that can be mutated by the write cache.
             */
            CacheExtentPtr extract(const std::filesystem::path &file, uint64_t extent_id);

            /**
             * Re-inserts a write cache extent back into the read cache.  Should be called once a
             * Page holding the Extent has been flush()'d to disk and no longer refereces the Extent
             * object's cache_id.
             *
             * @param extent The extent that should be returned to the clean cache.
             */
            void reinsert(CacheExtentPtr extent);


            void flush(CacheExtentPtr extent);
            void remove_empty(CacheExtentPtr extent);
            std::pair<ExtentRef, ExtentRef> split(CacheExtentPtr extent);

            CacheExtentPtr get_empty(const std::filesystem::path &file,
                                     uint64_t table_id,
                                     uint64_t index_id,
                                     uint64_t xid);

        private:
            CacheExtentPtr _get_clean(const CacheKey &key);

            /**
             * Internal helper to make space for a new extent within the cache by evicting another extent.
             *
             * @param lock The lock currently holding the cache mutex.
             */
            void _make_space();

            /**
             * Internal helper to release an extent back to the cache.  Reduces use count and places
             * into the LRU queue if no longer in use.
             *
             * @param extent The extent to release.
             */
            void _release(CacheExtentPtr extent);

            void _flush(CacheExtentPtr extent);

            CacheExtentPtr _read_extent(const CacheKey &key, std::function<void(CacheExtentPtr)> callback);

            void _gen_cache_id(CacheExtentPtr extent);

        private:
            boost::mutex _mutex; ///< Mutex on the cache object to maintain thread-safety.

            CleanCache _clean_cache; ///< The lookup cache of clean extents.
            ExtentLru _clean_lru; ///< An LRU of the clean extents.

            DirtyCache _dirty_cache; ///< The lookup cache of dirty extents.
            ExtentLru _dirty_lru; ///< An LRU of the dirty extents.

            /** Map of condition variables used to block multiple callers reading the same extent from disk concurrently. */
            std::map<CacheKey, std::shared_ptr<boost::condition_variable>> _io_map;

            /** Map from cache ID to the most recent on-disk location of the extent. */
            std::map<uint64_t, CacheKey> _cache_id_map;

            uint64_t _size; ///< The current size of the cache.
            uint64_t _max_size; ///< The maximum allowed size of the cache.
            uint64_t _next_cache_id; ///< The next cache ID to assign.
        };

    public:
        // PUBLIC CLASSES

        /**
         * A wrapper around a single logical extent, which may contain a single extent or set of
         * extents.  Page objects can grow arbitrarily large since they need to support applying an
         * unbounded number of mutations to a single extent.
         *
         * When a page grows too large, it's underlying extents are written to disk, and their
         * in-memory extent pointers can be replaced with extent IDs.  These can be retrieved later
         * on-demand if needed.
         *
         * Note: This approach may still result in exceeding memory limits if the Page grows too
         * large.  For example, assuming an average extent size of 16KB, a 1GB index of 8-byte
         * extent IDs would only allow access to 256GB of data.  To exceed this limitation we should
         * eventually utilize some kind of temporary BTree to allow the index of the extents to also
         * be paged to disk.
         */
        class Page {
            friend PageCache;

        public:
            Page(const std::filesystem::path &file, uint64_t extent_id,
                 uint64_t table_id, uint64_t index_id,
                 uint64_t start_xid, uint64_t end_xid, const std::vector<uint64_t> &offsets);

            Page(const std::filesystem::path &file,
                 uint64_t table_id, uint64_t index_id, uint64_t xid);

            /**
             * Writes all of the dirty in-memory extents to disk and returns the full set of extent
             * IDs for the page.
             *
             * @param flush_xid The XID at which this page is being written out.  The page is
             *                  considered valid from this XID forward.
             * @return The ordered list of extent IDs that represent the data of the Page.
             */
            std::vector<uint64_t> flush(uint64_t flush_xid, ExtentType type, uint64_t table_id, uint64_t index_id);

            /**
             * Returns the cache key of this page.
             */
            CacheKey key() const {
                return CacheKey(_file, _extent_id);
            }

            /**
             * Returns the starting XID of this page.
             */
            uint64_t xid() const {
                return _start_xid;
            }

            /**
             * Check if the requested XID is within the bounds of the valid XIDs for this page.
             */
            bool check_xid_valid(uint64_t xid) const {
                return (_start_xid <= xid && xid <= _end_xid);
            }

            /**
             * Registers an eviction callback.
             */
            void register_evict(std::function<bool(std::shared_ptr<Page>)> callback) {
                boost::unique_lock lock(_mutex);
                _evict_callback = callback;
            }

        private:
            /**
             * RAII container for a CacheExtent to ensure it is put back into the read cache after
             * use.
             */
            class SafeExtent {
            public:
                SafeExtent()
                    : _extent(nullptr)
                { }

                SafeExtent(const std::filesystem::path &file,
                           const ExtentRef &ref)
                    : _extent(nullptr)
                {
                    if (ref.second) {
                        _extent = StorageCache::get_instance()->_data_cache->get(ref.first);
                    } else {
                        _extent = StorageCache::get_instance()->_data_cache->get(file, ref.first);
                    }
                }

                SafeExtent(const std::filesystem::path &file,
                           ExtentRef &ref,
                           bool is_mutable = false)
                    : _extent(nullptr)
                {
                    if (ref.second) {
                        _extent = StorageCache::get_instance()->_data_cache->get(ref.first);
                    } else if (!is_mutable) {
                        _extent = StorageCache::get_instance()->_data_cache->get(file, ref.first);
                    } else {
                        _extent = StorageCache::get_instance()->_data_cache->extract(file, ref.first);

                        // XXX
                        ref.first = _extent->cache_id();
                        ref.second = true;
                    }
                }

                // no copying
                SafeExtent(const SafeExtent &) = delete;
                SafeExtent &operator=(const SafeExtent &) = delete;

                // move handling
                SafeExtent(SafeExtent &&other) {
                    _extent = other._extent;
                    other._extent = nullptr;
                }

                SafeExtent &operator=(SafeExtent &&other) {
                    if (_extent) {
                        StorageCache::get_instance()->_data_cache->put(_extent);
                    }

                    _extent = other._extent;
                    other._extent = nullptr;

                    return *this;
                }

                ~SafeExtent()
                {
                    if (_extent) {
                        StorageCache::get_instance()->_data_cache->put(_extent);
                    }
                }

                CacheExtentPtr operator*() const {
                    return _extent;
                }

                CacheExtentPtr *operator->() {
                    return &_extent;
                }

            private:
                CacheExtentPtr _extent;
            };

        public:
            // ACCESS
            // note: access is to a bi-directional iterator that dereferences to rows

            /**
             * A class to access the rows of the page via a bidirectional iterator interface.
             */
            class Iterator {
                friend Page;

            public:
                using iterator_category = std::bidirectional_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                reference operator*() const {
                    return *_row;
                }
                pointer operator->() {
                    return &(*_row);
                }

                // Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }
                Iterator &operator++() {
                    // move to the next row
                    ++_row;

                    // check if this is the end of the extent
                    if (_row != (*_extent)->end()) {
                        return *this;
                    }

                    // move to the next extent
                    ++_extent_i;
                    if (_extent_i == _page->_extents.end()) {
                        // at the end, return
                        _extent = SafeExtent();
                        return *this;
                    }

                    // retrieve the extent
                    _extent = SafeExtent(_page->_file, *_extent_i);

                    // start at the first row
                    _row = (*_extent)->begin();

                    return *this;
                }

                // Iterator operator--(int) { Iterator tmp = *this; --(*this); return tmp; }
                Iterator &operator--() {
                    // try to move to the prev row
                    if (_row != (*_extent)->begin()) {
                        --_row;
                        return *this;
                    }

                    // try to move to the previous extent
                    // note: should never call if we are at begin()
                    assert(_extent_i != _page->_extents.begin());
                    --_extent_i;

                    // retrieve the extent
                    _extent = SafeExtent(_page->_file, *_extent_i);

                    // start at the last row
                    _row = (*_extent)->last();

                    return *this;
                }

                bool operator==(const Iterator &rhs) {
                    if (_page == rhs._page && _extent_i == rhs._extent_i) {
                        return (_extent_i == _page->_extents.end() || _row == rhs._row);
                    }
                    return false;
                }

                bool operator!=(const Iterator &rhs) { return !(*this == rhs); }

            private:
                Iterator(Page *page,
                         std::vector<ExtentRef>::iterator extent_i)
                    : _page(page),
                      _extent_i(extent_i)
                {
                    // if at the end, do nothing
                    if (_extent_i == _page->_extents.end()) {
                        return;
                    }

                    // get the extent, potentially from the read cache
                    _extent = SafeExtent(_page->_file, *_extent_i);

                    // get the first row in the extent
                    _row = (*_extent)->begin();
                }

                Iterator(Page *page,
                         std::vector<ExtentRef>::iterator extent_i,
                         SafeExtent &&extent,
                         Extent::Iterator row_i)
                    : _page(page),
                      _extent_i(extent_i),
                      _extent(std::move(extent)),
                      _row(row_i)
                { }

            private:
                Page *_page;
                std::vector<ExtentRef>::iterator _extent_i;
                SafeExtent _extent;
                Extent::Iterator _row;
            };

            /**
             * Returns an iterator to the first row of the page.
             */
            Iterator begin() {
                boost::shared_lock lock(_mutex);
                return Iterator(this, _extents.begin());
            }

            /**
             * Returns an iterator past the last row of the page.
             */
            Iterator end() {
                boost::shared_lock lock(_mutex);
                return Iterator(this, _extents.end());
            }

            /**
             * Returns the first row of the page with columns >= the matching columns of the provided tuple.
             * @param tuple A tuple holding data that matches the sort columns of the extent.
             */
            Iterator lower_bound(TuplePtr tuple);

        public:
            // MUTATIONS
            // note: all mutations will invalidate an Iterator on the page

            void insert(TuplePtr tuple);
            void append(TuplePtr tuple);
            void upsert(TuplePtr tuple);
            void update(TuplePtr tuple);
            void remove(TuplePtr key);

        private:
            // HELPER FUNCTIONS
            void _check_split(std::vector<ExtentRef>::iterator pos, CacheExtentPtr extent);

        private:
            /** A count of the number of users of this page. */
            std::atomic<uint16_t> _use_count;

            /** A mutex to protect access. */
            boost::shared_mutex _mutex;

            /** The extents that make up this page. */
            std::vector<ExtentRef> _extents;

            /** A flag indicating if the page is clean or dirty. */
            bool _is_dirty;

            /** The file that this page is associated with. */
            std::filesystem::path _file;

            /** The original extent_id of this page. */
            uint64_t _extent_id;

            /** The starting XID that this page is known valid from. */
            uint64_t _start_xid;

            /** The ending XID up through which this page is known valid. */
            uint64_t _end_xid;

            /** The position on the LRU list; only valid when _use_count is zero. */
            std::list<std::shared_ptr<Page>>::iterator _lru_pos;

            /** The schema of the underlying extent data. */
            ExtentSchemaPtr _schema;

            /** Callback to be issued if the page is evicted. */
            std::function<bool(std::shared_ptr<Page>)> _evict_callback;

            // table and index IDs?
            uint64_t _table_id;
            uint64_t _index_id;
        };
        using PagePtr = std::shared_ptr<Page>;


    private:
        /**
         * LRU cache of Page objects.  Maintains a maximum number of extent references across all of the pages.
         */
        class PageCache {
        public:
            PageCache(uint64_t max_size)
                : _max_size(max_size),
                  _size(0)
            { }

            PagePtr get(const std::filesystem::path &file,
                        uint64_t extent_id, uint64_t table_id, uint64_t index_id, uint64_t xid);

            PagePtr get_empty(const std::filesystem::path &file,
                              uint64_t table_id, uint64_t index_id, uint64_t xid);

            void update_size(PagePtr page);

            void put(PagePtr page);

        private:
            void _put(PagePtr page);

            PagePtr _try_get(const std::filesystem::path &file, uint64_t extent_id, uint64_t xid);

            void _try_evict(PagePtr page);

            PagePtr _create(const std::filesystem::path &file, uint64_t extent_id,
                            uint64_t table_id, uint64_t index_id,
                            uint64_t xid, const std::vector<uint64_t> &offsets);

            void _make_space(uint32_t size);

        private:
            using XidMap = std::map<uint64_t, PagePtr>;
            using CacheMap = std::unordered_map<CacheKey, XidMap, boost::hash<CacheKey>>;

            boost::mutex _mutex;

            CacheMap _cache;
            std::list<PagePtr> _lru;

            uint64_t _max_size;
            uint64_t _size;
        };

    public:
        // PUBLIC INTERFACES

        /**
         * Acquire a Page object from the cache.  All data access and manipulation is done via Page
         * objects, which transparently contain one or more Extent objects.  To get an empty page
         * that will eventually get appended to a file but is not based on an existing extent, pass
         * UNKNOWN_EXTENT as the extent_id.
         *
         * Note that the returned Page is pinned until is it put() back into the cache.
         *
         * @param file The file to read the page from.
         * @param extent_id The extent_id that this page represents.
         * @param access_xid The XID at which this extent_id was found.  This is an XID at which the
         *                   on-disk extent is known valid.
         * @param target_xid The XID at which we actually want to operate at.  In the case of a
         *                   roll-forward data extent, this is the XID up-to-which in-flight
         *                   mutations must be applied.  If this is the same as the access_xid (or
         *                   LATEST_XID), then no changes are applied.
         * @param table_id The ID of the table being modified.
         * @param index_id If INDEX_DATA, then specifies that the requested extent is raw data from
         *                 the table with this ID and that the cache should do a roll-forward of the
         *                 extent to the target_xid.
         *                 Otherwise, then the caller is assumed to be performing mutations to bring
         *                 the page forward to the target_xid, meaning that the cache will return
         *                 the same in-flight dirty page to other callers.
         * @return The retrieved Page object.
         */
        PagePtr get(const std::filesystem::path &file,
                    uint64_t extent_id,
                    uint64_t table_id, 
                    uint64_t access_xid,
                    uint64_t target_xid = constant::LATEST_XID,
                    uint64_t index_id = constant::INDEX_DATA,
                    bool do_rollforward = false);

        // // get a page at the same xid at which the extent_id was identified
        // PagePtr get(const std::filesystem::path &file, uint64_t extent_id, uint64_t access_xid);

        // // get a page starting with the extent found at access_xid that will be mutated to target_xid
        // PagePtr get(const std::filesystem::path &file, uint64_t extent_id,
        //             uint64_t access_xid, uint64_t target_xid);

        // // get a page starting with the extent found at access_xid that the cache will roll-forward to the requested target_xid
        // PagePtr get(const std::filesystem::path &file, uint64_t extent_id,
        //             uint64_t access_xid, uint64_t target_xid, uint64_t table_id);



        /**
         * Release a Page object back to the cache.
         *
         * @param page The page to release.
         */
        void put(PagePtr page);

    private:
        // INTERNAL MEMBER VARIABLES

        /**
         * Mutex to protect access to the internal variables.
         */
        boost::mutex _mutex;

        /**
         * The lookup map for read-only CacheExtent objects.
         */
        std::shared_ptr<DataCache> _data_cache;

        /**
         * The lookup map for Page objects.
         */
        std::shared_ptr<PageCache> _page_cache;
    };
}
