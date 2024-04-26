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
        class Page;
        using PagePtr = std::shared_ptr<Page>;

    public:
        /**
         * @brief getInstance() of singleton StorageCache; create if it doesn't exist.
         * @return instance of StorageCache
         */
        static StorageCache *get_instance();

        /**
         * @brief Shutdown the StorageCache singleton.
         */
        static void shutdown();

    public:
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
         * @param table_id If non-zero, then specifies that the requested extent is raw data from
         *                 the table with this ID and that the cache should do a roll-forward of the
         *                 extent to the target_xid.
         *                 If zero, then the caller is assumed to be performing mutations to bring
         *                 the page forward to the target_xid, meaning that the cache will return
         *                 the same in-flight dirty page to other callers.
         * @return The retrieved Page object.
         */
        PagePtr get(const std::filesystem::path &file, uint64_t extent_id, uint64_t access_xid,
                    uint64_t target_xid = constant::LATEST_XID, uint64_t table_id = 0);

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
        static StorageCache *_instance; ///< static instance (singleton)
        static boost::mutex _instance_mutex; ///< protects lookup/creation of singleton _instance

        /** Constructor.  Uses global properties to configure itself. */
        StorageCache();

        /**
         * Try to get a page from the cache if it exists.  Return nullptr if it doesn't.
         */
        PagePtr _try_get_page(const std::filesystem::path &file, uint64_t extent_id, uint64_t xid);

        /**
         * Get a page from the cache, if it doesn't exist, create it.
         */
        PagePtr _get_page(const std::filesystem::path &file, uint64_t extent_id, uint64_t xid);


        /** Key for cache entries. */
        using CacheKey = std::pair<std::filesystem::path, uint64_t>;

        /**
         * A wrapper around an extent to hold additional information needed by the cache.
         */
        class CacheExtent : public Extent {
        public:
            CacheExtent(const std::vector<std::shared_ptr<std::vector<char>>> &data,
                        const std::filesystem::path &file,
                        uint64_t extent_id)
                : Extent(data),
                  _file(file),
                  _extent_id(extent_id),
                  _use_count(1)
            { }

            /**
             * Copy constructor.
             */
            CacheExtent(const CacheExtent &extent)
                : Extent(extent),
                  _file(extent._file),
                  _extent_id(constant::UNKNOWN_EXTENT),
                  _use_count(1)
            { }

            /**
             * Returns they cache key of this extent.
             */
            CacheKey key() {
                return CacheKey(_file, _extent_id);
            }

            /**
             * Increase the use count by one.
             */
            void increment_use() {
                ++_use_count;
            }

            /**
             * Decrease the use count by one.
             */
            void decrement_use() {
                --_use_count;
            }

            /**
             * Get the current use count.
             */
            uint16_t use_count() const {
                return _use_count;
            }

            /**
             * Flush the extent to disk.
             */
            uint64_t flush() {
                assert(_extent_id == constant::UNKNOWN_EXTENT);

                auto handle = IOMgr::get_instance()->open(_file, IOMgr::IO_MODE::APPEND, true);
                auto response = this->async_flush(handle);
                _extent_id = response.get()->offset;

                return _extent_id;
            }

        private:
            std::filesystem::path _file; ///< The file containing this extent.
            uint64_t _extent_id; ///< The extent_id of this extent.
            std::atomic<uint16_t> _use_count; ///< The number of users of this extent.
        };
        using CacheExtentPtr = std::shared_ptr<CacheExtent>;

        /** Union of a dirty in-memory Extent or a clean on-disk extent_id. */
        using ExtentVar = std::variant<CacheExtentPtr, uint64_t>;

        /**
         * A cache of clean extents.  Can be re-sized as needed based on the size of the dirty page
         * cache.  Backs the clean extents of the Page objects.
         */
        class ReadCache {
        public:
            ReadCache() { }

            /**
             * Retrieve an extent from the cache.  Must be released back to the cache after use with
             * put().  May block due to IO.
             * 
             * @param file The file to read the extent from.
             * @param extent_id The extent_id (offset) of the extent.
             * @return A pointer to the extent.
             */
            CacheExtentPtr get(const std::filesystem::path &file, uint64_t extent_id);

            /**
             * Releases an extent back to the cache, reducing it's use count.
             *
             * @param extent The extent to release.
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
            CacheExtentPtr extract(CacheExtentPtr extent);

            /**
             * Re-inserts a write cache extent back into the read cache.  Should be called once an
             * extent retrieved via extract() has been flush()'d to disk.
             *
             * @param extent The flushed extent that should be returned to the read cache.
             */
            void reinsert(CacheExtentPtr extent);

            /**
             * Borrows space from the read cache for use by the write cache by reducing the max size
             * of the read cache.  Called when the write cache needs additional space due to
             * insert/update mutations.
             *
             * @param count The number of bytes being borrowed.
             * @return The number of bytes made available.
             */
            uint32_t borrow_space(uint32_t count);

        private:
            /**
             * Internal helper to make space within the read cache by evicting clean extents.  May
             * create more space than is requested (or less if insufficient extents are available to
             * free).
             *
             * @param count The target number of bytes to free.
             */
            void _make_space(uint32_t count);

            /**
             * Internal helper to release an extent back to the cache.  Reduces use count and places
             * into the LRU queue if no longer in use.
             *
             * @param extent The extent to release.
             */
            void _release(CacheExtentPtr extent);

        private:
            // XXX we could instead use a std::unordered_map<CacheExtentPtr, CacheList::iterator>
            //     for the CacheMap if we created the right comparison overloads, which would reduce
            //     key copies

            using ReadCacheList = std::list<CacheExtentPtr>;
            using ReadCacheValue = std::pair<CacheExtentPtr, ReadCacheList::iterator>;
            using ReadCacheMap = std::unordered_map<CacheKey, ReadCacheValue, boost::hash<CacheKey>>;

            boost::mutex _mutex; ///< Mutex on the cache object to maintain thread-safety.

            ReadCacheMap _cache; ///< The lookup cache of extents.
            ReadCacheList _lru; ///< The LRU queue of currently unused extents.

            /** Map of condition variables used to block multiple callers reading the same extent from disk concurrently. */
            std::map<CacheKey, boost::condition_variable> _io_map;

            uint64_t _size; ///< The current size of the cache.
            uint64_t _max_size; ///< The maximum allowed size of the cache.
        };

        using WriteCacheList = std::list<PagePtr>;
        using WriteCacheValue = std::pair<PagePtr, WriteCacheList::iterator>;
        using WriteCacheXidMap = std::map<uint64_t, WriteCacheValue>;
        using WriteCacheMap = std::unordered_map<CacheKey, WriteCacheXidMap, boost::hash<CacheKey>>;

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
        public:
            Page(const std::filesystem::path &file,
                 uint64_t extent_id,
                 uint64_t start_xid,
                 uint64_t end_xid);

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
             * Requests the page to reduce it's dirty in-memory footprint by at least 'count' bytes.
             * Returns the number of bytes it was actually able to reduce by.
             */
            uint64_t reduce_size(uint64_t count);

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
            bool check_xid_valid(uint64_t xid) {
                return (_start_xid <= xid && xid <= _end_xid);
            }

        private:
            /**
             * RAII container for a CacheExtent to ensure it is put back into the read cache after
             * use.
             */
            class SafeExtent {
            public:
                SafeExtent()
                    : _extent(nullptr),
                      _release(false)
                { }

                SafeExtent(const std::filesystem::path &file,
                           const ExtentVar &var)
                {
                    if (std::holds_alternative<CacheExtentPtr>(var)) {
                        _extent = std::get<CacheExtentPtr>(var);
                        _release = false;
                    } else {
                        // read-only access
                        _extent = StorageCache::get_instance()->_read_cache.get(file, std::get<uint64_t>(var));
                        _release = true;
                    }
                }

                SafeExtent(const std::filesystem::path &file,
                           ExtentVar &var,
                           bool is_mutable = false)
                {
                    if (std::holds_alternative<CacheExtentPtr>(var)) {
                        _extent = std::get<CacheExtentPtr>(var);
                        _release = false;
                    } else if (is_mutable) {
                        // mutable access
                        _extent = StorageCache::get_instance()->_read_cache.get(file, std::get<uint64_t>(var));
                        _extent = StorageCache::get_instance()->_read_cache.extract(_extent);
                        _release = false;

                        // store the extent for future access within the extent variant
                        var = _extent;
                    } else {
                        // read-only access
                        _extent = StorageCache::get_instance()->_read_cache.get(file, std::get<uint64_t>(var));
                        _release = true;
                    }
                }

                // no copying
                SafeExtent(const SafeExtent &) = delete;
                SafeExtent &operator=(const SafeExtent &) = delete;

                // move handling
                SafeExtent(SafeExtent &&other) {
                    _extent = other._extent;
                    _release = other._release;

                    other._extent = nullptr;
                    other._release = false;
                }

                SafeExtent &operator=(SafeExtent &&other) {
                    _try_release();

                    _extent = other._extent;
                    _release = other._release;

                    other._extent = nullptr;
                    other._release = false;

                    return *this;
                }

                ~SafeExtent()
                {
                    _try_release();
                }

                CacheExtentPtr operator*() const {
                    return _extent;
                }

                CacheExtentPtr *operator->() {
                    return &_extent;
                }

            private:
                void _try_release() {
                    if (_release) {
                        StorageCache::get_instance()->_read_cache.put(_extent);
                    }
                }

                CacheExtentPtr _extent;
                bool _release;
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

            private:
                Iterator(Page *page,
                         std::vector<ExtentVar>::iterator extent_i)
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
                         std::vector<ExtentVar>::iterator extent_i,
                         SafeExtent &&extent,
                         Extent::Iterator row_i)
                    : _page(page),
                      _extent_i(extent_i),
                      _extent(std::move(extent)),
                      _row(row_i)
                { }

            private:
                Page *_page;
                std::vector<ExtentVar>::iterator _extent_i;
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

        public:
            /** A count of the number of users of this page. */
            std::atomic<uint16_t> use_count;

        private:
            /** A mutex to protect access. */
            boost::shared_mutex _mutex;

            /** The extents that make up this page. */
            std::vector<ExtentVar> _extents;

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

            /** The schema of the underlying extent data. */
            ExtentSchemaPtr _schema;

            // XXX these should actually be kept in the ExtentSchema
            std::shared_ptr<std::vector<FieldPtr>> _sort_fields;
            std::vector<std::string> _sort_keys;

            /** XXX Callbacks. */
            /** Callback to see if this page can be safely evicted?  May not need it since the underlying extents can always be flushed and the page will remain valid.  */
            
            /** Callback to perform additional actions after the page flush?  Is this necessary now that the pages won't be evicted by the cache?  Would be necessary to notify the BTree that a page's extents have been written?  Or is it enough to get the list of extent IDs on flush? */
        };

        boost::mutex _mutex;

        WriteCacheMap _page_cache;
        WriteCacheList _page_lru;

        ReadCache _read_cache;
    };
}
