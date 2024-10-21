#pragma once

#include <filesystem>
#include <memory>

#include <boost/thread.hpp>

#include <common/constants.hh>
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
                  _state(State::DIRTY),
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
             * Returns the cache ID of a mutable / dirty extent.
             */
            uint64_t cache_id() const {
                return _cache_id;
            }

        private:
            std::filesystem::path _file; ///< The file containing this extent.
            uint64_t _extent_id; ///< The extent_id of this extent.

            // note: the following are only used by the DataCache and are protected by it's mutex

            uint16_t _use_count; ///< The number of users of this extent.
            std::list<std::shared_ptr<CacheExtent>>::iterator _pos; ///< The position of this entry on it's global LRU list.  Invalid if use count is non-zero.

            State _state; ///< The current state of this extent.
            std::shared_ptr<boost::condition_variable> _flush_cv; ///< A condition variable used to notify waiters when the extent is no longer FLUSHING.

            uint64_t _cache_id; ///< A unique ID provided from the DataCache when the CacheExtent is MUTABLE / DIRTY and shouldn't be referenced by extent_id.
        };
        using CacheExtentPtr = std::shared_ptr<CacheExtent>;

        /**
         * Reference to an extent in the DataCache.  First holds the ID of the extent.  If second is
         * true, then the ID is a cache ID, false then an extent ID.
         */
        using ExtentRef = std::pair<uint64_t, bool>;

        /**
         * A list of CacheExtent objects used for tracking the LRU entry.
         */
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
             * @param mark_dirty Mark the extent as DIRTY while retrieving.
             * @return A pointer to the extent.
             */
            CacheExtentPtr get(uint64_t cache_id, bool mark_dirty = false);

            /**
             * Increment the use count on a cache extent.
             */
            void use(CacheExtentPtr extent) {
                boost::unique_lock lock(_mutex);
                ++(extent->_use_count);
            }

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

            /**
             * Write the provided DIRTY extent to disk.  Extent is returned to the MUTABLE state
             * after the flush() is complete.
             */
            void flush(CacheExtentPtr extent);

            /**
             * Invalidates the provided DIRTY extent, ensuring that it is released without being
             * written to disk.
             */
            void drop_dirty(CacheExtentPtr extent);

            /**
             * Splits the provided extent into two new extents, each holding roughly half of the
             * rows.  The provided extent is invalidated and references to the two new DIRTY extents
             * are returned.
             */
            std::pair<ExtentRef, std::weak_ptr<CacheExtent>> split(CacheExtentPtr extent, ExtentSchemaPtr schema);

            /**
             * Returns an empty DIRTY extent tied to the provided file.
             */
            CacheExtentPtr get_empty(const std::filesystem::path &file, const ExtentHeader &header);

        private:
            /**
             * Helper to retrieve an extent from the clean cache.  If the provided key is not
             * cached, this function will retrieve the extent from disk.
             */
            CacheExtentPtr _get_clean(const CacheKey &key);

            /**
             * Internal helper to make space for a new extent within the cache by evicting another extent.
             *
             * @param lock The lock currently holding the cache mutex.
             */
            void _make_extent_space();

            /**
             * Internal helper to release an extent back to the cache.  Reduces use count and places
             * into the LRU queue if no longer in use.
             *
             * @param extent The extent to release.
             */
            void _release(CacheExtentPtr extent);

            /**
             * Helper to write the provided DIRTY extent to disk.  Extent is returned to the MUTABLE
             * state after the flush() is complete.
             */
            void _flush(CacheExtentPtr extent);

            /**
             * Helper to read a CLEAN extent into memory.  A callback is provided to be run after
             * the IO to read the extent is complete.
             */
            CacheExtentPtr _read_extent(const CacheKey &key, std::function<void(CacheExtentPtr)> callback);

            /**
             * Helper to create a new unique cache ID and associate it with the provided extent.
             */
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
            /**
             * Constructor for creating a page based on an existing set of extent IDs.  The provided
             * access XID defines the start / end XIDs for the new page.  If the target XID is the
             * same as the access XID, then the page is not going to be rolled forward.
             */
            Page(const std::filesystem::path &file, uint64_t extent_id,
                 uint64_t start_xid, uint64_t end_xid, const std::vector<uint64_t> &offsets);

            /**
             * Constructor for creating an empty page.  Starts marked dirty and uses the provided
             * XID as the start, end and target XID for the page.
             */
            Page(const std::filesystem::path &file, uint64_t xid);

            /**
             * Writes all of the dirty in-memory extents to disk and returns the full set of extent
             * IDs for the page.
             *
             * @param flush_xid The XID at which this page is being written out.  The page is
             *                  considered valid from this XID forward.
             * @return The ordered list of extent IDs that represent the data of the Page.
             */
            std::vector<uint64_t> flush(const ExtentHeader &header);

            /**
             * Flushes an empty page to disk by creating an empty extent and flushing that to disk,
             * returning the single extent offset of that empty extent.
             */
            uint64_t flush_empty(const ExtentHeader &header);

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
             * Check if the requested XID is within the bounds of the valid XIDs for accessing this page.
             */
            bool check_xid_valid(uint64_t xid) const {
                return (_start_xid <= xid && xid <= _end_xid);
            }

        private:
            /**
             * Registers an eviction callback.
             */
            void _register_flush(std::function<bool(std::shared_ptr<Page>)> callback) {
                boost::unique_lock lock(_mutex);
                _flush_callback = callback;
            }

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
                           bool mark_dirty = false)
                    : _extent(nullptr)
                {
                    if (ref.second) {
                        _extent = StorageCache::get_instance()->_data_cache->get(ref.first, mark_dirty);
                    } else if (!mark_dirty) {
                        _extent = StorageCache::get_instance()->_data_cache->get(file, ref.first);
                    } else {
                        _extent = StorageCache::get_instance()->_data_cache->extract(file, ref.first);

                        // update the reference in the Page to reflect the extent's cache ID
                        ref.first = _extent->cache_id();
                        ref.second = true;
                    }
                }

                // copy causes the use count to be incremented
                SafeExtent(const SafeExtent &other) {
                    if (other._extent != nullptr) {
                        StorageCache::get_instance()->_data_cache->use(other._extent);
                    }
                    _extent = other._extent;
                }
                SafeExtent &operator=(const SafeExtent &other) {
                    if (other._extent != nullptr) {
                        StorageCache::get_instance()->_data_cache->use(other._extent);
                    }
                    _extent = other._extent;
                    return *this;
                }

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

                Iterator()
                    : _page(nullptr)
                { }

                reference operator*() const {
                    return *_row;
                }
                pointer operator->() {
                    return &(*_row);
                }

                /**
                 * Increment operator -- moves to point at the next row in the page.
                 */
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

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }


                /**
                 * Decrement operator -- moves to point at the previous row in the page.
                 */
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

                Iterator operator--(int) { Iterator tmp = *this; --(*this); return tmp; }

                /**
                 * Equality operator -- compares if this iterator is at the same row position as the
                 * provided iterator.
                 */
                bool operator==(const Iterator &rhs) const {
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
                Page *_page; ///< The associated page.  Used to check for the _extent_i end() condition.
                std::vector<ExtentRef>::iterator _extent_i; ///< Iterator into the extents of the page.
                SafeExtent _extent; ///< The current extent.
                Extent::Iterator _row; ///< Iterator into the current extent.
            };

            /**
             * Returns an iterator to the first row of the page.
             */
            Iterator begin() {
                boost::shared_lock lock(_mutex);
                return Iterator(this, _extents.begin());
            }

            /**
             * Returns an iterator to the last row of the page.
             */
            Iterator last() {
                boost::shared_lock lock(_mutex);
                SafeExtent extent(_file, _extents.back());
                auto row_i = (*extent)->last();
                return Iterator(this, --_extents.end(), std::move(extent), row_i);
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
            Iterator lower_bound(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Returns the first row of the page with columns > the matching columns of the provided tuple.
             * @param tuple A tuple holding data that matches the sort columns of the extent.
             */
            Iterator upper_bound(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Returns the first row of the page with columns <= the matching columns of the provided tuple.
             * @param tuple A tuple holding data that matches the sort columns of the extent.
             */
            Iterator inverse_lower_bound(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Returns an iterator to the row at the provided index within the page.
             * @param index The index within the page to retrieve the row.
             */
            Iterator at(uint32_t index);

            /**
             * Returns the Page object's extent header data.  It is based of the original extent the
             * Page is based on.
             */
            ExtentHeader header() const {
                SafeExtent extent(_file, _extents.front());
                return (*extent)->header();
            }

            /**
             * Checks if the Page object is empty -- either contains no extents, or a single extent
             * with no rows.
             */
            bool empty() const {
                // if no extents, empty
                if (_extents.empty()) {
                    return true;
                }

                // if more than one extent, can't be empty
                if (_extents.size() > 1) {
                    return false;
                }

                // if one extent, and the extent is empty, then empty
                SafeExtent extent(_file, _extents.front());
                return (*extent)->empty();
            }

            /**
             * Returns the number of extents that are backing the Page.
             */
            uint32_t extent_count() const {
                return _extents.size();
            }

        public:
            // MUTATIONS
            // note: all mutations will invalidate an Iterator on the page

            /**
             * Inserts the provided tuple into the Page using the provided ExtentSchema.
             */
            void insert(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Appends the provided tuple to the Page using the provided ExtentSchema.
             */
            void append(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Upserts the provided tuple to the Page using the provided ExtentSchema.
             * @return True if the upsert() resulted in an insert()
             */
            bool upsert(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Updates the row in the Page with a matching key as the provided tuple to fully match
             * the tuple, using the provided ExtentSchema.
             */
            void update(TuplePtr tuple, ExtentSchemaPtr schema);

            /**
             * Removes a row with the provided key from the Page using the provided ExtentSchema.
             */
            void remove(TuplePtr key, ExtentSchemaPtr schema);

            /**
             * Removes the row at the provided position in the Page.
             */
            void remove(const Iterator &pos);

            /**
             * Converts the page to the provided target_schema using the provided schema to read
             * target_schema formatted rows from the existing extents and writing them to new extents.
             */
            void convert(VirtualSchemaPtr schema, ExtentSchemaPtr target_schema);

        private:
            // HELPER FUNCTIONS

            /**
             * Checks if the provided extent needs to be split and performs the split if needed.
             */
            void _check_split(std::vector<ExtentRef>::iterator pos, CacheExtentPtr extent, ExtentSchemaPtr schema);

            /**
             * Helper to flush the underlying extents of a Page and to return the extent IDs of the
             * new on-disk positions.
             */
            std::vector<uint64_t> _flush(const ExtentHeader &header);

        private:
            /** A count of the number of users of this page. */
            std::atomic<uint16_t> _use_count;

            /** A mutex to protect access. */
            mutable boost::shared_mutex _mutex;

            // XXX we should utilize weak_ptr to provide direct access to in-memory extents when available
            /** The extents that make up this page. */
            std::vector<ExtentRef> _extents;

            /** A flag indicating if the page is clean or dirty. */
            bool _is_dirty;

            /** The file that this page is associated with. */
            std::filesystem::path _file;

            /** The original extent_id of this page. */
            uint64_t _extent_id;

            /** The starting XID from which this page is known valid for access. */
            uint64_t _start_xid;

            /** The ending XID up through which this page is known valid for access. */
            uint64_t _end_xid;

            /** The position on the LRU list; only valid when _use_count is zero. */
            std::list<std::shared_ptr<Page>>::iterator _lru_pos;

            /** Callback to be issued to flush the page.  Used if the page is evicted or if the page
                is flushed as part of a flush_file(). */
            std::function<bool(std::shared_ptr<Page>)> _flush_callback;

            /** Position on the PageCache flush list.  Set to end() if not on the list. */
            std::list<std::shared_ptr<Page>>::iterator _flush_pos;
        };
        using PagePtr = std::shared_ptr<Page>;


    private:
        /**
         * LRU cache of Page objects.  Maintains a maximum number of extent references across all of the pages.
         *
         * Pages are valid over a given XID range.  When requesting a page, it should be requested
         * based on a given extent_id using a given access XID and target XID.
         *
         * The access XID specifies the XID at which the Page will be starting.  This is used to
         * check for any known changes to the extent ID that occurred up to the given access XID.
         *
         * The target XID specifies the XID at to which the Page will be modified.  If the target
         * XID matches the access XID, it implies that the Page will not be modified.
         */
        class PageCache {
        public:
            PageCache(uint64_t max_size)
                : _max_size(max_size),
                  _size(0)
            { }

            /**
             * Retrieve a Page object from the cache.
             *
             * @param file The file from which to retrieve the page.
             * @param extent_id The extent ID to retrieve from the file.
             * @param access_xid The XID at which the extent ID is being read.
             * @param target_xid The XID at which the page will operate and perform mutations.
             */
            PagePtr get(const std::filesystem::path &file, uint64_t extent_id,
                        uint64_t access_xid, uint64_t target_xid);

            /**
             * Retrieve an empty Page object from the cache for a given file, operating at the
             * provided XID.
             */
            PagePtr get_empty(const std::filesystem::path &file, uint64_t xid);

            /**
             * Returns a page to the cache.  Optionally registers a callback that will be called
             * against the page when the page is evicted or flush_file() is called.
             */
            void put(PagePtr page, std::function<bool(std::shared_ptr<Page>)> flush_callback);

            /**
             * Flushes all of the pages associated with a file that have a registered flush
             * callback.
             */
            void flush_file(const std::filesystem::path &file);

            /**
             * Drops all of the dirty pages associated with a file without flushing them to disk.
             * Used to implement table truncate.
             */
            void drop_file(const std::filesystem::path &file);

        private:
            /**
             * Helper to return a page to the cache.
             */
            void _put(PagePtr page);

            /**
             * Helper to retrieve a page from the cache.  If the page doesn't exist in the cache
             * then returns a nullptr.
             */
            PagePtr _try_get(const std::filesystem::path &file, uint64_t extent_id, uint64_t xid);

            /**
             * Helper to try and evict a page from the cache.  Will silently fail if there is a
             * registered flush_callback that does not succeed.
             */
            void _try_evict(PagePtr page);

            /**
             * Helper to create a Page in the cache, potentially evicting another page to make space
             * for this new page.
             */
            PagePtr _create(const std::filesystem::path &file, uint64_t extent_id,
                            uint64_t xid, const std::vector<uint64_t> &offsets);

            /**
             * Makes space for some number of pages, evicting existing pages in the cache if necessary.
             */
            void _make_page_space(uint32_t space_needed);

        private:
            using XidMap = std::map<uint64_t, PagePtr>;
            using CacheMap = std::unordered_map<CacheKey, XidMap, boost::hash<CacheKey>>;

            boost::mutex _mutex; ///< Mutex to protect the cache members

            CacheMap _cache; ///< The page cache, keyed by CacheKey and XID
            std::list<PagePtr> _lru; ///< LRU list of pages.

            /** List of pages with flush callbacks for each file. */
            std::map<std::filesystem::path, std::list<PagePtr>> _flush_list;

            uint64_t _max_size; ///< The max number of pages in the cache.
            uint64_t _size; ///< The current number of pages in the cache.
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
         * @param extent_id The extent_id that this page represents.  If UNKNOWN then creates an
         *                  empty page that will be appended to the file on flush.
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
                    uint64_t access_xid,
                    uint64_t target_xid = constant::LATEST_XID,
                    bool do_rollforward = false);

        /**
         * Release a Page object back to the cache.
         *
         * @param page The page to release.
         */
        void put(PagePtr page,
                 std::function<bool(std::shared_ptr<Page>)> flush_callback = nullptr);

        /**
         * Flush all of the pages associated with a given file to disk.  Waits for all of the pages
         * to have valid extent IDs and returns a future that can be used to wait for the sync to
         * disk to complete.
         */
        void flush(const std::filesystem::path &file);

        /**
         * Drop all of the dirty pages associated with a given file without writing them.  Used to
         * support truncate.
         */
        void drop_for_truncate(const std::filesystem::path &file);

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
