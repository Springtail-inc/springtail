#pragma once

#include <boost/thread.hpp>

#include <storage/extent.hh>
#include <storage/field.hh>

namespace springtail {

    // forward declaration
    class Table;
    class MutableTable;

    /**
     * This cache holds Page objects that represent mutated data extents.  It is used for mutating
     * tables both during system table changes and for the GC commit phase.
     */
    class DataCache {
    public:
        /**
         * Holds the data for a single mutated extent.  Because there may be a large number of
         * mutations in a single XID, the Page must eventually be able to spill to disk, however,
         * right now we keep all of the extents in a single vector in-memory.
         */
        class Page {
        public:
            class Iterator;

            /**
             * Constructor.
             */
            Page(DataCache *cache, uint64_t extent_id, std::shared_ptr<MutableTable> table);

            void
            set_target_xid(uint64_t xid)
            {
                // XXX need to set the XID that we are bringing the data forward to so that we can
                // write out extents with the correct XID in the header.
            }

            /**
             * Adds a new row to the Page with the provided value.
             */
            void insert(TuplePtr value);

            /**
             * Adds a new row to the end of the Page.
             */
            void append(TuplePtr value);

            /**
             * Adds a new row to the Page with the provided value, if a row with a matching key
             * doesn't already exist.  If a row with the key already exists, then replace it.
             */
            void upsert(TuplePtr value);

            /**
             * Removes a row from the Page with the provided primary key.
             */
            void remove(TuplePtr key);

            /**
             * Removes a row from the Page at the provided position.
             */
            void remove(const Iterator &pos);

            /**
             * Updates a row in the Page with the provided value, using the primary key in the value.
             */
            void update(TuplePtr value);

            /**
             * Flush this page.
             * XXX we may want a way to partially flush a page to make space in the cache
             */
            void flush();

            /**
             * Retrieve the extent ID of this page.
             */
            uint64_t id() const
            {
                return _id;
            }

            /**
             * Retrieve the table ID of this page.
             */
            uint64_t table_id() const;

        public:
            class Iterator {
                friend DataCache;

                Iterator(Page *page,
                         std::vector<ExtentPtr>::iterator extent_i)
                    : page(page),
                      extent_i(extent_i)
                { }

                Iterator(Page *page,
                         std::vector<ExtentPtr>::iterator extent_i,
                         Extent::Iterator row_i)
                    : page(page),
                      extent_i(extent_i),
                      row_i(row_i)
                { }

            public:
                using iterator_category = std::bidirectional_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                reference operator*() const { return *(row_i); }
                pointer operator->() { return &(*(row_i)); }

                Iterator& operator++();
                Iterator& operator--();

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }
                Iterator operator--(int) { Iterator tmp = *this; --(*this); return tmp; }

                friend bool operator==(const Iterator& a, const Iterator& b) {
                    return (a.extent_i == b.extent_i &&
                            (a.extent_i == a.page->_extents.end() || a.row_i == b.row_i));
                }

                friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

            private:
                Page * const page;
                std::vector<ExtentPtr>::iterator extent_i;
                Extent::Iterator row_i;
            };

            Iterator begin()
            {
                // if the extent vector has no rows, return end()
                if (_extents.empty() || (_extents.size() == 1 && _extents[0]->empty())) {
                    return end();
                }

                // get the first row from the first vector entry
                auto extent_i = _extents.begin();
                auto row_i = (*extent_i)->begin();
                return Iterator(this, extent_i, row_i);
            }

            Iterator end()
            {
                auto extent_i = _extents.end();
                return Iterator(this, extent_i);
            }

        protected:
            void _remove(const Iterator &pos);

            Iterator _lower_bound(TuplePtr search_key);

            ExtentPtr _read_extent(uint64_t extent_id);

            uint64_t _write_extent(ExtentPtr extent);

            void _check_split(std::vector<ExtentPtr>::iterator extent_i);

        private:
            /** Protects the page against incorrect concurrent access. */
            boost::shared_mutex _mutex;

            /**
             * Holds a set of dirty extents that make up the mutated data of the original extent.
             */
            std::vector<ExtentPtr> _extents;

            DataCache *_cache;

            /** The original extent ID of the underlying data. */
            uint64_t _id;

            /** A pointer to the table that this Page is from. */
            std::shared_ptr<MutableTable> _table;

            MutableFieldArrayPtr _fields;
            MutableFieldArrayPtr _key_fields;
            uint64_t _size;
        };
        typedef std::shared_ptr<Page> PagePtr;

    public:
        /**
         * Constructs the DataCache object.  When being used for the garbage collector the data
         * cache will make callbacks into the table to update the indexes.
         *
         * @param for_gc When true the data cache will issue callbacks to the table to update it
         *               primary and secondary indexes to match the provided mutations.
         */
        DataCache(bool for_gc = false)
            : _for_gc(for_gc)
        { }

        /**
         * Retrieve a Page based on the extent_id.  If no Page is available, create a new Page for it.
         */
        PagePtr get(uint64_t extent_id, std::shared_ptr<MutableTable> table);

        /**
         * Releases a dirty page back to the cache.  Page objects that are in-use cannot be flushed
         * until everyone using them has released them back to the cache.
         */
        void release(PagePtr page);

        /**
         * Evict all of the pages associated with a given table.  Optionally evict without flushing them to disk.
         */
        void evict(std::shared_ptr<MutableTable> table, bool flush = true);

    private:
        typedef std::tuple<PagePtr, std::list<PagePtr>::iterator, uint32_t> CacheEntry;
        typedef std::pair<uint64_t, uint64_t> CacheKey;

        /** Protects the cache against incorrect concurrent access. */
        boost::shared_mutex _mutex;

        std::list<PagePtr> _lru;
        std::map<CacheKey, CacheEntry> _cache;
        bool _for_gc;
    };

    typedef std::shared_ptr<DataCache> DataCachePtr;

}
