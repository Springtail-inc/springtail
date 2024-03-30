#pragma once

#include <boost/thread.hpp>

#include <storage/temp_btree.hh>

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
         * mutations in a single XID, the Page must be able to spill to disk.  To support this, the
         * Page may utilize a TempBTree (kept in node-local storage) to track extents
         * that have been flushed to disk before they are committed.
         */
        class Page {
        public:
            /**
             * Constructor.
             */
            Page(uint64_t extent_id, std::shared_ptr<MutableTable> table);

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
             * Removes a row from the Page with the provided primary key.
             */
            void remove(TuplePtr key);

            /**
             * Removes a row from the Page with the provided primary key.
             */
            void update(TuplePtr value);

            /**
             * Flush this page.
             * XXX we may want a way to partially flush a page to make space in the cache
             */
            void flush(bool finalize = false);

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
                         TempBTree::Iterator index_i,
                         std::map<uint64_t, std::vector<ExtentPtr>>::iterator map_i,
                         std::vector<ExtentPtr>::iterator extent_i)
                    : page(page),
                      index_i(index_i),
                      map_i(map_i),
                      extent_i(extent_i)
                { }

                Iterator(Page *page,
                         TempBTree::Iterator index_i,
                         std::map<uint64_t, std::vector<ExtentPtr>>::iterator map_i,
                         std::vector<ExtentPtr>::iterator extent_i,
                         Extent::Iterator row_i)
                    : page(page),
                      index_i(index_i),
                      map_i(map_i),
                      extent_i(extent_i),
                      row_i(row_i)
                { }

            public:
                using iterator_category = std::forward_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                reference operator*() const { return *(row_i); }
                pointer operator->() { return &(*(row_i)); }

                Iterator& operator++();

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

                friend bool operator==(const Iterator& a, const Iterator& b) {
                    return ((a.index_i == b.index_i && a.index_i == a.page->_index->end()) ||
                            (a.index_i ==  b.index_i && a.extent_i == a.extent_i && a.row_i == b.row_i));
                }

                friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

            private:
                Page * const page;
                TempBTree::Iterator index_i;
                std::map<uint64_t, std::vector<ExtentPtr>>::iterator map_i;
                std::vector<ExtentPtr>::iterator extent_i;
                Extent::Iterator row_i;
            };

            Iterator begin()
            {
                // if the tree is empty, return end()
                if (_index->empty()) {
                    return end();
                }

                // get the first entry in the tree
                auto index_i = _index->begin();

                // get the extent
                uint64_t extent_id;
                if (index_i == _index->end()) {
                    // in this case where data has never been flushed, the original extent ID is used
                    extent_id = _id;
                } else {
                    extent_id = _extent_id_f->get_uint64(*index_i);
                }

                // find the extent in the map
                auto &&map_i = _extent_map.find(extent_id);
                if (map_i == _extent_map.end()) {
                    // if missing, read it and add it to the map
                    auto extent = _read_extent(extent_id);
                    auto &&entry = _extent_map.insert({ extent_id, { extent } });
                    map_i = entry.first;
                }

                auto extent_i = map_i->second.begin();
                auto row_i = (*extent_i)->begin();
                return Iterator(this, index_i, map_i, extent_i, row_i);
            }

            Iterator end()
            {
                // XXX
                auto index_i = _index->end();
                auto map_i = _extent_map.find(_id);
                auto extent_i = map_i->second.end();
                return Iterator(this, index_i, map_i, extent_i);
            }

        protected:
            Iterator _lower_bound(TuplePtr search_key);

            ExtentPtr _read_extent(uint64_t extent_id);

            uint64_t _write_extent(ExtentPtr extent);

            void _check_split(Iterator pos_i);

        private:
            /** Protects the page against incorrect concurrent access. */
            boost::shared_mutex _mutex;

            /**
             * Holds a map from on-disk extent ID to a set of dirty extents that make up the mutated
             * data of the original extent.
             */
            std::map<uint64_t, std::vector<ExtentPtr>> _extent_map;

            /**
             * Holds the full list of mutated on-disk extents.  If a Page grows large, or other Page
             * mutations fill the cache, it's underlying data extents may be flushed to disk.  In
             * that case, the newly written extent locations are kept in this index.
             */
            TempBTreePtr _index;

            /** A pointer to the table that this Page is from. */
            std::shared_ptr<MutableTable> _table;

            /** The original extent ID of the underlying data. */
            uint64_t _id;

            FieldPtr _extent_id_f;

            ExtentSchemaPtr _schema;
            std::vector<std::string> _keys;
            MutableFieldArrayPtr _fields;
            MutableFieldArrayPtr _key_fields;
            DataCache *_cache;
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
         * Flush all of the pages associated with a given table.
         */
        void flush(std::shared_ptr<Table> table);

        /**
         * Discard all of the pages associated with a given table.
         */
        void discard(std::shared_ptr<Table> table);

    private:
        typedef std::tuple<PagePtr, std::list<PagePtr>::iterator, uint32_t> CacheEntry;
        typedef std::pair<uint64_t, uint64_t> CacheKey;

        /** Protects the cache against incorrect concurrent access. */
        boost::shared_mutex _mutex;

        std::shared_ptr<IOHandle> _handle;

        std::list<PagePtr> _lru;
        std::map<CacheKey, CacheEntry> _cache;
        bool _for_gc;
    };

    typedef std::shared_ptr<DataCache> DataCachePtr;

}
