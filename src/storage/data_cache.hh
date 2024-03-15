#pragma once

namespace springtail {

    /**
     * This cache holds Page objects that represent mutated data extents.  It is used for mutating
     * tables both during system table changes and for the GC commit phase.
     */
    class DataCache {
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
        PagePtr
        get(uint64_t extent_id, TablePtr table)
        {
            boost::scoped_lock lock(_mutex);

            // check the cache
            auto page_i = _cache.find({ table->id(), extent_id });
            if (page_i != _cache.end()) {
                // increase the use count
                ++std::get<2>(page_i->second);

                // remove from the LRU list
                _lru.erase(std::get<1>(page_i->second));
                std::get<1>(page_i->second) = _lru.end();

                // return the page
                return std::get<0>(page_i->second);
            }

            // create and populate the page
            auto page = std::make_shared<Page>(extent_id, table, _read_cb, _flush_cb);
            _cache.insert(extent_id, { page, _lru.end(), 0 });

            // XXX how / when do we flush pages out of the cache?  Pages increase when being mutated...

            return page;
        }

        /**
         * Releases a dirty page back to the cache.  Page objects that are in-use cannot be flushed
         * until everyone using them has released them back to the cache.
         */
        void
        release(PagePtr page)
        {
            boost::scoped_lock lock(_mutex);

            // find the cache entry
            auto cache_i = _cache.find({ page->table_id(), page->id() });
            assert(cache_i != _cache.end());

            // reduce the usage count on the page
            --std::get<2>(cache_i->second);

            // place the page onto the LRU list
            if (std::get<2>(cache_i->second) == 0) {
                auto pos = _lru.insert(_lru.end(), page);
                std::get<1>(cache_i->second) = pos;
            }
        }

        /**
         * Flush all of the pages associated with a given table.
         */
        void
        flush(TablePtr table)
        {
            boost::scoped_lock(_mutex);

            auto i = _cache.lower_bound({ table->id(), 0 });
            while (i != _cache.end() && i->first == table->id()) {
                // no one can be using the table's pages
                assert(std::get<2>(i->second) == 0);

                // flush the page
                std::get<0>(i->second)->flush(false);

                // clear the page from the LRU list?
                _lru.erase(std::get<1>(i->second));

                // move to the next entry
                ++i;
            }
        }

        /**
         * Discard all of the pages associated with a given table.
         */
        void
        discard(TablePtr table)
        {
            boost::scoped_lock(_mutex);

            auto i = _cache.lower_bound({ table->id(), 0 });
            while (i != _cache.end() && i->first == table->id()) {
                // no one can be using the table's pages
                assert(std::get<2>(i->second) == 0);

                // flush the page
                std::get<0>(i->second)->flush(true);

                // move to the next entry
                auto j = i++;

                // remove the page from the cache
                _lru.erase(std::get<1>(j->second));
                _cache.erase(j);
            }
        }

    public:
        /**
         * Holds the data for a single mutated extent.  Because there may be a large number of
         * mutations in a single XID, the Page must be able to spill to disk.  To support this, the
         * Page may utilize a temporary MutableBTree kept in node-local storage to track extents
         * that have been flushed to disk.
         */
        class Page {
        public:
            Page(uint64_t extent_id, TablePtr table)
                : _table(table)
            {
                // read the initial extent
                auto extent = _read_extent(extent_id);
                _extent_map.insert({ extent_id, { extent } });
            }

            void
            set_target_xid(uint64_t xid)
            {
                // XXX need to set the XID that we are bringing the data forward to so that we can
                // write out extents with the correct XID in the header.
            }

            /**
             * Adds a new row to the Page with the provided value.
             */
            void
            insert(TuplePtr value)
            {
                // lock the page
                boost::scoped_lock lock(_mutex);

                auto key = _schema->tuple_subset(value, _keys);
                auto pos_i = _lower_bound(key);

                // insert the new row
                ExtentPtr e = *(pos_i.extent_i);
                Extent::Row row = e->insert(pos_i.row_i);
                MutableTuple(fields, row).assign(value);

                // check if we need to split the extent
                _check_split(pos_i);
            }

            /**
             * Removes a row from the Page with the provided primary key.
             */
            void
            remove(TuplePtr key)
            {
                boost::scoped_lock lock(_mutex);

                // find the entry
                auto pos_i = _lower_bound(key);

                // make sure we got an exact match
                if (key.less_than(MutableTuple(key_fields, row))) {
                    SPDLOG_ERROR("Tried to remove non-existant key");
                    return;
                }

                // remove the row
                ExtentPtr e = *(pos_i.extent_i);
                e->remove(pos_i->row_i);

                // note: we currently rely on the GC-3 to perform all data merging, but it would
                //       make sense to merge in-memory extents here
            }

            /**
             * Removes a row from the Page with the provided primary key.
             */
            void
            update(TuplePtr value)
            {
                boost::scoped_lock lock(_mutex);

                // find the entry
                auto key = _schema->tuple_subset(value, _keys);
                auto pos_i = _lower_bound(key);

                // make sure we got an exact match
                if (key.less_than(MutableTuple(key_fields, row))) {
                    SPDLOG_ERROR("Tried to remove non-existant key");
                    return;
                }

                // update the row
                ExtentPtr e = *(pos_i.extent_i);
                MutableTuple(fields, *(pos_i.row_i)).assign(value);

                // check if we need to split the extent
                _check_split(pos_i);
            }

            /**
             * Flush this page.
             * XXX we may want a way to partially flush a page to make space in the cache
             */
            void
            flush(bool finalize = false)
            {
                // write the dirty extents to the data file
                for (auto &&entry : _extent_map) {
                    // remove the internal btree index entry for the old extent ID
                    // note: no need to remove if the entry is still the original extent
                    if (entry.first != _id) {
                        _index->remove(MutableTuple(_key_fields, entry.second.back()->back()));
                    }

                    // get the fields of the btree which are the <key fields, extent ID field>
                    FieldArrayPtr key_fields = _index->get_key_fields();
                    FieldArrayPtr value_fields = std::make_shared<FieldArray>(1);

                    // write out each extent from the list
                    for (auto &&extent : entry.second) {
                        uint64_t extent_id = _write_extent(extent);

                        // set the extent ID as the value
                        (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

                        // add the new extent ID into the internal btree index
                        auto &&row = extent->back();
                        _index->insert(std::make_shared<KeyValueTuple>(key_fields, value_fields, row));

                        if (_cache->_for_gc) {
                            // re-populate the indexes of the table
                            _table->populate_indexes(extent, extent_id);
                        }
                    }
                }

                // clear the extent map
                // note: clean extents are cached in the main data cache if they need to be re-read
                _extent_map.clear();
            }

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
            uint64_t table_id() const
            {
                return _table->id();
            }

        public:
            class Iterator {
                friend DataCache;

            public:
                using iterator_category = std::forward_iterator_tag;
                using difference_type   = std::ptrdiff_t;
                using value_type        = const Extent::Row;
                using pointer           = const Extent::Row *;  // or also value_type*
                using reference         = const Extent::Row &;  // or also value_type&

                Iterator(Page *page,
                         MutableBTree::Iterator index_i,
                         std::vector<ExtentPtr>::iterator extent_i,
                         Extent::Iterator row_i)
                    : page(page),
                      index_i(index_i),
                      extent_i(extent_i),
                      row_i(row_i)
                { }

                reference operator*() const { return *(row_i); }
                pointer operator->() { return &(*(row_i)); }

                Iterator& operator++() {
                    // move to the next row in the extent
                    ++row_i;
                    if (row_i != (*extent_i)->end()) {
                        return *this;
                    }

                    // if we reached the end of the extent, move to the next extent in the vector
                    ++extent_i;
                    if (extent_i != extents.end()) {
                        row_i = (*extent_i)->begin();
                        return *this;
                    }

                    // if we reached the end of the vector, move to the next extent in the BTree
                    ++index_i;
                    if (index_i != page->_index.end()) {
                        // check if we have the extent in memory
                        uint64_t extent_id = extent_id_f->get_uint64(*index_i);
                        auto map_i = page->_extent_map.find(extent_id);
                        if (map_i == page->_extent_map.end()) {
                            // read the extent into memory
                            auto extent = page->_read_extent(extent_id);
                            map_i = page->_extent_map.insert({extent_id, { extent } });
                        }

                        // we now are guaranteed to have the extent in memory
                        extents = map_i->second;
                        extent_i = extents.begin();
                        row_i = (*extent_i)->begin();
                        return *this;
                    }

                    return *this;
                }

                Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

                friend bool operator==(const Iterator& a, const Iterator& b) {
                    return ((a.index_i == b.index_i && a.index_i == page->_index.end()) ||
                            (a.index_i ==  b.index_i && a.extent_i = a.extent_i && a.row_i == b.row_i));
                }

                friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

            private:
                // friend constructor
                Iterator()
                { }

            private:
                Page * const page;
                MutableBTree::Iterator index_i;
                std::vector<ExtentPtr>::iterator extent_i;
                Extent::Iterator row_i;
            };

            Iterator begin() const
            {
                auto index_i = _index.begin();
                return Iterator(this, index_i);
            }

            Iterator end() const
            {
                auto index_i = _index.end();
                return Iterator(this, index_i);
            }

        protected:
            Iterator
            _lower_bound(TuplePtr search_key)
            {
                Iterator it;

                // if the extent_map contains the original extent_id, then we've never flushed data
                // to disk, so no need to search the btree index
                auto entry_i = _extent_map.find(_id);
                if (entry_i == _extent_map.end()) {
                    // the original extent was flushed, so we need to search the btree
                    it.index_i = _index.lower_bound(value);
                    auto extent_id = _extent_id_f->get_uint64(*it.index_i);

                    // check if the extent we need is already cached
                    entry_i = _extent_map.find(extent_id);
                    if (entry_i == _extent_map.end()) {
                        // need to read the extent from disk
                        auto extent = _read_extent(extent_id);
                        entry_i = _extent_map.insert({ extent_id, { extent } });
                    }
                } else {
                    it.index_i = _index.end();
                }

                // once we have the list of extents, search them for the insert position
                auto &&extents = entry_i->second;

                // search the extent vector
                it.extent_i = std::lower_bound(extents.begin(), extents.end(), search_key,
                                               [this](const ExtentPtr &extent, TuplePtr key) {
                                                   return MutableTuple(this->_key_fields, extent->back()).less_than(key);
                                               });

                // search within the extent
                ExtentPtr e = *extent_i;
                it.row_i = std::lower_bound(e->begin(), e->end(), search_key,
                                            [this](const Extent::Row &row, TuplePtr key) {
                                                return MutableTuple(this->_key_fields, row).less_than(key);
                                            });

                return it;
            }

            ExtentPtr
            _read_extent(uint64_t extent_id)
            {
                // XXX read the given extent_id from disk
                extent = _cache->io->read(extent_id);

                if (_cache->_for_gc) {
                    // we assume that the extent is being read for modification, so we need to
                    // invalidate the secondary index entries for this extent
                    _table->invalidate_indexes(extent_id, extent);
                }

                return extent;
            }

            uint64_t
            _write_extent(ExtentPtr extent)
            {
                // write the extent to disk
                // XXX when not performing GC we should keep these extents in a look-aside file; how to track?
                auto extent_id = _cache->io->write(extent);

                // cache the clean page in case it's needed again
                _cache->_release(extent_id, extent);

                return extent_id;
            }

            void
            _check_split(Iterator pos_i)
            {
                ExtentPtr e = *(pos_i.extent_i)

                // check the size of the extent
                if (e->byte_count() < MAX_EXTENT_SIZE) {
                    return;
                }

                // extent has grown too large, split it
                auto &&pair = e->split();
                _size -= e->byte_count();

                // XXX remove the existing entry and insert the two new ones
                auto pos = map_i->second.erase(pos_i.extent_i);
                pos = map_i->second.insert(pos, pair.second);
                map_i->second.insert(pos, pair.first);

                _size += pair.first->byte_count() + pair.second->byte_count();

                // XXX if the page is getting too large then we may need to proactively flush it to
                //     disk like we do for the BTree.  Less critical here given the lack of lock
                //     contention, but from a memory pressure perspective it will be necessary.

                // XXX would be great if we could somehow keep an LRU of dirty extents and use that
                //     to evict just a single extent, or a single vector of extents
            }

        private:
            boost::shared_mutex _mutex;

            std::map<uint64_t, std::vector<ExtentPtr>> _extent_map;
            MutableBTree _index;

            TablePtr _table;
            uint64_t _id;
        };
        typedef std::shared_ptr<Page> PagePtr;

    private:
        typedef std::tuple<PagePtr, std::list<PagePtr>::iterator, uint32_t> CacheEntry;
        typedef std::pair<uint64_t, uint64_t> CacheKey;

        std::list<PagePtr> _lru;
        std::map<CacheKey, CacheEntry> _cache;
    };

}
