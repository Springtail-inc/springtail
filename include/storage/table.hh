#pragma once

namespace springtail {

    /**
     * Read-only interface to a table at a fixed XID.  Provides interfaces for accessing table
     * information, performing scans, extent_id lookups, etc.
     */
    class Table {
    public:
        Table(uint64_t table_id,
              const std::vector<std::string> &primary_key,
              std::shared_ptr<ExtentCache> cache,
              uint64_t xid,
              uint64_t root_offset)
        { }

        /** Returns true if the table has a primary key.  False otherwise. */
        bool has_primary() {
            return !_primary_key.empty();
        }

        /** Finds the extent_id that may contain the provided key, using the primary key index. */
        uint64_t primary_lookup(TuplePtr tuple)
        {
            // always returns an iterator to a leaf entry where the key *could* exist in the table
            auto &&i = _primary_index->find_for_update(tuple, xid);
            if (i == _primary_index->end()) {
                // this can only happen if the table is empty, in which case we need to use a
                // special extent_id that indicates an append
                return -1;
            }

            // extract the extent_id and return it
            return _primary_extent_id_f->get_uint64(*i);
        }

        /**
         * Retrieves the schema for the table at a given XID.
         */
        SchemaPtr schema(uint64_t xid) const
        {
            return _schema_manager->get_extent_schema(_id, xid);
        }

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key_columns;
        }

        /** Retrieve the ID of this table. */
        uint64_t id() const
        {
            return _id;
        }

        /**
         * Returns an iterator to the first row that is greater than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator lower_bound(TuplePtr search_key)
        {
            // find the extent that could contain the lower_bound() key
            auto &&i = _primary_index->lower_bound(search_key);
            if (i == _primary_index->end()) {
                return end();
            }

            // read the extent and find the lower_bound() of the key within it
            ExtentPtr extent = _read_extent_via_primary(i);

            // find the lower_bound() of the key within the data extent
            return std::lower_bound(extent->begin(), extent->end(), search_key,
                                    [this](const Extent::Row &row, TuplePtr key)
                                    {
                                        return FieldTuple(this->_pkey_fields, row).less_than(key);
                                    });
        }

        /**
         * An iterator to the start of the table.
         */
        Iterator begin()
        {
            auto &&index_i = _primary_index->begin();
            auto extent = _read_extent_via_primary(index_i);
            Iterator(this, _primary_index, index_i, extent, extent->begin());
        }

        /**
         * An iterator to the end of the table.
         */
        Iterator end()
        {
            Iterator(this, _primary_index);
        }

    public:
        class Iterator {
        public:
            /** Specifically for the end() iterator. */
            Iterator(const Table *table, BTreePtr btree)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree->end())
            { }

            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i,
                     ExtentPtr extent, const Extent::Iterator &extent_i)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree_i),
                  _extent(extent),
                  _extent_i(extent_i)
            { }

            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_extent_i); }
            pointer operator->() { return &(*(_extent_i)); }

            Iterator& operator++() {
                // move to the next row in the data extent
                ++_extent_i;
                if (_extent_i != _extent->end()) {
                    return *this;
                }

                // no more rows in the extent, so need to move to the next data extent
                ++_btree_i;
                if (_btree_i == _btree->end()) {
                    return *this;
                }
                
                // retrieve the data extent
                _extent = _table->_read_extent_via_primary(_btree_i);
                _extent_i = _extent->begin();

                return *this;
            }

            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            friend bool operator==(const Iterator& a, const Iterator& b) {
                return (a._btree_i == b._btree_i &&
                        (a._btree_i == a._btree->end() || a._extent_i == b._extent_i));
            }

            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

        private:
            const Table *_table;

            BTreePtr _btree;
            BTree::Iterator _btree_i;

            ExtentPtr _extent;
            Extent::Iterator _extent_i;
        };

    protected:
        ExtentPtr
        _read_extent_via_primary(BTree::Iterator &pos)
        {
            uint64_t extent_id = _primary_leaf_extent_f.get_uint64(*pos);
            return _read_extent(extent_id);
        }

        ExtentPtr
        _read_extent(uint64_t extent_id)
        {
            auto response = _handle->read(extent_id);
            return std::make_shared<Extent>(_schema, response->data);
        }

    private:
        /** The ID of the table. */
        uint64_t _id;

        /** The primary index of the table. */
        BTreePtr _primary_index;
    };


    /**
     * Interface for mutating a table at the most recent XID.
     */
    class MutableTable {
    public:
        /**
         * Add a row to the table.  The data extent ID is provided externally by the write cache, or
         * if the extent_id is UNKNOWN, then it will utilize the tuple data to determine where the
         * row should be added.
         */
        void insert(TuplePtr value, uint64_t xid, uint64_t extent_id)
        {
            if (extent_id == UNKNOWN_EXTENT) {
                if (_primary_key_columns.empty()) {
                    _insert_append(value, xid);
                } else {
                    _insert_by_lookup(value, xid);
                }
            } else {
                _insert_direct(value, xid, extent_id);
            }
        }

        /**
         * Remove a row from the table.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void remove(TuplePtr key, uint64_t xid, uint64_t extent_id)
        {
            if (extent_id == UNKNOWN_EXTENT) {
                if (_primary_key_columns.empty()) {
                    _remove_by_scan(key, xid);
                } else {
                    _remove_by_lookup(key, xid);
                }
            } else {
                _remove_direct(key, xid, extent_id);
            }
        }

        /**
         * Update a row in the table.  The value must contain the primary key to be updated and the
         * table must contain a primary key.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void update(TuplePtr value, uint64_t xid, uint64_t extent_id)
        {
            if (extent_id == UNKNOWN_EXTENT) {
                if (_primary_key_columns.empty()) {
                    // XXX error -- cannot perform an update() with no primary key, should be split into a remove() and insert()
                } else {
                    _update_by_lookup(value, xid);
                }
            } else {
                _update_direct(value, xid, extent_id);
            }
        }

        /**
         * Remove the entries into the extent from the primary and secondary indexes.
         */
        void
        invalidate_indexes(uint64_t extent_id, ExtentPtr extent)
        {
            // get the key from the last row of the extent and remove it from the primary index
            FieldArrayPtr key_fields = _primary_index->get_key_fields();
            
            // remove the primary index entry
            _primary_index->remove(FieldTuple(key_fields, extent.back()));

            // setup the value fields for the secondary indexes
            FieldArrayPtr value_fields = std::make_shared<FieldArray>(2);
            value_fields[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

            // go through each row and pass the relevant key to each of the secondary indexes for removal
            uint32_t row_id;
            for (auto &&row_i : *extent) {
                value_fields[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

                for (auto &&secondary : _secondary_indexes) {
                    key_fields = secondary->get_key_fields();

                    secondary->remove(KeyValueTuple(key_fields, value_fields, *row_i));
                }

                ++row_id;
            }
        }

        /**
         * Add the entries in the extent into the primary and secondary indexes.
         */
        void
        populate_indexes(uint64_t extent_id, ExtentPtr extent)
        {
            // get the key from the last row of the extent and add it to the primary index
            FieldArrayPtr key_fields = _primary_index->get_key_fields();
            FieldArrayPtr value_fields = std::make_shared<FieldArray>(1);
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(extent_id);

            _primary_index->insert(KeyValueTuple(key_fields, value_fields, extent.back()));

            // go through each row and pass the relevant key to each of the secondary indexes for insertion
            value_fields->resize(2);
            uint32_t row_id;
            for (auto &&row_i : *extent) {
                (*value_fields)[1] = std::make_shared<ConstTypeField<uint32_t>>(row_id);

                for (auto &&secondary : _secondary_indexes) {
                    key_fields = secondary->get_key_fields();

                    secondary->insert(KeyValueTuple(key_fields, value_fields, *row_i));
                }
            }
        }

    private:
        void
        _insert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id)
        {
            // get the page from the cache
            auto page = _cache->get(extent_id, this);

            // add the row to the page
            page->insert(value);

            // release the page back to the write cache
            _cache->release(page);
        }

        void
        _insert_append(TuplePtr value, uint64_t xid)
        {
            // there is no primary key, so append the row to the last extent
            uint64_t extent_id = _primary_index->back();

            // get the page from the cache
            auto page  = _cache->get(extent_id);

            // append the value to the extent
            page->append(value);

            // release the extent back to the write cache
            // note: the primary index is just a btree of extent IDs in the no-primary-key scenario
            _cache->release(page);
        }

        void
        _insert_by_lookup(TuplePtr value, uint64_t xid)
        {
            // we didn't receive an extent_id, so we need to look up the extent from the primary index
            auto search_key = _schema->tuple_subset(value, _primary_key_columns);
            auto i = _primary_index->lower_bound(search_key, xid);
            uint64_t extent_id = extent_id_f->get_uint64(*i);

            // then we can do a direct insert
            _insert_direct(value, xid, extent_id);
        }

        void
        _remove_direct(TuplePtr value, uint64_t xid, uint64_t extent_id)
        {
            // get the page from the cache
            auto page = _cache->get(extent_id, this);

            // remove the row from the page
            // note: this can only be used when a primary key is present, otherwise use _remove_by_scan()
            page->remove(value);

            // release the page back to the write cache
            _cache->release(page);
        }

        void
        _remove_by_lookup(TuplePtr key, uint64_t xid)
        {
            // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
            auto i = _primary->lower_bound(key, xid);
            uint64_t extent_id = extent_id_f->get_uint64(*i);

            // then we can do a direct removal
            _remove_direct(key, xid, extent_id);
        }

        void
        _remove_by_scan(TuplePtr value, uint64_t xid)
        {
            // we didn't receive an extent_id, and there is no primary index, so we must scan the
            // file to find the row to remove
            // note: in this case, it must be a full row match
            // note: it would be much more performant to perform all of the scan-based removals in
            //       an XID at once as a batch, since the table is likely to be much larger than the
            //       set of removals
            auto fields = _schema->get_fields();

            // scan the index
            bool found = false;
            auto &&i = _primary->begin();
            while (!found && i != _primary->end()) {
                // scan each extent, looking for a match
                uint64_t extent_id = extent_id_f->get_uint64(*i);

                // XXX need a way to get a clean page that can be preferentially released if it doesn't contain the row
                auto page = _cache->get(extent_id, this, true);
                auto &&j = page->begin();
                while (!found && j != page->end()) {
                    if (value->equal(FieldTuple(fields, *j))) {
                        page->remove(j);
                        found = true;
                        continue;
                    }
                    ++j;
                }

                if (!found) {
                    ++i;
                }
            }
        }

        void
        _update_direct(TuplePtr value, uint64_t xid, uint64_t extent_id)
        {
            // get the page from the cache
            auto page = _cache->get(extent_id, this);

            // find the row in the page
            // note: this can only be used when a primary key is present, otherwise update should have been split
            auto &&i = page->find(key);
            if (i == page->end()) {
                // XXX error
            }

            // set the row to the new values
            auto fields = _schema->get_fields();
            MutableTuple(fields, *i).assign(value);

            // release the page back to the write cache
            _cache->release(page);
        }

        void
        _update_by_lookup(TuplePtr key, uint64_t xid)
        {
            // we didn't receive an extent_id, but we have a primary index, so perform a lookup of the key
            auto i = _primary->lower_bound(key, xid);
            uint64_t extent_id = extent_id_f->get_uint64(*i);

            // then we can do a direct update
            _update_direct(key, xid, extent_id);
        }

    };
}
