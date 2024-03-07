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
              std::shared_ptr<ExtentSchema> schema,
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

}
