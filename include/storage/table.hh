#pragma once

#include <common/object_cache.hh>

#include <storage/btree.hh>
#include <storage/data_cache.hh>
#include <storage/mutable_btree.hh>
#include <storage/schema_mgr.hh>

namespace springtail {

    /**
     * Read-only interface to a table at a fixed XID.  Provides interfaces for accessing table
     * information, performing scans, extent_id lookups, etc.
     */
    class Table : public std::enable_shared_from_this<Table> {
    public:
        /**
         * A forward iterator over the rows of a Table object.
         */
        class Iterator {
            friend Table;

        public:
            /** Copy constructor. */
            Iterator(const Iterator &i) = default;

            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_extent_i); }
            pointer operator->() { return &(*(_extent_i)); }

            /**
             * Move the iterator forward to the next row.
             */
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

            /**
             * Returns a new iterator at the next row.
             */
            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            /**
             * Compares two iterators for equality.
             */
            friend bool operator==(const Iterator& a, const Iterator& b) {
                return (a._btree_i == b._btree_i &&
                        (a._btree_i == a._btree->end() || a._extent_i == b._extent_i));
            }

            /**
             * Compares two iterators for inequality.
             */
            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }

        private:
            /** Specifically for the end() iterator. */
            Iterator(const Table *table, BTreePtr btree)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree->end()),
                  _extent(nullptr)
            { }

            /** For constructing an Iterator from the Table functions. */
            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i,
                     ExtentPtr extent, const Extent::Iterator &extent_i)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree_i),
                  _extent(extent),
                  _extent_i(extent_i)
            { }

        private:
            const Table *_table; ///< A pointer to the Table object this iterator is for.

            BTreePtr _btree;
            BTree::Iterator _btree_i;

            ExtentPtr _extent;
            Extent::Iterator _extent_i;
        };

    public:
        Table(uint64_t table_id,
              uint64_t xid,
              const std::filesystem::path &table_dir,
              const std::vector<std::string> &primary_key,
              const std::vector<std::vector<std::string>> &secondary_keys,
              std::vector<uint64_t> root_offsets,
              ExtentSchemaPtr schema,
              ExtentCachePtr cache);

        /** Returns true if the table has a primary key.  False otherwise. */
        bool has_primary();

        /** Finds the extent_id that may contain the provided key, using the primary key index. */
        uint64_t primary_lookup(TuplePtr tuple);

        /**
         * Retrieves the schema for the table at a given XID.
         */
        ExtentSchemaPtr extent_schema() const;

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        SchemaPtr schema(uint64_t extent_xid) const;

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key;
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
        Iterator lower_bound(TuplePtr search_key);

        /**
         * An iterator to the start of the table.
         */
        Iterator begin();

        /**
         * An iterator to the end of the table.
         */
        Iterator end()
        {
            return Iterator(this, _primary_index);
        }

        BTreePtr secondary(uint32_t idx) {
            return _secondary_indexes[idx];
        }

        ExtentPtr read_extent(uint64_t extent_id) const;

    protected:
        ExtentPtr _read_extent_via_primary(BTree::Iterator &pos) const;

        ExtentPtr _read_extent(uint64_t extent_id) const;

    private:
        /** The ID of the table. */
        uint64_t _id;

        uint64_t _xid;
        std::filesystem::path _table_dir;
        std::vector<std::string> _primary_key;
        std::vector<std::vector<std::string>> _secondary_keys;
        ExtentSchemaPtr _schema;
        ExtentCachePtr _cache;

        FieldArrayPtr _pkey_fields;
        FieldPtr _primary_extent_id_f;

        std::shared_ptr<IOHandle> _handle;

        /** The primary index of the table. */
        BTreePtr _primary_index;

        std::vector<BTreePtr> _secondary_indexes;

        ExtentSchemaPtr _roots_schema;
        FieldPtr _roots_root_f;
    };
    typedef std::shared_ptr<Table> TablePtr;

    /**
     * Interface for mutating a table at the most recent XID.
     */
    class MutableTable : public std::enable_shared_from_this<MutableTable> {
    public:
        MutableTable(uint64_t id,
                     uint64_t target_xid,
                     std::vector<uint64_t> root_offsets,
                     const std::filesystem::path &table_dir,
                     const std::vector<std::string> &primary_key,
                     const std::vector<std::vector<std::string>> &secondary_keys,
                     ExtentSchemaPtr schema,
                     DataCachePtr cache,
                     MutableBTree::PageCachePtr page_cache,
                     ExtentCachePtr read_cache);

        std::filesystem::path data_file() const {
            return _data_file;
        }

        uint64_t target_xid() const {
            return _target_xid;
        }

        /**
         * Add a row to the table.  The data extent ID is provided externally by the write cache, or
         * if the extent_id is UNKNOWN, then it will utilize the tuple data to determine where the
         * row should be added.
         */
        void insert(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Add a row to the table if it doesn't exist, otherwise update the existing row with the
         * new value.  The data extent ID is provided externally by the write cache, or if the
         * extent_id is UNKNOWN, then it will utilize the tuple data to determine where the row
         * should be added.
         */
        void upsert(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Remove a row from the table.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void remove(TuplePtr key, uint64_t xid, uint64_t extent_id);

        /**
         * Update a row in the table.  The value must contain the primary key to be updated and the
         * table must contain a primary key.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void update(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Remove the entries into the extent from the primary and secondary indexes.
         */
        void invalidate_indexes(uint64_t extent_id, ExtentPtr extent);

        /**
         * Add the entries in the extent into the primary and secondary indexes.
         */
        void populate_indexes(uint64_t extent_id, ExtentPtr extent);

        /**
         * Flush any dirty pages to disk and return the roots of the indexes to be updated in the
         * system tables.
         */
        std::vector<uint64_t> finalize();

        ExtentSchemaPtr schema() const {
            return _schema;
        }

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key;
        }

        uint64_t id() const {
            return _id;
        }

    private:
        void _insert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        void _insert_append(TuplePtr value, uint64_t xid);

        void _insert_by_lookup(TuplePtr value, uint64_t xid);

        void _upsert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        void _upsert_by_lookup(TuplePtr value, uint64_t xid);

        void _remove_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        void _remove_by_lookup(TuplePtr key, uint64_t xid);

        void _remove_by_scan(TuplePtr value, uint64_t xid);

        void _update_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        void _update_by_lookup(TuplePtr key, uint64_t xid);

    private:
        /** The ID of the table. */
        uint64_t _id;

        uint64_t _target_xid;
        std::filesystem::path _table_dir;
        std::filesystem::path _data_file;

        std::vector<std::string> _primary_key;
        std::vector<std::vector<std::string>> _secondary_keys;

        /** A lookup version of the primary index.  Pinned to the most recent XID. */
        BTreePtr _primary_lookup;
        FieldPtr _primary_extent_id_f;

        /** The primary index of the table. */
        MutableBTreePtr _primary_index;
        std::vector<MutableBTreePtr> _secondary_indexes;
        ExtentSchemaPtr _schema;

        DataCachePtr _cache;

        ExtentSchemaPtr _roots_schema;
        MutableFieldPtr _roots_root_f;
    };
    typedef std::shared_ptr<MutableTable> MutableTablePtr;

}
