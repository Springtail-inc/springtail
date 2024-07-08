#pragma once

#include <storage/btree.hh>
#include <storage/cache.hh>
#include <storage/mutable_btree.hh>
#include <storage/schema_mgr.hh>

namespace springtail {

    /**
     * Structure to hold table statistics.  Currently only holds the row count of the table.
     */
    struct TableStats {
        uint64_t row_count;

        TableStats()
            : row_count(0)
        { }
    };

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
            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() const { return *(_page_i); }
            pointer operator->() { return &(*(_page_i)); }

            /**
             * Move the iterator forward to the next row.
             */
            Iterator& operator++() {
                // move to the next row in the data extent
                ++_page_i;
                if (_page_i != _page->end()) {
                    return *this;
                }

                // no more rows in the extent, so need to move to the next data extent
                ++_btree_i;
                if (_btree_i == _btree->end()) {
                    return *this;
                }
                
                // retrieve the data extent
                _page = _table->_read_page_via_primary(_btree_i);
                _page_i = _page->begin();

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
                        (a._btree_i == a._btree->end() || a._page_i == b._page_i));
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
                  _page(nullptr)
            { }

            /** For constructing an Iterator from the Table functions. */
            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i,
                     StorageCache::PagePtr page,
                     const StorageCache::Page::Iterator &page_i)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree_i),
                  _page(page),
                  _page_i(page_i)
            { }

        private:
            const Table *_table; ///< A pointer to the Table object this iterator is for.

            BTreePtr _btree; ///< A pointer to the BTree of the primary index.
            BTree::Iterator _btree_i; ///< An iterator into the BTree.

            StorageCache::PagePtr _page; ///< A pointer to the data page currently being processed.
            StorageCache::Page::Iterator _page_i; ///< An iterator into the Extent.
        };

    public:
        /**
         * Table constructor.
         */
        Table(uint64_t table_id,
              uint64_t xid,
              const std::filesystem::path &table_dir,
              const std::vector<std::string> &primary_key,
              const std::vector<std::vector<std::string>> &secondary_keys,
              std::vector<uint64_t> root_offsets,
              ExtentSchemaPtr schema,
              const TableStats &stats);

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
         * Returns an iterator to the first row that is less than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator inverse_lower_bound(TuplePtr search_key);

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

        /**
         * Returns the requested index BTree of the table based on the index ID in the "indexes" table.
         * @param idx The id of the index to retrieve.  Note that 0 is the primary index.
         * @return A BTree object of the requested index.
         */
        BTreePtr index(uint32_t idx) {
            if (idx == 0) {
                return _primary_index;
            }
            return _secondary_indexes[idx - 1];
        }

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::PagePtr read_page(uint64_t extent_id) const;

    protected:
        /**
         * Reads a data extent using the provided iterator position within the primary index.
         * @param pos The primary index btree iterator.
         * @return A pointer to the requested page.
         */
        StorageCache::PagePtr _read_page_via_primary(BTree::Iterator &pos) const;

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::PagePtr _read_page(uint64_t extent_id) const;

    private:
        /** The ID of the table. */
        uint64_t _id;

        uint64_t _xid; ///< The XID at which this table is being accessed.
        std::filesystem::path _table_dir; ///< The directory holding the table data.
        std::vector<std::string> _primary_key; ///< The primary index key columns.
        std::vector<std::vector<std::string>> _secondary_keys; ///< The key columns for each secondary index.
        ExtentSchemaPtr _schema; ///< The schema of the data extents for this table.

        FieldArrayPtr _pkey_fields; ///< The field accessors for the primary index key columns within the primary index extents.
        FieldPtr _primary_extent_id_f; ///< The field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        BTreePtr _primary_index;

        std::vector<BTreePtr> _secondary_indexes; ///< The secondary indexes of the table.

        ExtentSchemaPtr _roots_schema; ///< The schema of the "roots" file.
        FieldPtr _roots_root_f; ///< The field accessor to read the root extent ID from each row in the "roots" file.

        TableStats _stats; ///< The statistics for this table.
    };
    typedef std::shared_ptr<Table> TablePtr;

    /**
     * Interface for mutating a table at the most recent XID.
     */
    class MutableTable : public std::enable_shared_from_this<MutableTable> {
    public:
        /**
         * Mutable table constructor.
         */
        MutableTable(uint64_t id,
                     uint64_t access_xid,
                     uint64_t target_xid,
                     std::vector<uint64_t> root_offsets,
                     const std::filesystem::path &table_dir,
                     const std::vector<std::string> &primary_key,
                     const std::vector<std::vector<std::string>> &secondary_keys,
                     ExtentSchemaPtr schema,
                     const TableStats &stats,
                     bool for_gc = false);

        /**
         * Returns the file of the raw data associated with the table.
         */
        std::filesystem::path data_file() const {
            return _data_file;
        }

        /**
         * Returns the target XID of this table.
         */
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


        void evict_handler(StorageCache::PagePtr page);

        /**
         * Flush any dirty pages to disk and return the roots of the indexes to be updated in the
         * system tables.
         */
        std::vector<uint64_t> finalize();

        /**
         * Returns the schema of the table.
         */
        ExtentSchemaPtr schema() const {
            return _schema;
        }

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key;
        }

        /**
         * Returns the table ID of this table.
         */
        uint64_t id() const {
            return _id;
        }

    private:
        /**
         * Page callback on evict() / flush_file() that will perform an _invalidate_indexes() and
         * _flush_and_populate_indexes() on the provided page.
         */
        bool _flush_handler(StorageCache::PagePtr page);

        /**
         * Remove the rows in the page from the primary and secondary indexes.
         */
        void _invalidate_indexes(StorageCache::PagePtr page);

        /**
         * Flush the page and add it's rows into the primary and secondary indexes.
         */
        void _flush_and_populate_indexes(StorageCache::PagePtr page);


        /**
         * Inserts a tuple directly into the provided extent at the given XID.
         */
        void _insert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Inserts a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        void _insert_empty(TuplePtr value, uint64_t xid);

        /**
         * Inserts a tuple at the end of the last extent of the table at the given XID.
         */
        void _insert_append(TuplePtr value, uint64_t xid);

        /**
         * Inserts a tuple at the given XID into the extent returned by a primary key lookup using
         * the tuple.
         */
        void _insert_by_lookup(TuplePtr value, uint64_t xid);

        /**
         * Either inserts a tuple directly into the provided extent at the given XID, or updates an
         * existing row with the same primary key value.
         */
        bool _upsert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Upserts a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        bool _upsert_empty(TuplePtr value, uint64_t xid);

        /**
         * Inserts a tuple at the given XID, or updates an existing tuple with the same primary key
         * value, using a primary key lookup to find the containing extent.
         */
        bool _upsert_by_lookup(TuplePtr value, uint64_t xid);

        /**
         * Removes a tuple from the provided extent at the given XID that has the same primary key
         * value.
         */
        void _remove_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Removes a row from the "empty" Page object.  Used when the Table started empty.
         */
        void _remove_empty(TuplePtr value, uint64_t xid);

        /**
         * Removes a tuple at the given XID that has the same primary key value by using a primary
         * key lookup to find the containing extent.
         */
        void _remove_by_lookup(TuplePtr key, uint64_t xid);

        /**
         * Removes a tuple at the given XID that has the same primary key value by using a table
         * scan to find the containing extent.
         */
        void _remove_by_scan(TuplePtr value, uint64_t xid);

        /**
         * Updates an existing row with the matching primary key value in the provided extent at the
         * given XID.
         */
        void _update_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Updates a row in the "empty" Page object.  Used when the Table started empty.
         */
        void _update_empty(TuplePtr value, uint64_t xid);

        /**
         * Updates an existing row with the matching primary key value at the given XID, using a
         * primary key lookup to find the containing extent.
         */
        void _update_by_lookup(TuplePtr key, uint64_t xid);

    private:
        /** The ID of the table. */
        uint64_t _id;

        uint64_t _access_xid; ///< The access XID for this set of mutations.
        uint64_t _target_xid; ///< The final target XID for this set of mutations.
        std::filesystem::path _table_dir; ///< The directory containing the table data.
        std::filesystem::path _data_file; ///< The file containing the table data extents.

        std::vector<std::string> _primary_key; ///< The key columns of the primary index.
        std::vector<std::vector<std::string>> _secondary_keys; ///< The key columns of each secondary index.

        /** A lookup version of the primary index.  Pinned to the most recent XID. */
        BTreePtr _primary_lookup;
        FieldPtr _primary_extent_id_f; ///< A field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        MutableBTreePtr _primary_index; ///< The mutable primary index btree.
        std::vector<MutableBTreePtr> _secondary_indexes; ///< The mutable secondary index btrees.
        ExtentSchemaPtr _schema; ///< The schema of the data extents of the table.

        ExtentSchemaPtr _roots_schema; ///< The schema of the "roots" file.
        MutableFieldPtr _roots_root_f; ///< The field accessor for the tree roots stored within each row of the "roots" file.

        StorageCache::PagePtr _empty_page; ///< Used to handle the empty table corner-case.
        TableStats _stats; ///< The stats for the table.

        bool _for_gc; ///< If this table is being used for the ingest pipeline.
    };
    typedef std::shared_ptr<MutableTable> MutableTablePtr;

}
