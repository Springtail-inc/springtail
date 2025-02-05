#pragma once

#include "common/constants.hh"
#include <memory>
#include <stdexcept>
#include <storage/btree.hh>
#include <storage/cache.hh>
#include <storage/mutable_btree.hh>

#include <sys_tbl_mgr/schema_mgr.hh>
#include <variant>

namespace springtail {

namespace table_helpers {

/** Constructs the full directory path for a table given the parameters. */
std::filesystem::path get_table_dir(const std::filesystem::path &base, uint64_t db_id,
                                    uint64_t table_id, uint64_t snapshot_xid);

} // namespace table_helpers

    /**
     * Structure to hold table statistics.  Currently only holds the row count of the table.
     */
    struct TableStats {
        uint64_t row_count = 0;
    };

    /**
     * Structure to hold table roots.
     */
    struct TableRoot {
        uint64_t index_id = 0;
        uint64_t extent_id = 0;
    };

    /**
     * Structure to hold table metadata required to record a table snapshot.
     */
    struct TableMetadata {
        std::vector<TableRoot> roots;
        TableStats stats;
        uint64_t snapshot_xid = 0;
    };
    using TableMetadataPtr = std::shared_ptr<TableMetadata>;

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

            /** 
             * We use the same Iterator type for both primary and secondary indexes.
             * However the way the indexes move around isn't the same.
             * Tracker provides an abstraction for the various index types.
             */
            struct Tracker
            {
                explicit Tracker(const Table *table)
                : _table(table)
                {}

                Tracker(const Table *table,
                         BTreePtr btree, const BTree::Iterator &btree_i)
                : _table(table),
                  _btree(btree),
                  _btree_i(btree_i)
                {}

                friend bool operator==(const Tracker& a, const Tracker& b) {
                    CHECK_EQ(a._table, b._table);
                    if (a._btree == nullptr && b._btree == nullptr) {
                        return true;
                    } else if (a._btree == nullptr || b._btree == nullptr) {
                        return false;
                    }
                    return (a._btree_i == b._btree_i);
                }

                virtual void next() = 0;
                virtual void prev() = 0;
                virtual const Extent::Row & row() const = 0;

                const Table *_table{}; ///< A pointer to the Table object this iterator is for.
                BTreePtr _btree; ///< A pointer to the BTree of the primary index.
                BTree::Iterator _btree_i; ///< An iterator into the BTree.
            };

            /**
             * This is to iterate using the primary index.
             */
            struct Primary : Tracker
            {
                Primary(const Table *table,
                        BTreePtr btree, const BTree::Iterator &btree_i,
                        StorageCache::SafePagePtr page,
                        const StorageCache::Page::Iterator &page_i )
                    : Tracker{table, btree, btree_i},
                    _page(std::move(page)),
                    _page_i(page_i)
                {}

                explicit Primary(const Table *table) 
                    :Tracker{table}
                {}

                Primary(Primary&&) noexcept = default;
                virtual ~Primary() = default;

                void next() override;
                void prev() override;

                const Extent::Row& row() const override 
                {
                    return *_page_i;
                }

                friend bool operator==(const Primary& a, const Primary& b) {
                    const Tracker& ta = a;
                    const Tracker& tb = b;
                    
                    if (ta == tb) {
                        return (a._btree_i == a._btree->end() || a._page_i == b._page_i);
                    }
                    return false;
                }

                StorageCache::SafePagePtr _page; ///< A pointer to the data page currently being processed.
                StorageCache::Page::Iterator _page_i; ///< An iterator into the Extent.
            };

            /**
             * This is to iterate using the secondary index.
             */
            struct Secondary : Tracker
            {
                Secondary(const Table *table,
                        BTreePtr btree, const BTree::Iterator &btree_i,
                        ExtentSchemaPtr schema )
                    : Tracker{table, btree, btree_i}
                {
                    _extent_id_f = schema->get_field(constant::INDEX_EID_FIELD);
                    _row_id_f = schema->get_field(constant::INDEX_RID_FIELD);
                    if (_btree_i != btree->end()) {
                        update_page();
                    }
                }

                Secondary(Secondary&&) = default;
                virtual ~Secondary() = default;

                void next() override;
                void prev() override;
                const Extent::Row& row() const override;

                friend bool operator==(const Secondary& a, const Secondary& b) {
                    const Tracker& ta = a;
                    const Tracker& tb = b;
                    return ta == tb;
                }

                FieldPtr _extent_id_f;
                FieldPtr _row_id_f;

                uint64_t _extent_id = 0;
                StorageCache::SafePagePtr _page;
                StorageCache::Page::Iterator _page_i;

                void update_page();
            };

            std::variant<std::monostate, Primary, Secondary> _tracker;

        public:
            using iterator_category = std::bidirectional_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Extent::Row;
            using pointer           = const Extent::Row *;  // or also value_type*
            using reference         = const Extent::Row &;  // or also value_type&

            reference operator*() { 
                return tracker().row();
            }
            pointer operator->() { return &*(*this); }

            /**
             * Move the iterator forward to the next row.
             */
            Iterator& operator++() {
                tracker().next();
                return *this;
            }

            /**
             * Move the iterator backward to the previous row.
             */
            Iterator& operator--() {
                tracker().prev();
                return *this;
            }

            /**
             * Compares two iterators for equality.
             */
            friend bool operator==(const Iterator& a, const Iterator& b) {
                if (auto pa = std::get_if<Primary>(&a._tracker)) {
                    auto pb =  std::get_if<Primary>(&b._tracker);
                    assert(pb);
                    return *pa == *pb;
                } else if (auto pa = std::get_if<Secondary>(&a._tracker)) {
                    auto pb =  std::get_if<Secondary>(&b._tracker);
                    assert(pb);
                    return *pa == *pb;
                }
                assert(false);
                return false;
            }

            /**
             * Compares two iterators for inequality.
             */
            friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }


            /** This will return the current extent id of the iterator.
            */
            uint64_t extent_id() const {
                if (auto p = std::get_if<Primary>(&_tracker)) {
                    return p->_page_i.extent_id();
                }
                throw std::runtime_error("Unsupported for secondary indexes");
            }

        private:
            /** Specifically for the end() iterator of a vacant table. */
            Iterator(const Table *table)
            { 
                _tracker.emplace<Primary>(table, 
                        BTreePtr{}, 
                        BTree::Iterator{}, 
                        StorageCache::SafePagePtr{}, 
                        StorageCache::Page::Iterator{});
            }

            /** Specifically for the end() iterator. */
            Iterator(const Table *table, uint32_t index_id);

            /** For constructing an Iterator from the Table functions. */
            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i,
                     StorageCache::SafePagePtr page,
                     const StorageCache::Page::Iterator &page_i)
            { 
                _tracker.emplace<Primary>(table, btree, btree_i, std::move(page), page_i);
            }

            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i,
                     ExtentSchemaPtr index_schema)
            { 
                _tracker.emplace<Secondary>(table, btree, btree_i, index_schema);
            }

            Tracker& tracker() 
            {
                if (auto p = std::get_if<Primary>(&_tracker)) {
                    return *p;
                } else if (auto p = std::get_if<Secondary>(&_tracker)) {
                    return *p;
                } else {
                    assert(false);
                }
                throw std::runtime_error("Bad iterator tracker");
            }
        };

    public:
        /**
         * Table constructor.
         */
        Table(uint64_t db_id,
              uint64_t table_id,
              uint64_t xid,
              const std::filesystem::path &table_base,
              const std::vector<std::string> &primary_key,
              const std::vector<Index> &secondary,
              const TableMetadata &metadata,
              ExtentSchemaPtr schema);

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

        /** Retrieve the Database ID of this table. */
        uint64_t db() const
        {
            return _db_id;
        }

        /** Retrieve the ID of this table. */
        uint64_t id() const
        {
            return _id;
        }

        /** This will convert column positions to column names based on the table schema
         */
        std::vector<std::string> get_column_names(const std::vector<uint32_t>& col_position);

        /**
         * Returns an iterator to the first row that is greater than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator lower_bound(TuplePtr search_key, uint32_t index_id = constant::INDEX_PRIMARY);

        Iterator upper_bound(TuplePtr search_key, uint32_t index_id = constant::INDEX_PRIMARY);

        /**
         * Returns an iterator to the first row that is less than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator inverse_lower_bound(TuplePtr search_key);

        /**
         * An iterator to the start of the table.
         */
        Iterator begin(uint32_t index_id = constant::INDEX_PRIMARY);

        /**
         * An iterator to the end of the table.
         */
        Iterator end(uint32_t index_id = constant::INDEX_PRIMARY)
        {
            // check for vacant table
            if (index_id == constant::INDEX_PRIMARY && _primary_index == nullptr) {
                return Iterator(this);
            }
            return Iterator(this, index_id);
        }

        /**
         * Returns the requested index BTree of the table based on the index ID in the "indexes" table.
         * @param idx The id of the index to retrieve.  Note that 0 is the primary index.
         * @return A BTree object of the requested index.
         */
        BTreePtr index(uint32_t idx) const {
            if (idx == 0) {
                return _primary_index;
            }
            return _secondary_indexes.at(idx).first;
        }

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr read_page(uint64_t extent_id) const;

        /**
         * @brief Get table stats
         * @return TableStats
         */
        TableStats get_stats() const {
            return _stats;
        }

    protected:
        /**
         * Reads a data extent using the provided iterator position within the primary index.
         * @param pos The primary index btree iterator.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr _read_page_via_primary(BTree::Iterator &pos) const;

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr _read_page(uint64_t extent_id) const;

        /**
         * Creates read-only index of the table.
         */
        BTreePtr 
        _create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, uint64_t offset);

    private:
        uint64_t _db_id; ///< The ID of the database containing this table.
        uint64_t _id; ///< The ID of the table.

        uint64_t _xid; ///< The XID at which this table is being accessed.
        std::filesystem::path _table_dir; ///< The directory holding the table data.
        std::vector<std::string> _primary_key; ///< The primary index key columns.
        ExtentSchemaPtr _schema; ///< The schema of the data extents for this table.

        FieldArrayPtr _pkey_fields; ///< The field accessors for the primary index key columns within the primary index extents.
        FieldPtr _primary_extent_id_f; ///< The field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        BTreePtr _primary_index;


        /** A map of secondary indexes
         * first is the index id
         * second.first is btree
         * second.second are the index columns
         */
        std::map<uint64_t, std::pair<BTreePtr, std::vector<uint32_t>>> _secondary_indexes; ///< The secondary indexes of the table..

        ExtentSchemaPtr _roots_schema; ///< The schema of the "roots" file.
        FieldPtr _roots_root_f; ///< The field accessor to read the root extent ID from each row in the "roots" file.
        FieldPtr _roots_index_id_f; ///< The field accessor to read the root index ID from each row in the "roots" file.

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
        MutableTable(uint64_t db_id,
                     uint64_t table_id,
                     uint64_t access_xid,
                     uint64_t target_xid,
                     const std::filesystem::path &table_base,
                     const std::vector<std::string> &primary_key,
                     const std::vector<Index> &secondary,
                     const TableMetadata &metadata,
                     ExtentSchemaPtr schema,
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

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        void truncate();

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr read_page(uint64_t extent_id);

        /**
         * Flush any dirty pages to disk and return the roots of the indexes to be updated in the
         * system tables.
         */
        TableMetadata finalize();

        /**
         * Returns the schema of the table.
         */
        ExtentSchemaPtr schema() const {
            return _schema;
        }

        /** Returns true if the table has a primary key.  False otherwise. */
        bool has_primary() const
        {
            return !_primary_key.empty();
        }

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key;
        }

        /** Retrieve the Database ID of this table. */
        uint64_t db() const
        {
            return _db_id;
        }

        /**
         * Returns the table ID of this table.
         */
        uint64_t id() const {
            return _id;
        }

        /** Create a btree that can be used for indexes.
         * @param index_id PG index ID.
         * @param index_columns Positions of the index columns.
         */
        MutableBTreePtr create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns);

        /**
         * Returns the requested index BTree of the table based on the index ID in the "indexes" table.
         * @param idx The id of the index to retrieve.  Note that 0 is the primary index.
         * @return A BTree object of the requested index.
         */
        MutableBTreePtr index(uint32_t idx) {
            if (idx == 0) {
                return _primary_index;
            }
            return _secondary_indexes.at(idx).first;
        }

        /** This will convert column positions to column names based on the table schema
         */
        std::vector<std::string> get_column_names(const std::vector<uint32_t>& col_position);

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
        void _flush_and_populate_indexes(StorageCache::PagePtr::element_type* page);


        /**
         * Inserts a tuple directly into the provided extent at the given XID.
         */
        void _insert_direct(TuplePtr value, uint64_t xid, uint64_t extent_id);

        /**
         * Inserts a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        void _insert_empty(TuplePtr value, uint64_t xid);

        /**
         * Appends a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        void _append_empty(TuplePtr value, uint64_t xid);

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

        /**
         * Convert the schema of the page, if needed based on the target XID.
         */
        void _check_convert_page(StorageCache::SafePagePtr &page);

    private:
        uint64_t _db_id; ///< The ID of the database containing this table.
        uint64_t _id; ///< The ID of the table.

        uint64_t _access_xid; ///< The access XID for this set of mutations.
        uint64_t _target_xid; ///< The final target XID for this set of mutations.
        uint64_t _snapshot_xid; ///< The XID of the snapshot that this version of the table started from.
        std::filesystem::path _table_dir; ///< The directory containing the table data.
        std::filesystem::path _data_file; ///< The file containing the table data extents.

        std::vector<std::string> _primary_key; ///< The key columns of the primary index.

        /** A lookup version of the primary index.  Pinned to the most recent XID. */
        BTreePtr _primary_lookup;
        FieldPtr _primary_extent_id_f; ///< A field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        MutableBTreePtr _primary_index; ///< The mutable primary index btree.
        /** A map of secondary indexes
         * first is the index id
         * second.first is btree
         * second.second are the index columns
         */
        std::map<uint64_t, std::pair<MutableBTreePtr, std::vector<uint32_t>>> _secondary_indexes; ///< The mutable secondary index btrees.
        ExtentSchemaPtr _schema; ///< The schema of the data extents of the table.

        ExtentSchemaPtr _roots_schema; ///< The schema of the "roots" file.
        MutableFieldPtr _roots_root_f; ///< The field accessor for the tree roots stored within each row of the "roots" file.
        MutableFieldPtr _roots_index_id_f; ///< The field accessor for the tree roots index ids stored within each row of the "roots" file.

        std::unique_ptr<StorageCache::SafePagePtr> _empty_page; ///< Used to handle the empty table corner-case.
        TableStats _stats; ///< The stats for the table.

        bool _for_gc; ///< If this table is being used for the ingest pipeline.
                      ///
    };
    typedef std::shared_ptr<MutableTable> MutableTablePtr;


}
