#pragma once

#include <common/constants.hh>
#include <common/circular_buffer.hh>
#include <algorithm>
#include <memory>
#include <storage/btree.hh>
#include <storage/cache.hh>
#include <storage/mutable_btree.hh>

#include <thread>
#include <variant>

namespace springtail {

    /**
     * Alias to a map holding secondary indexes :: index_id => (root, index_cols)
     */
    using SecondaryIndexesCache = std::map<uint64_t, std::pair<MutableBTreePtr, std::vector<uint32_t>>>;

namespace table_helpers {

/** Constructs the full directory path for a table given the parameters. */
std::filesystem::path get_table_dir(const std::filesystem::path &base, uint64_t db_id,
                                    uint64_t table_id, uint64_t snapshot_xid);

} // namespace table_helpers

namespace indexer_helpers {
    /**
     * @brief Remove secondary-index entries for every row in a page.
     *
     * @param extent_id  Extent ID
     * @param page       SafePagePtr whose rows should be removed from index.
     * @param root       Mutable B-tree root of the secondary index.
     * @param idx_cols   index columns
     * @param schema     ExtentSchemaPtr to fetch schema columns
     */
    void invalidate_index_for_page(uint64_t extent_id, const StorageCache::SafePagePtr &page,
            const MutableBTreePtr &root, const std::vector<std::string> &idx_cols, const ExtentSchemaPtr &schema);

    /**
     * @brief Remove secondary-index entries for every row in an extent.
     *
     * @param extent_id  Extent ID
     * @param extent     Extent whose rows should be removed.
     * @param root       Mutable B-tree root of the secondary index.
     * @param idx_cols   index columns
     * @param schema     ExtentSchemaPtr to fetch schema columns
     */
    void invalidate_index_for_extent(uint64_t extent_id, const std::shared_ptr<Extent> &extent,
            const MutableBTreePtr &root, const std::vector<std::string> &idx_cols, const ExtentSchemaPtr &schema);

    /**
     * @brief Insert secondary-index entries for every row in a page.
     *
     * @param extent_id  Extent ID
     * @param page       SafePagePtr whose rows should be indexed.
     * @param root       Mutable B-tree root of the secondary index.
     * @param idx_cols   index columns
     * @param schema     ExtentSchemaPtr to fetch schema columns
     */
    void populate_index_for_page(uint64_t extent_id, const StorageCache::SafePagePtr &page,
            const MutableBTreePtr &root, const std::vector<std::string> &idx_cols, const ExtentSchemaPtr &schema);

    /**
     * @brief Insert secondary-index entries for every row in an extent.
     *
     * @param extent_id  Extent ID
     * @param extent     Extent whose rows should be indexed.
     * @param root       Mutable B-tree root of the secondary index.
     * @param idx_cols   index columns
     * @param schema     ExtentSchemaPtr to fetch schema columns
     */
    void populate_index_for_extent(uint64_t extent_id, const std::shared_ptr<Extent> &extent,
            const MutableBTreePtr &root, const std::vector<std::string> &idx_cols, const ExtentSchemaPtr &schema);
} // namespace indexer_helpers

    /**
     * Structure to hold table statistics,
     * currently holds the row count of the table
     * and end offset of table data file post XID disk flush
     */
    struct TableStats {
        uint64_t row_count = 0;
        uint64_t end_offset = 0;
        uint64_t last_internal_row_id = 0;
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
    class Table {
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
                    DCHECK_EQ(a._table, b._table);
                    if (a._btree == nullptr && b._btree == nullptr) {
                        return true;
                    } else if (a._btree == nullptr || b._btree == nullptr) {
                        return false;
                    }
                    return (a._btree_i == b._btree_i);
                }

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

                void next();
                void prev();

                const Extent::Row& row() const {
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
            std::optional<StorageCache::Page::Iterator> _end;
        };

        /**
         * This is to iterate using the secondary index.
         */
        struct Secondary : Tracker
        {
            Secondary(const Table *table,
                    BTreePtr btree, const BTree::Iterator &btree_i,
                    ExtentSchemaPtr schema );

            Secondary(Secondary&&) = default;
            virtual ~Secondary() = default;

            void next();
            void prev();
            const Extent::Row& row() const {
                return *_page_i;
            }

            friend bool operator==(const Secondary& a, const Secondary& b) {
                const Tracker& ta = a;
                const Tracker& tb = b;
                return ta == tb;
            }

            FieldPtr _extent_id_f;
            FieldPtr _row_id_f;
            FieldPtr _internal_row_id_f;
            FieldArrayPtr _look_aside_key_fields;

            StorageCache::Page::Iterator _page_i;

            void update_page();
        };

        /**
         * This is to iterate using the secondary index. It is different from
         * the Secondary type in so that it doesn't provide access to full data rows but
         * to the index values only.
         */
        struct SecondaryIndexOnly : Tracker
        {
            SecondaryIndexOnly(const Table *table,
                    BTreePtr btree, const BTree::Iterator &btree_i);

            SecondaryIndexOnly(SecondaryIndexOnly&&) = default;
            virtual ~SecondaryIndexOnly() = default;

            void next() {
                ++_btree_i;
            }
            void prev() {
                --_btree_i;
            }
            const Extent::Row& row() const {
                return *_btree_i;
            }

            friend bool operator==(const SecondaryIndexOnly& a, const SecondaryIndexOnly& b) {
                const Tracker& ta = a;
                const Tracker& tb = b;
                return ta == tb;
            }
        };

        std::variant<std::monostate, Primary, Secondary, SecondaryIndexOnly> _tracker;

    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = const Extent::Row;
        using pointer           = const Extent::Row *;  // or also value_type*
        using reference         = const Extent::Row &;  // or also value_type&

        reference operator*() {

            struct visitor {
                reference operator()(const Primary& t) const {
                    return t.row();
                }
                reference operator()(const SecondaryIndexOnly& t) const {
                    return t.row();
                }
                reference operator()(const Secondary& t) const {
                    return t.row();
                }
                reference operator() [[noreturn]] (const std::monostate&) const {
                    CHECK(false);
                }
            };

            return std::visit<reference>(visitor{}, _tracker);
        }

        pointer operator->() { return &*(*this); }

        /**
         * Move the iterator forward to the next row.
         */
        Iterator& operator++() {
            struct visitor {
                void operator()(Primary& t) const {
                    t.next();
                }
                void operator()(SecondaryIndexOnly& t) const {
                    t.next();
                }
                void operator()(Secondary& t) const {
                    t.next();
                }
                void operator()(const std::monostate&) const {
                    CHECK(false);
                }
            };
            std::visit(visitor{}, _tracker);
            return *this;
        }

        /**
         * Move the iterator backward to the previous row.
         */
        Iterator& operator--() {
            struct visitor {
                void operator()(Primary& t) const {
                    t.prev();
                }
                void operator()(SecondaryIndexOnly& t) const {
                    t.prev();
                }
                void operator()(Secondary& t) const {
                    t.prev();
                }
                void operator()(const std::monostate&) const {
                    CHECK(false);
                }
            };
            std::visit(visitor{}, _tracker);
            return *this;
        }

        /**
         * Compares two iterators for equality.
         */
        friend bool operator==(const Iterator& a, const Iterator& b) {
            struct visitor {
                const Iterator& _b;
                explicit visitor(const Iterator&b) : _b{b} {}
                bool operator()(const Primary& t) const {
                    return t == std::get<Primary>(_b._tracker);
                }
                bool operator()(const SecondaryIndexOnly& t) const {
                    return t == std::get<SecondaryIndexOnly>(_b._tracker);
                }
                bool operator()(const Secondary& t) const {
                    return t == std::get<Secondary>(_b._tracker);
                }
                bool operator() [[noreturn]] (const std::monostate&) const {
                    CHECK(false);
                }
            };
            return std::visit<bool>(visitor{b}, a._tracker);
        }

        /**
         * Compares two iterators for inequality.
         */
        friend bool operator!= (const Iterator& a, const Iterator& b) { return !(a == b); }


        /** This will return the current extent id of the iterator.
        */
        uint64_t extent_id() const {
            struct visitor {
                uint64_t operator()(const Primary& t) const {
                    return t._page_i.extent_id();
                }
                uint64_t operator() [[noreturn]] (const SecondaryIndexOnly&) const {
                    CHECK(false);
                }
                uint64_t operator() [[noreturn]] (const Secondary&) const {
                    CHECK(false);
                }
                uint64_t operator() [[noreturn]] (const std::monostate&) const {
                    CHECK(false);
                }
            };
            return std::visit<uint64_t>(visitor{}, _tracker);
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
            Iterator(const Table *table, uint32_t index_id, bool index_only);

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
                     ExtentSchemaPtr index_schema )
            {
                _tracker.emplace<Secondary>(table, btree, btree_i, index_schema);
            }

            Iterator(const Table *table,
                     BTreePtr btree, const BTree::Iterator &btree_i)
            {
                _tracker.emplace<SecondaryIndexOnly>(table, btree, btree_i);
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
            ExtentSchemaPtr schema,
            const ExtensionCallback &extension_callback = {});

        /** Returns true if the table has a primary key.  False otherwise. */
        bool has_primary();

        /** Finds the extent_id that may contain the provided key, using the primary key index. */
        uint64_t primary_lookup(TuplePtr tuple);

        /**
         * Retrieves the schema for the table at a given XID.
         */
        virtual ExtentSchemaPtr extent_schema() const { return nullptr; }

        /**
         * Get a schema for accessing an extent from this table that was written at the provided XID.
         */
        virtual SchemaPtr schema(uint64_t extent_xid) const { return nullptr; }

        /** Retrieves the ordered set of columns that form the primary key. */
        std::vector<std::string> primary_key() const
        {
            return _primary_key;
        }

        /** Retrieve the Database ID of this table. */
        uint64_t db() const { return _db_id; }

        /** Retrieve the ID of this table. */
        uint64_t id() const { return _id; }

        bool empty() const;

        /**
         * Returns an iterator to the first row that is greater than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator lower_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

        Iterator upper_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

        /**
         * Returns an iterator to the first row that is less than or equal to the provided search
         * key.  Search key must match the primary index order.
         */
        Iterator inverse_lower_bound(TuplePtr search_key, uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

        /**
         * An iterator to the start of the table.
         */
        Iterator begin(uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false);

        /**
         * An iterator to the end of the table.
         */
        Iterator end(uint64_t index_id = constant::INDEX_PRIMARY, bool index_only = false)
        {
            // check for vacant table
            if (index_id == constant::INDEX_PRIMARY && _primary_index == nullptr) {
                return Iterator(this);
            }
            return Iterator(this, index_id, index_only);
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
         * Get the index schema.
         */
        ExtentSchemaPtr get_index_schema(uint64_t index_id) const;

        /**
         * Get the secondary index column names in the order as they appear in the index.
         */
        std::vector<std::string> get_index_column_names(uint64_t index_id) const;

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr read_page(uint64_t extent_id) const;

        /**
         * Reads an extent from the disk and returns it.
         * @param extent_id The extent ID to read.
         * @return std::pair where:
         *         - first -  std::shared_ptr<Extent> pointer to the retrieved extent
         *         - second - uint64_t Next offset
         */
        std::pair<std::shared_ptr<Extent>, uint64_t> read_extent_from_disk(uint64_t extent_id) const;

        /**
         * @brief Get table stats
         * @return TableStats
         */
        TableStats get_stats() const {
            return _stats;
        }

        /**
         * Returns the schema of the table.
         */
        ExtentSchemaPtr schema() const {
            return _schema;
        }

        /**
         * Returns the look aside schema of the table.
         */
        ExtentSchemaPtr look_aside_schema() const {
            return _look_aside_schema;
        }

        /**
         * Returns the look aside index of the table.
         */
        BTreePtr look_aside_index() const {
            return _look_aside_index;
        }

        std::filesystem::path get_dir_path() const
        {
            return _table_dir;
        }

        uint64_t get_xid() const
        {
            return _xid;
        }

        /**
         * @brief Get a vectore containing the list of secondary index ids for
         *      this table
         *
         * @return std::vector<uint64_t> - list of index ids
         */
        std::vector<uint64_t> get_secondary_idx_ids() const
        {
            std::vector<uint64_t> index_ids;
            index_ids.reserve(_secondary_indexes.size());
            for (auto &it: _secondary_indexes) {
                index_ids.push_back(it.first);
            }
            return index_ids;
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
        _create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, uint64_t offset, const ExtensionCallback &extension_callback = {});

    protected:
        uint64_t _db_id; ///< The ID of the database containing this table.
        uint64_t _id; ///< The ID of the table.

        uint64_t _xid; ///< The XID at which this table is being accessed.
        std::filesystem::path _table_dir; ///< The directory holding the table data.
        std::vector<std::string> _primary_key; ///< The primary index key columns.
        ExtentSchemaPtr _schema; ///< The schema of the data extents for this table.
        ExtentSchemaPtr _look_aside_schema; ///< The schema of the look aside index for this table.

        FieldArrayPtr _pkey_fields; ///< The field accessors for the primary index key columns within the primary index extents.
        FieldPtr _primary_extent_id_f; ///< The field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        BTreePtr _primary_index;

        /** Look aside index **/
        BTreePtr _look_aside_index;


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
        ExtensionCallback _extension_callback; ///< The extension callback for this table.
    };
    typedef std::shared_ptr<Table> TablePtr;

    /**
     * Interface for mutating a table at the most recent XID.
     */
    class MutableTable {
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
                     ExtentSchemaPtr schema_without_row_id = nullptr,
                     const ExtensionCallback &extension_callback = {},
                     const OpClassHandler &opclass_handler = {});

        ~MutableTable() {
            // if we have a dirty, empty page, then evict it
            if (_empty_page) {
                _empty_page->evict();
            }
        }

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
        void insert(TuplePtr value, uint64_t extent_id);

        /**
         * Add a row to the table if it doesn't exist, otherwise update the existing row with the
         * new value.  The data extent ID is provided externally by the write cache, or if the
         * extent_id is UNKNOWN, then it will utilize the tuple data to determine where the row
         * should be added.
         */
        void upsert(TuplePtr value, uint64_t extent_id);

        /**
         * Remove a row from the table.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void remove(TuplePtr key, uint64_t extent_id);

        /**
         * Update a row in the table.  The value must contain the primary key to be updated and the
         * table must contain a primary key.  The data extent ID is provided externally by the write
         * cache, or if the extent_id is UNKNOWN, then it will utilize the tuple data to identify a
         * row to remove.
         */
        void update(TuplePtr value, uint64_t extent_id);

        /**
         * Truncates the table, removing the callback of any mutated pages in the cache, clearing
         * all of the indexes, and marking the roots to be cleared in the system tables.
         */
        virtual void truncate() = 0;

        /**
         * Reads an extent from the tree and returns it.
         * @param extent_id The extent ID to read.
         * @return A pointer to the requested page.
         */
        StorageCache::SafePagePtr read_page(uint64_t extent_id);

        /**
         * Flush any dirty pages to disk and return the roots of the indexes to be updated in the
         * system tables.
         * @param call_sync If true, will call sync_data_and_indexes() before returning.
         */
        TableMetadata finalize(bool call_sync);

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
        MutableBTreePtr create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, const ExtensionCallback& extension_callback = {}, const OpClassHandler& opclass_handler = {}, const std::string_view index_type = constant::INDEX_TYPE_BTREE);

        /**
         * Create a btree that can be used for look aside index.
         */
        MutableBTreePtr create_look_aside_root(const ExtensionCallback& extension_callback = {});

        /**
         * Returns the look aside index of the table.
         */
        MutableBTreePtr look_aside_index() const {
            return _look_aside_index;
        }

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

        /**
         * @brief Get table stats
         * @return TableStats
         */
        TableStats get_stats() const {
            return _stats;
        }

        /** Commit the data and indexes to disk. */
        void sync_data_and_indexes();

        /** Get the list of table data and index files. */
        std::vector<std::filesystem::path> get_table_files() const;

        /**
         * @brief Get next internal row ID to be used for the row in the mutation
         */
        uint64_t get_next_internal_row_id() {
            return ++_internal_row_id;
        }

        /**
         * Initialize cached write cache schema and fields for committer performance.
         * Uses the table's existing _schema to build write cache schema with op/lsn columns.
         * Must be called before processing extents from write cache.
         */
        void initialize_wc_schema(const ExtensionCallback& extension_callback);

        /** Returns the cached write cache schema, or nullptr if not initialized */
        ExtentSchemaPtr wc_schema() const { return _wc_schema; }

        /** Returns the cached op field accessor, or nullptr if not initialized */
        FieldPtr wc_op_field() const { return _wc_op_field; }

        /** Returns the cached data fields, or nullptr if not initialized */
        FieldArrayPtr wc_fields() const { return _wc_fields; }

        /** Returns the cached data fields without internal_row_id */
        FieldArrayPtr actual_table_fields() const { return _actual_table_fields; }

        /** Returns the cached key fields, or nullptr if not initialized */
        FieldArrayPtr wc_key_fields() const { return _wc_key_fields; }

    protected:
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
        void _insert_direct(TuplePtr value, uint64_t extent_id);

        /**
         * Inserts a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        void _insert_empty(TuplePtr value);

        /**
         * Appends a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        void _append_empty(TuplePtr value);

        /**
         * Inserts a tuple at the end of the last extent of the table at the given XID.
         */
        void _insert_append(TuplePtr value);

        /**
         * Inserts a tuple into the extent returned by a primary key lookup using
         * the tuple.
         */
        void _insert_by_lookup(TuplePtr value);

        /**
         * Either inserts a tuple directly into the provided extent at the given XID, or updates an
         * existing row with the same primary key value.
         */
        bool _upsert_direct(TuplePtr value, uint64_t extent_id);

        /**
         * Upserts a tuple into the "empty" Page object.  Used when the Table started empty.
         */
        bool _upsert_empty(TuplePtr value);

        /**
         * Inserts a tuple at the given XID, or updates an existing tuple with the same primary key
         * value, using a primary key lookup to find the containing extent.
         */
        bool _upsert_by_lookup(TuplePtr value);

        /**
         * Removes a tuple from the provided extent at the given XID that has the same primary key
         * value.
         */
        void _remove_direct(TuplePtr value, uint64_t extent_id);

        /**
         * Removes a row from the "empty" Page object.  Used when the Table started empty.
         */
        void _remove_empty(TuplePtr value);

        /**
         * Removes a tuple at the given XID that has the same primary key value by using a primary
         * key lookup to find the containing extent.
         */
        void _remove_by_lookup(TuplePtr key);

        /**
         * Removes a tuple at the given XID that has the same primary key value by using a table
         * scan to find the containing extent.
         */
        void _remove_by_scan(TuplePtr value);

        /**
         * Updates an existing row with the matching primary key value in the provided extent at the
         * given XID.
         */
        void _update_direct(TuplePtr value, uint64_t extent_id);

        /**
         * Updates a row in the "empty" Page object.  Used when the Table started empty.
         */
        void _update_empty(TuplePtr value);

        /**
         * Updates an existing row with the matching primary key value at the given XID, using a
         * primary key lookup to find the containing extent.
         */
        void _update_by_lookup(TuplePtr key);

        /**
         * Convert the schema of the page, if needed based on the target XID.
         */
        void _check_convert_page(StorageCache::SafePagePtr &page);

        /**
         * Helper to extract the extent_id of the target page for a mutation from the primary index.
         */
        uint64_t _get_extent_id(TuplePtr search_key);

        /**
         * Helper to find the secondary indexes that will be affected by the update
         */
        std::vector<uint64_t> _find_updated_secondary_indexes(Extent::Row existing_row, TuplePtr value);

    private:
        enum class MutationType { INSERT, APPEND, UPDATE, UPSERT, REMOVE, REMOVE_BY_SCAN };

        /**
         * Helper method to wrap callback and invoke cache mutation methods
         * @param page   StorageCache Page in which value will be mutated
         * @param value  TuplePtr to be mutated
         */
        template <MutationType m_type>
        auto _mutation_wrapper(StorageCache::SafePagePtr &page, TuplePtr value);

    protected:
        uint64_t _db_id; ///< The ID of the database containing this table.
        uint64_t _id; ///< The ID of the table.

        uint64_t _access_xid; ///< The access XID for this set of mutations.
        uint64_t _target_xid; ///< The final target XID for this set of mutations.
        uint64_t _snapshot_xid{0}; ///< The XID of the snapshot that this version of the table started from.

        /**
         * Internal row ID for each of the row in the table, used in generating
         * look-aside index for secondary indexes
         */
        std::atomic<uint64_t> _internal_row_id{1};

        std::filesystem::path _table_dir; ///< The directory containing the table data.
        std::filesystem::path _data_file; ///< The file containing the table data extents.

        std::vector<std::string> _primary_key; ///< The key columns of the primary index.

        bool _use_empty; ///< True if the table started or became empty, requiring use of the _empty_page.
        FieldPtr _primary_extent_id_f; ///< A field accessor for the extent ID within the primary index extents.

        /** The primary index of the table. */
        MutableBTreePtr _primary_index; ///< The mutable primary index btree.

        /**
         * Look aside index for the secondary indexes
         */
        MutableBTreePtr _look_aside_index; ///< The mutable look-aside index btree.

        /** A map of secondary indexes
         * first is the index id
         * second.first is btree
         * second.second are the index columns
         */
        SecondaryIndexesCache _secondary_indexes; ///< The mutable secondary index btrees.
        ExtentSchemaPtr _schema; ///< The schema of the data extents of the table.
        ExtentSchemaPtr _schema_without_row_id; ///< The schema of the data extents of
                                                ///the table without internal row id.
        ExtentSchemaPtr _look_aside_schema; ///< The schema of the look aside index.

        ExtentSchemaPtr _roots_schema; ///< The schema of the "roots" file.
        MutableFieldPtr _roots_root_f; ///< The field accessor for the tree roots stored within each row of the "roots" file.
        MutableFieldPtr _roots_index_id_f; ///< The field accessor for the tree roots index ids stored within each row of the "roots" file.
        MutableFieldPtr _roots_last_internal_row_id_f; ///< The field accessor for the last_internal_row_id
                                                       ///stored within each row of the "roots" file

        std::unique_ptr<StorageCache::SafePagePtr> _empty_page; ///< Used to handle the empty table corner-case.
        TableStats _stats{}; ///< The stats for the table.

        ExtensionCallback _extension_callback; ///< The extension callback for this table.

        // Cached write cache schema and fields for committer performance
        ExtentSchemaPtr _wc_schema;           ///< Pre-computed write cache schema with op/lsn columns
        FieldPtr _wc_op_field;                ///< Field accessor for __springtail_op
        FieldArrayPtr _wc_fields;             ///< Field accessors for all data columns
        FieldArrayPtr _actual_table_fields;   ///< Field accessors for all data columns without internal_row_id
        FieldArrayPtr _wc_key_fields;         ///< Field accessors for primary key columns
    };
    typedef std::shared_ptr<MutableTable> MutableTablePtr;


}
