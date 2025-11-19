#pragma once

#include <sys_tbl_mgr/table.hh>

namespace springtail {

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
                     const OpClassHandler &opclass_handler = {},
                     bool bypass_schema_cache = false);

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
        MutableBTreePtr create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns, const ExtensionCallback& extension_callback = {}, const OpClassHandler& opclass_handler = {}, const std::string& index_type = "btree");

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
        bool _bypass_schema_cache; ///< True if schema construction should bypass TableMgr cache and use schema_helpers directly.
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

} // namespace springtail
