#pragma once

#include <sys_tbl_mgr/table.hh>

namespace springtail {

    /**
     * Interface for mutating a table at the most recent XID.  The primary, look-aside, and
     * secondary indexes are IndexWriter<BtreeWriteStrategy> instances (architecture.md "btree"
     * redesign Sec 5.1, 5.4): single-writer, no internal locking -- callers of this class must be
     * the sole mutator of a given MutableTable instance at a time (see Table/MutableTable's
     * Reader/Writer pairing invariant, architecture.md Sec 5.3).
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

        /** Create a writer root that can be used for indexes.
         * @param index_id PG index ID.
         * @param index_columns Positions of the index columns.
         * @param access_method The index's access method (btree redesign phase 9). Defaults to
         * BTREE since every call site reconstructing an already-READY index (no other kind can
         * reach READY yet) doesn't thread it through -- callers that know the real access method
         * (Indexer::_build) must pass it explicitly. GIST/GIN are not yet implemented: this
         * throws rather than silently building a btree or a real GiST/GIN tree. Phase 10/11 add a
         * case here, replacing the throw with real construction -- this is the only place in this
         * function they need to touch.
         */
        IndexWriterPtr create_index_root(uint64_t index_id, const std::vector<uint32_t>& index_columns,
                const ExtensionCallback& extension_callback = {},
                IndexAccessMethod access_method = IndexAccessMethod::BTREE);

        /**
         * Create a GiST index's Writer root (btree redesign phase 10) -- the GIST counterpart of
         * create_index_root(), kept as its own method rather than a widened create_index_root()
         * return type; see AnyIndexRoot's class comment (table.hh) for why. `opclass_names` is
         * per-column, same order as `index_columns` (phase 9's Indexes::OPCLASS catalog column,
         * threaded through storage::Index::Column::opclass).
         */
        GistWriterPtr create_gist_index_root(uint64_t index_id, const std::vector<uint32_t> &index_columns,
                const std::vector<std::string> &opclass_names,
                const ExtensionCallback &extension_callback = {});

        /**
         * Create a btree that can be used for look aside index.
         */
        IndexWriterPtr create_look_aside_root(const ExtensionCallback& extension_callback = {});

        /**
         * Returns the look aside index of the table.
         */
        IndexWriterPtr look_aside_index() const {
            return _look_aside_index;
        }

        /**
         * Returns the requested index writer of the table based on the index ID in the "indexes" table.
         * @param idx The id of the index to retrieve.  Note that 0 is the primary index.
         * @return An IndexWriter object of the requested index.
         */
        IndexWriterPtr index(uint32_t idx) {
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

        /** Every index writer this table owns: the primary index, the look-aside index (if one
            has been built yet -- see §5.6/create_look_aside_root()), and every secondary index,
            in no particular order. Used by run_compaction_cycle() to check each index's own
            segments against the Vacuumer's current compaction candidates -- the natural place
            for this is here, not in the caller, since this class is the one that already owns
            and enumerates _secondary_indexes. */
        std::vector<IndexWriterPtr> all_index_writers() const;

        /**
         * Phase 1's output for one table, held between prepare_compaction_cycle() and
         * apply_compaction_cycle() (architecture.md "btree" redesign Sec 7.1) -- deliberately a
         * plain data holder with no behavior, since the whole point of splitting these two
         * calls is letting a caller do something else (e.g. acquire a lock) in between them.
         * `writer` is null and `items` empty when prepare_compaction_cycle() found nothing to
         * do; apply_compaction_cycle() treats that as a no-op.
         */
        struct PreparedCompaction {
            IndexWriterPtr writer;
            std::filesystem::path scratch_file;
            std::vector<IndexWriterT::CompactionItem> items;
        };

        /**
         * Background space-reclaim compaction, phase 1 only (architecture.md "btree" redesign
         * Sec 7.1): checks the Vacuumer's current compaction-candidate segments against each of
         * this table's indexes' own segment files (IndexWriter::get_segment_paths()) and, for at
         * most one matching segment, bulk-copies its still-reachable extents into a scratch
         * file. Needs no exclusivity at all -- IndexWriter::bulk_copy_for_compaction() touches
         * only this tree's (already thread-safe) extent cache and construction-time-fixed
         * schema state, never `_root`/`_segments`/`_in_call` -- so, unlike every other mutating
         * entry point this class exposes, this is safe to call *without* this table's owning
         * component holding any lock at all, including whatever serialization point it would
         * otherwise need before apply_compaction_cycle() (round-1 review finding 1: running
         * phase 1 inside `Server::finalize()`'s per-db_id catalog-queue slot blocked every
         * other system table's catalog traffic in that database for the scan's duration, which
         * is exactly the "zero coordination" property phase 1 is specifically designed to not
         * need). A no-op (returns a `PreparedCompaction` with a null `writer`) if none of this
         * table's segments are currently a candidate.
         */
        PreparedCompaction prepare_compaction_cycle();

        /**
         * Background space-reclaim compaction, phase 2 only (architecture.md Sec 7.1): relocates
         * `batch` (prepare_compaction_cycle()'s output) into its writer's live structure, through
         * the ordinary single-writer mutation path -- same exclusivity contract as insert()/
         * remove(), the caller must already hold it (a Committer worker's claimed table_id turn,
         * or a system-table catalog-queue callback; see Decisions log entry 2). A no-op if
         * `batch.writer` is null (phase 1 found nothing to do). Never calls finalize() -- any
         * relocation performed here rides along with the caller's own already-scheduled
         * finalize() for this generation, exactly like any other mutation.
         */
        void apply_compaction_cycle(PreparedCompaction &batch);

        /**
         * Convenience wrapper running both phases back to back -- safe only for a caller that
         * doesn't need phase 1 to run outside whatever lock it already holds (architecture.md
         * Sec 5.4 case 3: an ordinary table's `Committer` worker is already this table's sole
         * mutator regardless, so there's no lock for phase 1 to escape here). System-table
         * callers must not use this -- see Server::finalize(), which calls
         * prepare_compaction_cycle() before acquiring its catalog-queue slot and
         * apply_compaction_cycle() after.
         */
        void run_compaction_cycle();

        /**
         * Background structural-reclaim (merge), phase 8 (architecture.md "btree" redesign Sec
         * 7.1): resolve every underfull flag Vacuumer currently holds for each of this table's
         * index writers, up to Vacuumer::merge_item_budget() total across all of them this
         * call. Unlike compaction, this is a single phase, not a prepare/apply split
         * (phases/phase-8.md OQ2) -- the background lookup itself (re-find the node, ask its
         * parent for a sibling) is small and local, with no slow scan to run outside this
         * table's single-writer exclusivity the way compaction's segment scan needed, so there
         * is nothing to split a "prepare" out of. Needs this table's ordinary exclusivity
         * throughout, exactly like insert()/remove()/apply_compaction_cycle() -- the caller
         * must already hold it (a Committer worker's claimed table_id turn, or a system-table
         * catalog-queue callback; mirrors Decisions log entry 2's routing for compaction). Every
         * flag this call examines is retired (merged, found stale, or dropped for "no sibling
         * had room") regardless of outcome -- see Vacuumer::clear_underfull_flag().
         */
        void run_merge_cycle();

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
        ExtentSchemaPtr wc_schema() const { return _wc_shema_info._wc_schema; }

        /** Returns the cached op field accessor, or nullptr if not initialized */
        FieldPtr wc_op_field() const { return _wc_shema_info._wc_op_field; }

        /** Returns the cached data fields, or nullptr if not initialized */
        FieldArrayPtr wc_fields() const { return _wc_shema_info._wc_fields; }

        /** Returns the cached data fields without internal_row_id */
        FieldArrayPtr actual_table_fields() const { return _wc_shema_info._actual_table_fields; }

        /** Returns the cached key fields, or nullptr if not initialized */
        FieldArrayPtr wc_key_fields() const { return _wc_shema_info._wc_key_fields; }

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
        IndexWriterPtr _primary_index; ///< The mutable primary index writer.

        /**
         * Look aside index for the secondary indexes
         */
        IndexWriterPtr _look_aside_index; ///< The mutable look-aside index writer.

        /** A map of secondary indexes
         * first is the index id
         * second.first is btree
         * second.second are the index columns
         */
        SecondaryIndexesCache _secondary_indexes; ///< The mutable secondary index btrees.

        /** GiST secondary indexes (btree redesign phase 10) -- kept separate from
            _secondary_indexes rather than merged into it; see AnyIndexRoot's class comment
            (table.hh). Not yet consulted by this class's own generic mutation machinery
            (indexer_helpers::mutation_handler, _mutation_wrapper()) -- ordinary per-row DML
            maintenance for a GiST index is a known, explicitly-flagged gap of this phase (see
            phases/phase-10.md's Design notes): only Indexer's own build-time scan (and its
            reconciliation catch-up, via AnyIndexRoot) populates one today. */
        std::map<uint64_t, std::pair<GistWriterPtr, std::vector<uint32_t>>> _gist_indexes;

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
        WcSchemaInfo _wc_shema_info;    ///< Information about write cache schema
    };
    typedef std::shared_ptr<MutableTable> MutableTablePtr;

} // namespace springtail
