#pragma once

#include <pg_log_mgr/index_reconciliation_queue_manager.hh>
#include <pg_log_mgr/index_requests_manager.hh>
#include <redis/redis_ddl.hh>
#include <sys_tbl_mgr/table.hh>

namespace springtail::committer {

    /**
     * Indexer is responsible for building table secondary indexes.
     */
    class Indexer {
    public:
        enum class IndexStatus {
            BUILDING,     // Default state
            DELETING,
            ABORTING
        };
        struct IndexParams {
            uint64_t _db_id;
            uint64_t _xid;
            proto::IndexProcessRequest _index_request;
            IndexStatus _status = IndexStatus::BUILDING;

            /**
             * @brief Checks if the current status matches the expected status.
             *
             * @param expected The status to compare against.
             * @return true if the current status matches the expected status, false otherwise.
             */
            bool is_status(IndexStatus expected) const {
                return _status == expected;
            }
        };

        Indexer(uint32_t worker_count, std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> index_reconciliation_queue_mgr);

        Indexer(const Indexer&) = delete;
        Indexer& operator=(const Indexer&) = delete;

        /**
         * @brief Process index requests(create/drop) at an XID
         * @param db_id          Database ID
         * @param xid            XID at which index is created/dropped
         * @param index_requests Index requests (create/drop)
         *
         */
        void process_requests(uint64_t db_id, uint64_t xid, const std::list<proto::IndexProcessRequest> &index_requests);

        /**
         * @brief Recover indexes which were not complete (build or drop) during shutdown/crash
         * @Param db_id The ID of the database
         */
        void recover_indexes(uint64_t db_id);

        /**
         * Build a secondary index.
         * @param job Defines parameters of the index.
         */
        void build(IndexParams idx);

        /**
         * Drop the index.
         * @param db_id The ID of the database.
         * @param index_id The ID of the index to drop.
         */
        void drop(uint64_t db_id, uint64_t index_id, uint64_t xid);

        /**
         * @brief Processes the given db_id/xid's entries for index reconciliation.
         *
         * Iterates through the xid's entries, calling reconcile_index() for each.
         * Cleans up empty entries from the map.
         *
         * @param db_id The database ID to process.
         * @param reconcile_xid XID for which index reconciliation to be done
         * @param end_xid XID at which index will be committed
         */
        void process_index_reconciliation(uint64_t db_id, uint64_t reconcile_xid, uint64_t end_xid);

        /**
         * @brief Set ABORTING state for the indices of a given db_id and table_id.
         *        This is for the table thats being resync'ed
         *
         * Locks both _table_idx_map and _work_set mutex, iterates through all keys,
         * retrieves corresponding work item, and sets state to ABORTING
         *
         * @param db_id Database ID.
         * @param table_id Table ID.
         * @param XID To manage index DDL counter
         */
        void abort_indexes(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * @brief Remove data associated with the given database id
         *
         * @param db_id database id
         */
        void remove_db(uint64_t db_id) { _cleanup_for_db(db_id); }

    private:
        void task(std::stop_token st);

        // Key is used to identify work items.
        using Key = std::pair<uint64_t, // DB id
            uint64_t // index ID
                >;

        struct IndexState;

        IndexState _build(std::stop_token st, const Key& key, const IndexParams& idx);

        void _drop(const Key& key, const IndexParams& idx, uint64_t end_xid);

        bool _was_dropped(const Key& key);
        void _commit_build(AnyIndexRoot root, const Key& key, const IndexParams& idx,
                uint64_t end_xid, IndexWriterPtr look_aside_root);

        // work state
        std::condition_variable_any _cv;
        std::mutex _m;
        std::unordered_map<Key, IndexParams, boost::hash<Key>> _work_set;
        std::queue<Key> _queue;

        // workers
        std::vector<std::jthread> _workers;

        // reconciliation Index

        /**
         * @brief Represents the state of an index after the initial build.
         *
         * This structure holds information about the index's root, key, and metadata.
         * After the initial index build, instances of this struct are added to the
         * pending reconciliation map for further processing.
         * - `key` contains `dbid` and `tableid`.
         * - `idx` contains `dbid`, `indexid`, and `ddl`.
         */
        struct IndexState {
            /** btree redesign phase 10: AnyIndexRoot (table.hh), not IndexWriterPtr -- the
                index's own root may be either a btree or a GiST Writer; see AnyIndexRoot's
                class comment for why Indexer needs this (not MutableTable::create_index_root()
                itself) to be kind-polymorphic. */
            AnyIndexRoot _root;
            Key _key;
            IndexParams _idx;
            uint64_t _tid;
            /** Always a btree -- the look-aside index is never GiST (architecture.md Sec 5.5). */
            IndexWriterPtr _look_aside_root;
        };

        /**
         * Per table_id: the look-aside IndexWriter owned by whichever concurrent "first index on
         * this table" build (architecture.md Sec 5.4 case 2) got there first this generation.
         * This map's presence/absence of a table_id entry *is* the external serialization point
         * case 2 calls for ("exactly one... creates and owns it; others... discover it already
         * exists") -- the single-writer IndexWriter contract (Sec 5.4) means only the owning
         * builder (the one that inserted this entry, tracked locally via _build()'s own
         * build_look_aside flag) may ever call a mutating method on the stored writer; a builder
         * that finds the entry already present must treat it as read-only discovery, never as a
         * second mutator, since IndexWriter has no internal locking to protect against that and
         * finalize() is one-shot. See phases/phase-3.md's look-aside bootstrap audit for the full
         * reasoning (including why passive discovery, not shared mutation, is the correct fix for
         * the race phase-2.md's own instance-independence note left open).
         */
        std::unordered_map<uint64_t, IndexWriterPtr> _look_aside_build_tracker;

        /**
         * Mutex to have safe access for _look_aside_build_tracker
         */
        std::mutex _look_aside_mutex;

        /**
         * @brief Tracks pending index reconciliation tasks by db_id and xid.
         *
         * Maps a database ID to a map of transaction IDs (XIDs), each holding
         * a list of `IndexState` entries pending reconciliation.
         */
        using PendingReconMap = std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::list<IndexState>>>;
        PendingReconMap _pending_idx_reconciliation_map;

        /**
         * @brief Maps database IDs to table IDs and their associated index keys being built.
         *
         * Structure: { db_id - { table_id - [index keys] } }.
         */
        using TableIndicesMap = std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::list<Key>>>;
        TableIndicesMap _table_idx_map;

        /**
         * @brief Tracks the number of DDL operations per XID.
         *
         * This map stores an atomic counter for each transaction ID (XID),
         * initialized with the total number of DDL operations for that XID.
         * The counter is decremented as each DDL is processed. When the count
         * reaches zero, it indicates all DDLs for the transaction have been completed.
         */
        std::unordered_map<uint64_t, std::atomic<int>> _xid_ddl_counter_map;

        /**
         * @brief Mutex for synchronizing access to the pending reconciliation map.
         */
        std::mutex _pending_reconciliation_map_mtx;

        /**
         * @brief Mutex for synchronizing access to the table index map.
         */
        std::mutex _table_idx_map_mtx;

        /**
         * @brief Mutex for synchronizing access to the DDL counter map.
         */
        std::mutex _xid_ddl_counter_map_mtx;

        /**
         * @brief Adds an IndexState to the pending reconciliation map.
         *
         * This method ensures the correct db_id and xid mapping before inserting the IndexState.
         * @param idx_state The IndexState to be added.
         */
        void _add_to_pending_reconciliation(IndexState&& idx_state);

        /**
         * @brief Reconcile indexes at the given xid
         * @param idx_state Index state
         * @param end_xid XID at which indexes to be committed
         */
        void _reconcile_index(IndexState& idx_state, uint64_t end_xid);

        /**
         * @brief Removes an index key and cleans up empty entries.
         * @param db_id Database ID.
         * @param table_id Table ID.
         * @param key Index key to remove.
         * @param owns_look_aside True iff *this* completing build is the one that owns
         *        `_look_aside_build_tracker[table_id]` (i.e. it was the builder that created the
         *        table's look-aside index this generation, architecture.md Sec 5.4 case 2).
         *        `_look_aside_build_tracker`'s entry is retired only when its owner's build
         *        completes -- a non-owning sibling build finishing first (or a `_drop()` call,
         *        which never owns a look-aside bootstrap) must never clear another build's
         *        in-flight ownership record, or a third concurrent "first index" build could
         *        start a second, independent look-aside bootstrap against the same table while
         *        the real owner is still mid-build.
         */
        void _remove_index_key(uint64_t db_id, uint64_t table_id, const Key& key, bool owns_look_aside);

        /**
         * @brief Cleanup all records related to the db before initiating recovery
         *        as it will populate from the scratch
         * @param db_id Database ID.
         */
        void _cleanup_for_db(uint64_t db_id);

        std::vector<uint32_t> _get_index_cols(proto::IndexInfo index_info);

        /** Same column ordering as _get_index_cols() (sorted by idx_position), but returning
            each column's opclass instead of its position -- btree redesign phase 10, needed to
            construct a GistWriteStrategy/GistReadStrategy with opclasses in the right order. */
        std::vector<std::string> _get_index_opclasses(proto::IndexInfo index_info);

        /**
         * @brief Helper method to get index ddl for create action
         * @param index_info proto::IndexInfo - Index to be created
         * @return ddl json containing index create action details
         */
        nlohmann::json _get_create_index_ddl(proto::IndexInfo index_info);

        /**
         * @brief Helper method to get index ddl for drop action
         * @param index_info proto::IndexInfo - Index to be dropped
         * @return ddl json containing index drop action details
         */
        nlohmann::json _get_drop_index_ddl(proto::IndexInfo index_info);

        /**
         * @brief shared_ptr to the index reconciliation manager to access the index reconciliation queues
         */
        std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> _index_reconciliation_queue_mgr;
    };
}
