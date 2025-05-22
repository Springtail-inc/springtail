#pragma once

#include <stop_token>
#include <thread>
#include <queue>
#include <condition_variable>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <utility>
#include <redis/redis_ddl.hh>
#include <boost/functional/hash.hpp>
#include <storage/mutable_btree.hh>
#include <common/common.hh>
#include <pg_repl/index_reconcile_request.hh>
#include <pg_log_mgr/index_reconciliation_queue_manager.hh>

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
            nlohmann::json _ddl;
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

        void process_ddls(uint64_t db_id, uint64_t xid, nlohmann::json const& ddls);

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
        void _commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx, uint64_t end_xid);

        // work state
        std::condition_variable_any _cv;
        std::mutex _m;
        std::unordered_map<Key, IndexParams, boost::hash<Key>> _work_set;
        std::queue<Key> _queue;

        // workers
        std::vector<std::jthread> _workers;

        RedisDDL _redis_ddl; ///< The interfaces to manage the DDL statements in Redis.
        
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
            MutableBTreePtr _root;
            Key _key;
            IndexParams _idx;
            uint64_t _tid;
        };
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
         */
        void _remove_index_key(uint64_t db_id, uint64_t table_id, const Key& key);

        /**
         * @brief shared_ptr to the index reconciliation manager to access the index reconciliation queues
         */
        std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> _index_reconciliation_queue_mgr;
    };
}
