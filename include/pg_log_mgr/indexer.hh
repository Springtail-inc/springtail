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

        explicit Indexer(uint32_t worker_count, std::shared_ptr<ConcurrentQueue<std::string>> index_reconciliation_queue);

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
         * @brief Processes the first pending xid's entries for the given db_id.
         * 
         * Iterates through the first xid's entries, calling reconcile_index() for each.
         * Cleans up empty entries from the map.
         * 
         * @param db_id The database ID to process.
         * @param end_xid XID at which index will be committed
         * @return std::optional<uint64_t> The XID that got reconciled or nullopt if none
         */
        std::optional<uint64_t> process_next_reconciliation(uint64_t db_id, uint64_t end_xid);

    private:
        void task(std::stop_token st);

        // Key is used to identify work items.
        using Key = std::pair<uint64_t, // DB id
            uint64_t // index ID
                >;

        struct IndexState;

        IndexState _build(std::stop_token st, const Key& key, const IndexParams& idx);

        void _drop(const Key& key, const IndexParams& idx);

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

        // Mutex to access pending reconciliation map
        std::mutex _pending_reconciliation_map_mtx;

        /**
         * @brief Adds an IndexState to the pending reconciliation map.
         * 
         * This method ensures the correct db_id and xid mapping before inserting the IndexState.
         * 
         * @param idxState The IndexState to be added.
         */
        void _add_to_pending_reconciliation(IndexState&& idxState);

        /**
         * This will reconcile the index by catching
         * all the table XIDs that happened post build initialization
         * @param idxState Index state
         */
        void _reconcile_index(IndexState& idxState, uint64_t end_xid);

        /*
         * Pick the XIDs for the given db_id and reconcile indexes 
         * belonging to the the XIDs
         * @param db_it Iterator to the XID map for a database
         * @param end_xid XID at which index will be committed
         * @return std::optional<uint64_t> The XID that got reconciled or nullopt if none
         */
        std::optional<uint64_t> _process_next_reconciliation(PendingReconMap::iterator db_it, uint64_t end_xid);

        /*
         * @brief A queue for indexer to notify committer to trigger index reconciliation
         */
        std::shared_ptr<ConcurrentQueue<std::string>> _index_reconciliation_queue;
    };
}
