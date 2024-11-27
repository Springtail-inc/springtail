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

namespace springtail::gc {

    /**
     * Indexer is responsible for building table secondary indexes.
     */
    class Indexer {
    public:
        struct IndexParams {
            uint64_t _db_id;
            uint64_t _xid;
            uint64_t _target_xid;
            nlohmann::json _ddl;
        };

        explicit Indexer(uint32_t worker_count);

        Indexer(const Indexer&) = delete;
        Indexer& operator=(const Indexer&) = delete;

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
        void drop(uint64_t db_id, uint64_t index_id, uint64_t xid, uint64_t target_xid);


        /**
         * This will wait for the index work completion for given db and table.
         * @param db_id The ID of the database.
         * @param tid The ID of the table.
         */
        void wait_for_completion(uint64_t db_id, uint64_t tid);

        /**
         * This will wait for the index work completion for given db. 
         * @param db_id The ID of the database.
         */
        void wait_for_completion(uint64_t db_id);

    private:
        void task(std::stop_token st);


        // Key is used to identify work items.
        using Key = std::pair<uint64_t, // DB id
            uint64_t // index ID
                >;

        MutableBTreePtr _build(std::stop_token st, const Key& key, const IndexParams& idx);
        // returns false if cancelled
        bool _check_work_state(const Key& key);
        void _commit_build(MutableBTreePtr root, const Key& key, const IndexParams& idx);

        // this is to notify when an index modifiction is completed 
        std::condition_variable _cv_done;

        // work state
        std::condition_variable_any _cv;
        std::mutex _m;
        std::unordered_map<Key, IndexParams, boost::hash<Key>> _work_set;
        std::queue<Key> _queue;

        // workers
        std::vector<std::jthread> _workers;

        RedisDDL _redis_ddl; ///< The interfaces to manage the DDL statements in Redis.
    };
}
