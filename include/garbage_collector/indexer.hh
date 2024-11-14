#pragma once

#include <stop_token>
#include <thread>
#include <queue>
#include <condition_variable>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <utility>

namespace springtail::gc {

    /**
     * Indexer is responsible for building table secondary indexes.
     */
    class Indexer {
    public:
        struct IndexParams {
            uint64_t _db_id;
            uint64_t _xid;
            uint64_t _completed_xid;
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
        void drop(uint64_t db_id, uint64_t index_id);

    private:
        void task(std::stop_token st);

        void _build(std::stop_token st, IndexParams idx);

        // Key is used to identify work items.
        using Key = std::pair<uint64_t, // DB id
            uint64_t // index ID
                >;

        struct Hash {
            size_t operator()(const Key& key) const
            { 
                auto a = std::hash<Key::first_type>{}(key.first);
                auto b = std::hash<Key::second_type>{}(key.second); 
                return a ^ (b << 1); 
            };
        };

        // work state
        std::condition_variable_any _cv;
        std::mutex _m;
        std::unordered_map<Key, std::optional<IndexParams>, Hash> _work_set;
        std::queue<Key> _queue;

        // workers
        std::vector<std::jthread> _workers;
    };

}
