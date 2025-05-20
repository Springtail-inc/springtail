#pragma once

#include <pg_repl/index_reconcile_request.hh>
#include <common/concurrent_queue.hh>

namespace springtail::pg_log_mgr {

    /**
     * Manages index reconciliation queues per DB
     */
    class IndexReconciliationQueueManager {
        public:
            using IndexReconcileRequestPtr = std::shared_ptr<IndexReconcileRequest>;
            using IRQueue = ConcurrentQueue<IndexReconcileRequest>;

            /**
             * @brief Adds a new queue with the given DB ID if it does not already exist.
             * 
             * @param db_id The identifier for the new queue.
             */
            void add_queue(uint64_t db_id) {
                std::unique_lock lock(map_mutex);
                index_reconciliation_queues.emplace(db_id, std::make_shared<IRQueue>());
            }

            /**
             * @brief Retrieves an index reconcile request from the 
             *        db's index reconciliation queue
             * 
             * @param db_id The database/queue identifier.
             * @param seconds timeout in seconds
             * @return IndexReconcileRequestPtr
             */
            std::optional<IndexReconcileRequestPtr> pop(uint64_t db_id, uint32_t seconds = 0) {
                std::unique_lock lock(map_mutex);
                auto it = index_reconciliation_queues.find(db_id);
                // Allow indexer to continue pushing to the queue
                // as the ConcurrentQueue has timeout if queue is empty
                lock.unlock();
                if (it != index_reconciliation_queues.end()) {
                    return it->second->pop(seconds);
                }
                return std::nullopt;
            }

            /**
             * @brief Pushes index reconcile request into the appropriate queue
             * @param db_id Queue identifier
             * @param IndexReconcileRequestPtr shared_ptr to the Index reconcile request
             * @return bool true if push is successful, false otherwise.
             */
            bool push(uint64_t db_id, IndexReconcileRequestPtr value) {
                std::unique_lock lock(map_mutex);
                auto it = index_reconciliation_queues.find(db_id);
                if (it != index_reconciliation_queues.end()) {
                    it->second->push(value);
                    return true;
                }
                return false;
            }

            /**
             * @brief Removes the queue with the specified DB ID.
             * 
             * @param db_id The database/queue identifier to remove.
             */
            void remove_queue(uint64_t db_id) {
                std::unique_lock lock(map_mutex);
                index_reconciliation_queues.erase(db_id);
            }

        private:
            std::unordered_map<uint64_t, std::shared_ptr<IRQueue>> index_reconciliation_queues;
            std::shared_mutex map_mutex;  ///< Protects access to the queue map.
    };
}
