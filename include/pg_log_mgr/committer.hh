#pragma once

#include <chrono>
#include <filesystem>
#include <thread>

#include <boost/thread.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <common/concurrent_queue.hh>
#include <common/constants.hh>
#include <common/coordinator.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>
#include <common/time_trace.hh>
#include <common/event_frequency.hh>

#include <redis/redis_ddl.hh>
#include <redis/redis_containers.hh>

#include <pg_log_mgr/xid_ready.hh>
#include <pg_log_mgr/indexer.hh>
#include <pg_log_mgr/index_reconciliation_queue_manager.hh>
#include <pg_log_mgr/index_requests_manager.hh>

#include <pg_repl/index_reconcile_request.hh>

#include <sys_tbl_mgr/table.hh>
#include <write_cache/write_cache_index.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::committer {

    /**
     * The Committer is responsible for reading modifications from the write cache and applying them
     * to the on-disk representation of the tables.  It operates by pulling a list of extents from
     * the WriteCache and then applying the mutations from each extent.  The application of
     * mutations are performed in parallel by a configurable set of worker threads.
     *
     * Once all of the mutations up through a given XID have been applied, the XID is committed to
     * disk and the various query subsystems are updated with a new base XID at which they can
     * operate.  It also allows for potential cleanup of resources from the WriteCache and the
     * on-disk copy of the WAL.
     */
    class Committer {
    public:
        struct TxCounters {
            size_t inserts = 0;
            size_t updates = 0;
            size_t deletes = 0;
            size_t truncates = 0;
            size_t messages = 0;

            TxCounters& operator+=(const TxCounters& rhs) {
                inserts += rhs.inserts;
                updates += rhs.updates;
                deletes += rhs.deletes;
                truncates += rhs.truncates;
                messages += rhs.messages;
                return *this;
            }
        };

        Committer(uint32_t worker_count, const std::shared_ptr<ConcurrentQueue<committer::XidReady>> &committer_queue,
                std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> index_reconciliation_queue_mgr,
                const std::shared_ptr<pg_log_mgr::IndexRequestsManager> &index_requests_mgr, uint32_t indexer_worker_count,
                std::chrono::milliseconds fsync_interval)
            : _worker_count(worker_count),
              _indexer_worker_count(indexer_worker_count),
              _fsync_interval(fsync_interval),
              _committer_queue(committer_queue),
              _index_reconciliation_queue_mgr(index_reconciliation_queue_mgr),
              _index_requests_mgr(index_requests_mgr)
        {}

        Committer(const Committer &) = delete;
        Committer &operator=(const Committer &) = delete;
        Committer(Committer &&) = delete;
        Committer &operator=(Committer &&) = delete;

        /** Initiate the committer loop. */
        void run();

        /** Stop all execution after draining any ongoing work with the current XID. */
        void shutdown();

        /** Perform cleanup on a failed thread. */
        void cleanup();

        // constants for the coordinator thread IDs
        constexpr static const std::string_view THREAD_TYPE = "commit";
        constexpr static const std::string_view THREAD_MAIN = "m";
        constexpr static const std::string_view THREAD_WORKER = "w";

    private:
        /**
         * Scan forward through the results deque to find the final XID for each database
         * in the upcoming batch. Stops at the first non-XACT_MSG message (batch boundary).
         * @param start_it Iterator to start scanning from
         * @param end_it Iterator marking the end of the deque
         * @return Map of db_id to final_xid for each database in the batch
         */
        std::map<uint64_t, uint64_t> _scan_batch_final_xids(
            std::deque<std::shared_ptr<XidReady>>::iterator start_it,
            std::deque<std::shared_ptr<XidReady>>::iterator end_it);

        /**
         * Clear the SysTblMgr::Client cache for any tables with DDL mutations.
         */
        void _invalidate_systbl_cache(uint64_t db, const nlohmann::json &completed_ddls);

        /**
         * @brief Expire dropped table dirs
         *
         * @param db_id          Database ID
         * @param completed_ddls DDLs processed
         * @param committed_xid  XID at which ddls were processed
         */
        void _expire_table_drops(uint64_t db_id, const nlohmann::json &completed_ddls, uint64_t committed_xid);

        /**
         * @brief Expire dropped index paths
         *
         * @param db_id          Database ID
         * @param index_requests Indexes processed
         * @param committed_xid  XID at which ddls were processed
         */
        void _expire_index_drops(uint64_t db_id, std::list<proto::IndexProcessRequest>& index_requests, uint64_t committed_xid);

        /**
         * The structure that defines a worker job.
         */
        struct WorkerEntry {
            uint64_t db_id;
            uint64_t tid;
            uint64_t completed_xid;
            uint64_t xid;
        };

        /**
         * The worker thread main loop.
         */
        void _run_worker(int thread_id);

        /**
         * Process all of the mutations for a given table.
         * @param db_id The database ID
         * @param tid The table ID
         * @param completed_xid The most recent XID we completed processing
         * @param xid The XID to process
         * @param thread_name The name of the thread registered with the coordinator
         */
        void _process_table(uint64_t db_id, uint64_t tid, uint64_t completed_xid, uint64_t xid, const std::string &thread_name);

        /**
         * Process a single extent of mutations from the write cache.
         * @param db_id The database ID
         * @param tid The table ID
         * @param xid The XID to process
         * @param table The MutableTable being mutated
         * @param wc_extent The WriteCacheExtent containing the mutations
         */
        TxCounters _process_extent(uint64_t db_id, uint64_t tid, MutableTablePtr table,
                             const std::shared_ptr<springtail::WriteCacheIndexExtent> wc_extent);

        /**
         * Shifts the provided metadata to start at the new future XID.  Returns true if the
         * metadata was modified, false otherwise.
         */
        bool _shift_to_xid(SchemaMetadata &meta, const XidLsn &xid);

        /**
         * Handle TABLE_SYNC_COMMIT and TABLE_SYNC_SWAP message types.
         * @param result The XidReady message to process
         * @param db_id The database ID
         */
        void _handle_table_sync_message(
            const std::shared_ptr<XidReady>& result,
            uint64_t db_id
        );

        /**
         * Handle RECONCILE_INDEX message type in isolation.
         * This commits any pending batch, processes the reconciliation, and commits it.
         * @param result The XidReady message to process
         * @param db_id The database ID
         * @param completed_xid The most recent XID we completed processing
         */
        void _handle_index_reconciliation(
            const std::shared_ptr<XidReady>& result,
            uint64_t db_id,
            uint64_t& completed_xid
        );

        /**
         * Handle XACT_MSG message types.
         * @param result The XidReady message to process
         * @param db_id The database ID
         * @param completed_xid The most recent XID we completed processing
         */
        void _handle_transaction_message(
            const std::shared_ptr<XidReady>& result,
            uint64_t db_id,
            uint64_t completed_xid
        );

    private:
        /**
         * Batch state tracked per database during batch processing
         */
        struct BatchState {
            std::map<uint64_t, MutableTablePtr> table_cache;  ///< tid → MutableTable
            std::vector<std::shared_ptr<XidReady>> xid_results;  ///< All XidReady messages for this db
            uint64_t final_xid = 0;  ///< The final XID where this batch will commit (determined upfront)
        };

        /**
         * Commits all accumulated changes for a single database batch.
         * @param db_id The database ID
         * @param batch The batch state to commit
         * @param completed_xid The XID we started from
         */
        void _commit_batch(
            uint64_t db_id,
            BatchState& batch,
            uint64_t completed_xid
        );

    private:
        bool _has_ddl_precommit = false; ///< Flag indiciating if the redis DDL is holding precommit entries

        /**
         * Table worker threads in the committer
         */
        uint32_t _worker_count;

        /**
         * Batch processing state per database. Maps db_id → BatchState
         */
        std::map<uint64_t, BatchState> _batch_state;

        /**
         * Indexer worker threads to process indexes
         */
        uint32_t _indexer_worker_count;
        std::chrono::milliseconds _fsync_interval; ///< Interval between fsync calls

        ConcurrentQueue<WorkerEntry> _worker_queue; ///< The queue of work for the worker threads.
        std::shared_ptr<ConcurrentQueue<XidReady>> _committer_queue;


        /**
         * @brief shared_ptr to the index reconciliation manager to access the index reconciliation queues
         */
        std::shared_ptr<pg_log_mgr::IndexReconciliationQueueManager> _index_reconciliation_queue_mgr;

        std::vector<std::thread> _worker_threads; ///< The worker threads.

        std::atomic<uint64_t> _shutdown = false; ///< Causes the committer to shut down when set to true.

        boost::mutex _mutex; ///< Mutex to protect internal maps.
        boost::condition_variable _cv; ///< Condition variable to notify from the workers back to the main loop

        std::set<uint64_t> _tid_set; ///< Set of in-flight tables being processed.

        /** Cache of mutable tables that are in-flight. */
        std::map<uint64_t, MutableTablePtr> _table_map;

        /** The most recently completed XID by db.  Note: if we are in a table sync, this may be
            ahead of the most recently committed XID at the XidMgr. */
        std::map<uint64_t, uint64_t> _completed_xids;

        /** The set of databases that are currently not committing XIDs because they are in a table
            sync state. */
        std::set<uint64_t> _block_commit;

        /** Indexer
         */
        std::unique_ptr<Indexer> _indexer;

        /**
         * @brief shared_ptr to the index requests manager to get
         * index requests (create/drop) for an XID per db
         */
        std::shared_ptr<pg_log_mgr::IndexRequestsManager> _index_requests_mgr;

        /**
         * @brief Table sync processor to handle fsync operations in the background
         */
        struct TableSyncProcessor
        {
            TableSyncProcessor(std::chrono::milliseconds sync_interval, size_t max_workers)
                : _sync_interval(sync_interval)
            {
                for (auto i = 0; i != max_workers; ++i) {
                    _workers.emplace_back([this](std::stop_token st) { task(st); });
                }
            }
            TableSyncProcessor(const TableSyncProcessor &) = delete;
            TableSyncProcessor &operator=(const TableSyncProcessor &) = delete;
            TableSyncProcessor(TableSyncProcessor &&) = delete;
            TableSyncProcessor &operator=(TableSyncProcessor &&) = delete;

            ~TableSyncProcessor() = default;

            /** Add a table to be synced.
             *
             * @param xid The XID associated with the table sync
             * @param tables The list of tables to sync
             */
            void add(uint64_t xid, const std::vector<MutableTablePtr>& tables);

        private:
            std::chrono::milliseconds _sync_interval;

            void task(std::stop_token st);

            // work state
            std::condition_variable_any _cv;
            std::mutex _m;

            // workers
            std::vector<std::jthread> _workers;

            struct WorkItem {
                uint64_t xid = 0;
                std::set<std::filesystem::path> files;
            };
            std::unordered_map<uint64_t, WorkItem> _work_map;  ///< db_id → WorkItem
        };

        std::unique_ptr<TableSyncProcessor> _table_sync_processor;
    };
}
