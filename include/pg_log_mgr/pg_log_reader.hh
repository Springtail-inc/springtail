#pragma once

#include <memory>
#include <fstream>
#include <filesystem>
#include <map>
#include <vector>

#include <common/concurrent_queue.hh>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Log reader class.  Reads logs written by PgLogWriter.  Collects the various table
     * mutations into batches which are passed to the WriteCache to make them available to the
     * Committer and FDWs.
     */
    class PgLogReader {
    public:
        /** convenience type for the shared transaction queue */
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgTransaction>>;

        /**
         * @brief Construct a new Pg Log Reader object
         * @param queue queue to enqueue parsed xactions for xid logger and GC
         */
        PgLogReader(const PgTransactionQueuePtr queue)
            : _queue(queue)
        {}

        /**
         * @brief Process next set of messages from log file
         * @param path file path
         * @param start_offset starting file offset
         * @param num_messages number of messages to process (-1 read until end of file)
         */
        void process_log(const std::filesystem::path &path,
                         uint64_t start_offset,
                         int num_messages);

        /**
         * @brief Set the xact map object; moves contents of xact_map to _xact_map
         * @param xact_map xact map -- will be empty after call
         */
        void set_xact_map(std::map<uint32_t, PgTransactionPtr> &xact_map) {
            _xact_map.swap(xact_map);
        }

    private:
        class Batch {
            // 4 MB
            constexpr uint32_t MAX_BATCH_SIZE = 4 * 1024 * 1024;

        public:
            /**
             * Send all extents to the WriteCache, apply all schema changes, notify the
             * Committer.
             */
            void commit();

            /**
             * Abort the entire transaction.  Drop all related batches from the WriteCache and from
             * memory.
             */
            void abort();

            /**
             * Starts a subtransaction by sending all changes thus-far to the WriteCache.  Keeps a
             * pointer to the schema changes so that they can be dropped in the case of a subtxn
             * abort.
             */
            void start_subtxn();

            /**
             * Aborts a subtxn by removing those entries from the WriteCache, dropping any in-memory
             * extents, and rolling back the set of schema changes.
             */
            void abort_subtxn();

            /**
             * Adds a mutation to a given table's batch.
             */
            void add_mutation(int32_t tid);

            /**
             * Records a truncation of a given table into the batch.
             */
            void truncate(int32_t tid);

        private:
            using ChangeList = std::vector<std::pair<PgMsgPtr, uint64_t>>;

            /** Tracks details about a single table within a txn or subtxn. */
            struct TableEntry {
                ExtentPtr extent; ///< The batch of changes to the table thus-far.
                uint64_t start_lsn; ///< The LSN of the first change in the batch.
                ExtentSchemaPtr schema; ///< The schema of the data stored in the batch.
                FieldPtr type_f; ///< The field accessor for the mutation's type.
                FieldPtr lsn_f; ///< The field accessor for the mutation's LSN.
            };
            using TableMap = std::map<int32_t, TableEntry>;

            /** Tracks details about a txn or subtxn. */
            struct TxnEntry {
                int32_t pg_xid; ///< The pgxid of the subtxn
                TableMap table_map; ///< Map from table ID to TableEntry
                ChangeList changes; ///< List of (schema change + LSN) ordered by LSN
            };
            using TxnEntryPtr = std::shared_ptr<TxnEntryPtr>;

            std::map<int32_t, TxnEntryPtr> _txns; ///< Map of pgxid to txn details.

            uint64_t _db; ///< The associated database ID
            int32_t _pg_xid; ///< The top-most pgxid for the transaction
            int32_t _cur_pg_xid; ///< The current pgxid being processed

            uint64_t _lsn = 0; ///< The LSN counter
        };
        using BatchPtr = std::shared_ptr<Batch>;

        std::filesystem::path _current_path; ///< current log file path
        PgMsgStreamReader _reader;           ///< msg stream reader for log file
        PgTransactionQueuePtr _queue;        ///< shared queue for xactions
        PgTransactionPtr _current_xact;      ///< current transaction
        std::map<uint32_t, PgTransactionPtr> _xact_map; ///< in progress xact map

        /** Tracks mutation batches using a map of pgxid -> Extent.  The pgxid is always the
            top-most pgxid and never a subtxn, which are handled within the batch. */
        std::map<int32_t, BatchPtr> _batch_map;
        BatchPtr _current_batch; ///< The batch matching the current pg xid

        /** Process begin message */
        void _process_begin(const PgMsgBegin &begin_msg);

        /** Process commit message */
        void _process_commit(const PgMsgCommit &commit_msg);

        /** Process stream start message */
        void _process_stream_start(const PgMsgStreamStart &start_msg);

        /** Process stream commit message */
        void _process_stream_commit(const PgMsgStreamCommit &commit_msg);

        /** Process stream abort message */
        void _process_stream_abort(const PgMsgStreamAbort &abort_msg);

        /** Process mutations */
        void _process_mutation(const PgMsgPtr &msg);

        /** Process truncation */
        void _process_truncate(const PgMsgTruncate &msg);

        /** Process ddl change message; add oid to xact oid set */
        void _process_ddl(uint32_t oid, int32_t xid, bool is_streaming);
    };
} // namespace springtail::pg_log_mgr
