#pragma once

#include <memory>
#include <fstream>
#include <filesystem>
#include <map>
#include <vector>

#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/span.h>

#include <common/concurrent_queue.hh>
#include <common/redis_types.hh>

#include <garbage_collector/xid_ready.hh>

#include <pg_repl/pg_repl_msg.hh>

#include <redis/redis_containers.hh>
#include <redis/redis_ddl.hh>

#include <storage/extent.hh>
#include <storage/field.hh>

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
        PgLogReader(uint64_t db_id,
                    const PgTransactionQueuePtr queue)
            : _db_id(db_id),
              _queue(queue)
        { }

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

        /**
         * Set the starting point for XID assignment.
         */
        void set_next_xid(uint64_t xid) {
            _next_xid = xid;
        }

        /**
         * Returns the most recently assigned XID.
         */
        uint64_t get_current_xid() const {
            return _next_xid - 1;
        }

        /** Get next xid */
        uint64_t get_next_xid() {
            return _next_xid.fetch_add(1, std::memory_order_relaxed);
        }

    private:
        class Batch {
            // 4 MB
            static constexpr uint32_t MAX_BATCH_SIZE = 4 * 1024 * 1024;

        public:
            Batch(uint64_t db_id, int32_t pg_xid)
                : _db(db_id), _pg_xid(pg_xid)
            {
                auto provider = opentelemetry::trace::Provider::GetTracerProvider();
                auto tracer = provider->GetTracer("PgLogReader");
                _span = tracer->StartSpan("Transaction");
                _span->SetAttribute("pg_xid", pg_xid);
            }

            /**
             * Send all extents to the WriteCache, apply all schema changes to the SysTblMgr at the
             * provided xid.
             */
            void commit(uint64_t xid);

            /**
             * Abort the entire transaction.  Drop all related batches from the WriteCache.
             */
            void abort();

            /**
             * Aborts a subtxn by removing those entries from the WriteCache, dropping any in-memory
             * extents, and rolling back the set of schema changes.
             */
            void abort_subtxn(int32_t pg_xid);

            /**
             * Adds a mutation to a given table's batch.
             */
            template <int T>
            void add_mutation(uint64_t current_xid, int32_t pg_xid, int32_t tid, const PgMsgTupleData &data);

            /**
             * Records a truncation of a given set of tables into the batch.
             */
            void truncate(uint64_t current_xid, const PgMsgTruncate &msg);

            /**
             * Records a schema change into the batch.
             */
            void schema_change(int32_t tid, int32_t pg_xid, PgMsgPtr msg);

        private:
            //// INTERNAL STRUCTURES
            using ChangeEntry = std::pair<PgMsgPtr, uint64_t>;
            using ChangeList = std::list<ChangeEntry>;
            using ChangeListPtr = std::shared_ptr<ChangeList>;
            using LsnChangeMap = std::map<uint64_t, ChangeListPtr>;

            /** Tracks details about a single table within a txn or subtxn. */
            struct TableEntry {
                ExtentPtr extent; ///< The batch of changes to the table thus-far.
                uint64_t start_lsn; ///< The LSN of the first change in the batch.

                ExtentSchemaPtr table_schema; ///< The schema of the data stored in the table.
                ExtentSchemaPtr schema; ///< The schema of the data stored in the batch.

                MutableFieldPtr op_f; ///< The field accessor for the mutation's operation type.
                MutableFieldPtr lsn_f; ///< The field accessor for the mutation's LSN.
                MutableFieldArrayPtr fields; ///< The underlying fields of the schema that match the columns from the table schema.
                MutableFieldArrayPtr pkey_fields; ///< The underlying fields of the schema that match the pkey columns from the table schema.

                FieldArrayPtr pg_fields; ///< The matching fields for processing a PgMsgTupleData
                FieldArrayPtr pg_pkey_fields; ///< The matching pkey fields for processing a PgMsgTupleData

                TableEntry() = default;
                TableEntry(ExtentSchemaPtr table_schema)
                    : table_schema(table_schema)
                { }

                /** Updates the schema and related fields from the table_schema. */
                void update_schema();
            };
            using TableMap = std::map<int32_t, TableEntry>;

            /** Tracks details about a txn or subtxn. */
            struct TxnEntry {
                int32_t pg_xid; ///< The pgxid of the subtxn
                TableMap table_map; ///< Map from table ID to TableEntry
                ChangeListPtr changes; ///< List of (schema change + LSN) ordered by LSN

                TxnEntry(int32_t pg_xid)
                    : pg_xid(pg_xid)
                { }
            };
            using TxnEntryPtr = std::shared_ptr<TxnEntry>;

            //// HELPER FUNCTIONS
            /**
             * Retrieve a pointer to the given TxnEntry.  Starts a new subtxn if necessary.
             */
            TxnEntryPtr _get_txn(int32_t pg_xid);

            /**
             * Start a new subtxn.
             */
            TxnEntryPtr _start_subtxn(int32_t pg_xid);

            /**
             * Applies the schema changes from txns / subtxns in LSN order to the SysTblMgr.
             */
            void _apply_schema_changes(const LsnChangeMap &change_map, uint64_t xid);


            //// MEMBER VARIABLES
            std::map<int32_t, TxnEntryPtr> _txns; ///< Map of pgxid to txn details.

            uint64_t _db; ///< The associated database ID
            int32_t _pg_xid; ///< The top-most pgxid for the transaction

            int32_t _cur_pg_xid = -1; ///< The current pgxid being processed
            TxnEntryPtr _cur_txn; ///< The TxnEntry of the current txn/subtxn

            uint64_t _lsn = 0; ///< The LSN counter

            opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> _span; ///< Timing for the txn processing.
        };
        using BatchPtr = std::shared_ptr<Batch>;

        uint64_t _db_id; ///< The database ID
        std::filesystem::path _current_path; ///< current log file path
        PgMsgStreamReader _reader;           ///< msg stream reader for log file
        PgTransactionQueuePtr _queue;        ///< shared queue for xactions
        PgTransactionPtr _current_xact;      ///< current transaction
        std::map<uint32_t, PgTransactionPtr> _xact_map; ///< in progress xact map
        std::atomic<uint64_t> _next_xid{0};        ///< next xid in xid range

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

        /** Process ddl change message; add oid to xact oid set */
        void _process_ddl(uint32_t oid, int32_t xid, bool is_streaming, PgMsgPtr msg);
    };
} // namespace springtail::pg_log_mgr
