#pragma once

#include <common/timestamp.hh>

#include <pg_repl/pg_msg_stream.hh>
#include <pg_repl/pg_copy_table.hh>

#include <pg_log_mgr/wal_progress_tracker.hh>
#include <pg_log_mgr/xid_ready.hh>
#include <pg_log_mgr/index_requests_manager.hh>

#include <storage/field.hh>

#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Log reader class.  Reads logs written by PgLogWriter.  Collects the various table
     * mutations into batches which are passed to the WriteCache to make them available to the
     * Committer and FDWs.
     */
    class PgLogReader {
    public:
        /** convenience type for the shared msg queue */
        using PgMsgQueuePtr = std::shared_ptr<ConcurrentQueue<PgMsg>>;

        /** convenience type for the shared transaction queue */
        using CommitterQueuePtr = std::shared_ptr<ConcurrentQueue<committer::XidReady>>;

        /**
         * @brief Construct a new Pg Log Reader object
         * @param db_id           - database id
         * @param queue_size      - size of the message queue owned by this object
         * @param repl_log_path   - path of replication logs directory
         * @param xact_log_path   - path of transaction logs directory
         * @param committer_queue - queue for passing work to committer
         * @param archive_logs    - flag for turning on repl and xact logs archiving
         */
        PgLogReader(uint64_t db_id, uint32_t queue_size,
                    const std::filesystem::path &repl_log_path,
                    const CommitterQueuePtr committer_queue,
                    const bool archive_logs,
                    const std::shared_ptr<IndexRequestsManager> &index_requests_mgr);

        ~PgLogReader();
        /**
         * @brief Queues a message to be processed by the log reader.
         * @param msg The PgMsg object to process.
         */
        void enqueue_msg(PgMsgPtr msg);

        /**
         * @brief Process next set of messages from log file
         * @param path file path
         * @param timestamp log file timestamp
         * @param start_offset starting file offset
         * @param num_messages number of messages to process (-1 read until end of file)
         */
        void process_log(const std::filesystem::path &path,
                         uint64_t timestamp,
                         uint64_t start_offset,
                         int num_messages);

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

        bool archive_logs() const { return _archive_logs; }

        /**
         * @brief Cleanup log files older than the given timestamp
         *
         * @param min_timestamp Min timestamp file to be retained,
         *                      other files will be removed/archived
         */
        void cleanup_log_files(uint64_t min_timestamp);

    private:
        /**
         * Local cache of whether a given table exists or not at the most recently processed XID.
         */
        class ExistsCache {
        public:
            explicit ExistsCache(uint32_t size)
                : _cache(size)
            { }

            /** Checks the cache if the table exists.  If not present, queries the SysTblMgr. */
            bool exists(uint64_t db_id, uint32_t table_id, const XidLsn &xid) {
                Key key(db_id, table_id);
                {
                    std::scoped_lock lock(_mutex);
                    auto entry = _cache.get(key);
                    if (entry) {
                        return *entry;
                    }
                }

                bool exists = sys_tbl_mgr::Client::get_instance()->exists(key.first, key.second, xid);

                {
                    std::scoped_lock lock(_mutex);
                    _cache.insert(key, std::make_shared<bool>(exists));
                }
                return exists;
            }

            /** Updates the local view of table existence. */
            void insert(uint64_t db_id, uint32_t table_id, bool exists) {
                std::scoped_lock lock(_mutex);
                _cache.insert(Key{db_id, table_id}, std::make_shared<bool>(exists));
            }

        private:
            using Key = std::pair<uint64_t, uint32_t>; ///< Pair of (db_id, table_id).
            LruObjectCache<Key, bool> _cache; ///< Cache of exists flags for tables.
            std::mutex _mutex; ///< Mutex to protect the cache.
        };
        using ExistsCachePtr = std::shared_ptr<ExistsCache>;

        /**
         * Maintains the state for a single top-level transaction and all sub-transactions.  Is
         * responsible for batching together mutations and writing them in bulk to the write cache.
         * Tracks schema state during the transaction and applies them once the transaction commits.
         * Also handles subtransaction rollback.
         */
        class Batch {
            // 4 MB
            static constexpr uint32_t MAX_BATCH_SIZE = 4 * 1024 * 1024;

        public:
            Batch(uint64_t db_id, int32_t pg_xid, const CommitterQueuePtr committer_queue,
                  ExistsCachePtr exists_cache, const std::shared_ptr<IndexRequestsManager>& index_requests_mgr)
                : _db(db_id), _pg_xid(pg_xid), _committer_queue(committer_queue),
                _exists_cache(exists_cache), _index_requests_mgr(index_requests_mgr)
            {
                auto tracer = open_telemetry::OpenTelemetry::get_instance()->tracer("PgLogReader");
                _span = tracer->StartSpan("Transaction");
                _span->SetAttribute("pg_xid", pg_xid);
            }

            ~Batch()
            {
                if (_span->IsRecording()) {
                    _span->End();
                }
            }

            /**
             * Send all extents to the WriteCache, apply all schema changes to the SysTblMgr at the
             * provided xid.
             * @param xid springtail xid
             * @param commit_ts Postgres commit ts
             */
            void commit(uint64_t xid, PostgresTimestamp commit_ts);

            /**
             * Abort the entire transaction.  Drop all related batches from the WriteCache.
             */
            void abort(PostgresTimestamp abort_ts);

            /**
             * Aborts a subtxn by removing those entries from the WriteCache, dropping any in-memory
             * extents, and rolling back the set of schema changes.
             */
            void abort_subtxn(int32_t pg_xid, PostgresTimestamp abort_ts);

            /**
             * Adds a mutation to a given table's batch.
             */
            template <int T>
            void add_mutation(uint64_t current_xid, int32_t pg_xid, int32_t tid, PgMsgTupleData &data);

            /**
             * Records a truncation of a given set of tables into the batch.
             */
            void truncate(uint64_t current_xid, int32_t pg_xid, const PgMsgTruncate &msg);

            /**
             * Records a schema change into the batch.
             */
            void schema_change(uint64_t current_xid, std::optional<uint32_t> tid,
                               int32_t oid, uint32_t pg_xid, uint32_t pg_xid_txn, PgMsgPtr msg,
                               const std::vector<std::string>& include_schemas);

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
                explicit TableEntry(ExtentSchemaPtr table_schema)
                    : table_schema(table_schema)
                { }

                /**
                 * Updates the schema and related fields from the table_schema.
                 */
                void update_schema();

                /**
                 * Update the fields and pkey fields for the table.
                 * Called by add_mutation()
                 * @param batch The current batch.
                 * @param xid The XID and LSN of the mutation.
                 */
                void update_fields(Batch *batch, const XidLsn &xid);

                /**
                 * Helper to update a field array.
                 * @param batch The current batch.
                 * @param xid The XID and LSN of the mutation.
                 * @param fields The field array to update.
                 */
                FieldArrayPtr get_pg_fields(Batch *batch,
                                            const XidLsn &xid,
                                            const MutableFieldArrayPtr fields,
                                            const std::vector<std::pair<int32_t, int>> &pg_types);
            };
            using TableMap = std::map<int32_t, TableEntry>;

            /** Tracks details about a txn or subtxn. */
            struct TxnEntry {
                int32_t pg_xid; ///< The pgxid of the subtxn
                TableMap table_map; ///< Map from table ID to TableEntry
                ChangeListPtr changes; ///< List of (schema change + LSN) ordered by LSN

                explicit TxnEntry(int32_t pg_xid)
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
            void _apply_schema_changes(const LsnChangeMap &change_map, uint64_t xid, const std::vector<uint64_t> &pg_xids);

            /**
             * Apply a single schema change to the SysTblMgr.
             */
            void _apply_schema_change(PgMsgPtr change, const XidLsn &xidlsn, const std::vector<uint64_t> &pg_xids);

            /**
             * @brief Helper method to mark the table for resync. Internally calls sync_tracker->mark_resync
             *
             * @param table_oid Table OID
             * @param xidlsn XID LSN
             */
            void _mark_table_resync(uint64_t table_oid, const XidLsn &xidlsn, const std::vector<uint64_t> &pg_xids);

            /**
             * Helper to check if a given table is invalid as visible within this Batch.
             *
             * @param table_oid Table OID.
             */
            bool _check_invalid(uint32_t table_oid);

            /**
             * Helper to handle the table validation management.  Updates the Batch-local view of
             * the invalid tables and also updates the msg object as needed.
             * @param msg The postgres message object.
             * @param include_schemas The included schemas.
             */
            bool _handle_validation(PgMsgPtr msg, const std::vector<std::string>& include_schemas);

            /**
             * Check if the table exists within this batch.
             */
            bool _table_exists(uint32_t table_id, const XidLsn &xid) {
                for (const auto &entry : _txns) {
                    if (entry.second->table_map.contains(table_id)) {
                        return true;
                    }
                }

                return _exists_cache->exists(_db, table_id, xid);
            }

            /**
             * Lookup a user type in the batch and return the schema.
             * @param pg_type The pg type of the field.
             * @param xidlsn The XID and LSN of the message.
             * @return UserTypePtr The user type schema.
             */
            UserTypePtr _usertype_cache_lookup(int32_t pg_type, const XidLsn &xidlsn) {
                auto it = _user_types.find(pg_type);
                if (it != _user_types.end()) {
                    return it->second;
                }
                auto utp = SchemaMgr::get_instance()->get_usertype(_db, pg_type, xidlsn);
                _user_types[pg_type] = utp;
                return utp;
            }

            //// MEMBER VARIABLES
            std::map<int32_t, TxnEntryPtr> _txns; ///< Map of pgxid to txn details.

            uint64_t _db; ///< The associated database ID
            int32_t _pg_xid; ///< The top-most pgxid for the transaction

            int32_t _cur_pg_xid = -1; ///< The current pgxid being processed
            TxnEntryPtr _cur_txn; ///< The TxnEntry of the current txn/subtxn

            uint64_t _lsn = 0; ///< The LSN counter

            open_telemetry::SpanPtr _span; ///< Timing for the txn processing.
            CommitterQueuePtr _committer_queue; ///< Reference to the committer queue

            ExistsCachePtr _exists_cache; ///< Reference to the exists cache
            std::shared_ptr<IndexRequestsManager> _index_requests_mgr;

            /** Records changes in the table validation state based on supported columns.  A valid
                table contains std::nullopt, while an invalid one contains a JSON describing the
                invalid columns.  */
            std::unordered_map<uint32_t, std::optional<nlohmann::json>> _table_validations;

            /** Simple map of pg_type to user defined type.  Currently invalidated on any alteration */
            std::unordered_map<int32_t, UserTypePtr> _user_types; ///< Map of user types to their schema
        };
        using BatchPtr = std::shared_ptr<Batch>;

        uint64_t _pg_log_timestamp{0};      ///< Timestamp id of the current Postgres log
        uint64_t _db_id; ///< The database ID
        uint64_t _committed_xid; ///< The most recently committed XID at startup
        bool _is_streaming{false};     ///< This flag indicates that the log is inside streaming block
        bool _archive_logs{false};     ///< This flag indicates that the reader should archive old logs instead of removing them
        std::filesystem::path _current_path; ///< current log file path
        std::filesystem::path _repl_log_path;   ///< Path for Postgres logs storage directory
        PgMsgStreamReader _reader;     ///< msg stream reader for log file
        CommitterQueuePtr _committer_queue;  ///< shared queue for committer
        PgTransactionPtr _current_xact;      ///< current transaction
        std::map<uint32_t, PgTransactionPtr> _xact_map; ///< in progress xact map for streams

        std::atomic<uint64_t> _next_xid{0};        ///< next xid in xid range

        ConcurrentQueue<PgMsg> _msg_queue; ///< Queue of PgMsg records to process
        std::thread _msg_thread; ///< Thread for processing messages using the _msg_worker()

        /** Tracks mutation batches using a map of pgxid -> Extent.  The pgxid is always the
            top-most pgxid and never a subtxn, which are handled within the batch. */
        std::map<int32_t, BatchPtr> _batch_map;
        BatchPtr _current_batch; ///< The batch matching the current pg xid

        WalProgressTrackerPtr _xid_ts_tracker;      ///< Timestamps and Xids tracker object

        /** Cache indicating if a table exists at the latest XID seen committed by the log reader. */
        ExistsCachePtr _exists_cache;

        /**
         * @brief shared_ptr to the index requests manager to get
         * index requests (create/drop) for an XID per db
         */
        std::shared_ptr<IndexRequestsManager> _index_requests_mgr;

        /** Function for cleaning up old log files. */
        void _remove_old_log_files();

        /** Worker function that processes the messages from the internal queue. */
        void _msg_worker();

        /** Process a PG message */
        void _process_msg(PgMsgPtr msg);

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
        void _process_ddl(std::optional<uint32_t> table_oid, uint32_t oid, int32_t xid, bool is_streaming, PgMsgPtr msg);

        /** Check if we need to perform a table swap / commit and notify the Committer if so. */
        void _check_sync_commit(uint64_t db_id, int32_t pg_xid);

        /** @brief Notify the Committer for an index reconciliation
         * @param db_id DB for which reconcile to be notified
         * @param reconcile_xid XID for which index reconciliation to be done
         */
        void _process_index_reconciliation(const uint64_t db_id, const uint64_t reconcile_xid);
    };
} // namespace springtail::pg_log_mgr
