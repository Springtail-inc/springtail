#include <boost/thread.hpp>

#include <common/service_register.hh>
#include <common/singleton.hh>
#include <pg_log_mgr/xid_ready.hh>
#include <pg_log_mgr/pg_redis_xact.hh>
#include <proto/pg_copy_table.pb.h>

namespace springtail::pg_log_mgr {
    /**
     * A structure to track the metadata around a table sync.  Used to determine if individual
     * mutations should be skipped due to an ongoing table sync.
     */
    class SyncTracker final : public Singleton<SyncTracker> {
    public:
        /**
         * Helper object to return the data about a table swap from check_commit()
         */
        class SwapRequest {
        public:
            SwapRequest(committer::XidReady::Type type,
                        uint64_t db_id,
                        std::vector<PgCopyResult::TableInfoPtr> &&table_info)
                : _type(type),
                  _db(db_id),
                  _table_info(std::move(table_info))
            {}

            committer::XidReady::Type type() const {
                return _type;
            }

            uint64_t db() const {
                return _db;
            }

            const std::vector<PgCopyResult::TableInfoPtr> &table_info() const {
                return _table_info;
            }

        private:
            committer::XidReady::Type _type;
            uint64_t _db;
            std::vector<PgCopyResult::TableInfoPtr> _table_info;
        };

        /**
         * Helper object to return details about skipping a table via should_skip()
         */
        class SkipDetails {
        public:
            SkipDetails(bool is_syncing, bool should_skip)
                : _is_syncing(is_syncing),
                  _should_skip(should_skip)
            { }

            SkipDetails(bool is_syncing, bool should_skip, std::shared_ptr<ExtentSchema> schema)
                : _is_syncing(is_syncing),
                  _should_skip(should_skip),
                  _schema(schema)
            { }

            bool is_syncing() const {
                return _is_syncing;
            }

            bool should_skip() const {
                return _should_skip;
            }

            std::shared_ptr<ExtentSchema> schema() const {
                return _schema;
            }

        private:
            bool _is_syncing;
            bool _should_skip;
            std::shared_ptr<ExtentSchema> _schema;
        };

    public:
        using CommitterQueuePtr = std::shared_ptr<ConcurrentQueue<committer::XidReady>>;

        /**
         * Block the committer from issuing commits during a table sync.
         */
        void block_commits(uint64_t db_id, CommitterQueuePtr committer_queue);

        /**
         * Issue's a resync request for a table and waits for the table copy to start.
         */
        void issue_resync_and_wait(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Marks that the coppy thread has started the COPY request for this table.  This record is
         * replaced by a full XidRecord when add_sync() is called from the TABLE_SYNC_MSG message
         * coming through the log.
         *
         * @return true if this is the first table from this sync-set
         */
        void mark_inflight(uint64_t db_id, uint64_t table_id, const XidLsn &xid,
                           const PgCopyResultPtr &copy, ExtentSchemaPtr schema);

        /**
         * Add the metadata for a given table sync into the tracker.
         *
         * @return true if this is the first table from this sync-set
         */
        void add_sync(const PgXactMsg::TableSyncMsg &sync_msg);

        /**
         * Check if there are any sync'd tables to swap/commit.
         * @param db_id The database to check.
         * @param pg_xid The pg_xid of the current transaction.
         * @return An optional XidReady containing the swap/commit details if available.
         */
        std::shared_ptr<SwapRequest> check_commit(uint64_t db_id, uint32_t pg_xid);

        /**
         * Clears any tables that were part of the swap/commit.
         * @param swap The details about the swap request.
         */
        void clear_tables(std::shared_ptr<SwapRequest> swap);

        /**
         * Remove a given table from the sync tracker.  Called after we have passed all of the
         * skipable transaction records.  We know this because we will see a COPY_SYNC message
         * come through the postgres replication stream (i.e., via an XactMsg with type
         * XACT_MSG).
         *
         * @return 0 if still in progress, otherwise the max assigned XID among the syncs
         */
        uint64_t clear_syncs(uint64_t db_id, uint32_t pg_xid, uint64_t lp_xid);

        /**
         * Checks if mutations at the given table + xid should be skipped due to an ongoing table sync.
         *
         * @return A pair of booleans.  The first indiciates if their is a sync ongoing for the
         *         requested table.  The second indicates if mutations should be skipped for that
         *         table given the pg_xid.
         */
        SkipDetails should_skip(uint64_t db_id, uint64_t table_id, uint32_t pg_xid) const;

    private:
        /**
         * Internal class representing the PG snapshot details
         */
        class Snapshot {
        public:
            Snapshot() = default;
            Snapshot(uint32_t pg_xid, uint32_t xmax, const std::vector<uint32_t> &xips)
                : _pg_xid(pg_xid),
                  _xmax(xmax),
                  _inflight(xips.begin(), xips.end())
            { }

            /**
             * Returns true if the given XID should be skipped given the stored metadata.
             */
            bool
            should_skip(uint32_t pg_xid) const
            {
                LOG_DEBUG(LOG_PG_LOG_MGR, "pg_xid={} xmax={} inflight={}", pg_xid, _xmax, _inflight.size());

                // do a guess-timate if the pgxid wrapped ahead of xmax
                if (pg_xid < (1 << 26) && _xmax > (1 << 30)) {
                    // we assume that the pg_xid is ahead of xmax
                    return false;
                }

                // now check if xmax wrapped ahead of the pgxid
                if (_xmax < (1 << 26) && pg_xid > (1 << 30)) {
                    // we assume that xmax is ahead of pg_xid
                    if (_inflight.contains(pg_xid)) {
                        return false;
                    }
                    return true;
                }

                // note: from here we assume no wrapping
                // don't skip if the txn came after xmax since it is either in-flight or started after the snapshot
                if (pg_xid >= _xmax) {
                    return false;
                }

                // if the xid came before xmax but was inflight, then don't skip
                if (_inflight.contains(pg_xid)) {
                    return false;
                }
                return true;
            }

            const uint32_t pg_xid() const {
                return _pg_xid;
            }

        protected:
            uint32_t _pg_xid = 0; ///< The PG xid at which the sync occurred
            uint32_t _xmax = 0; ///< The XMAX at postgres for the sync transaction
            std::set<uint32_t> _inflight; ///< The in-flight PG xids for the sync txn
        };

        /**
         * Internal class representing the XID metadata for an individual table sync.
         */
        class XidRecord : public Snapshot {
        public:
            explicit XidRecord(const PgXactMsg::TableSyncMsg &sync_msg)
                : Snapshot(sync_msg.pg_xid, sync_msg.xmax, sync_msg.xips),
                  _tids(sync_msg.tids)
            { }

            /**
             * Retrieve the list of tables that were part of this sync.
             */
            const std::vector<PgCopyResult::TableInfoPtr> &tids() const {
                return _tids;
            }

        private:
            std::vector<PgCopyResult::TableInfoPtr> _tids; ///< The table ids being synced and their associated RPC data
        };

        /** Object to track individual in-flight table copies. */
        class Inflight : public Snapshot {
        public:
            Inflight(uint32_t pg_xid,
                     uint32_t xmax,
                     const std::vector<uint32_t> &xips,
                     ExtentSchemaPtr schema)
                : Snapshot(pg_xid, xmax, xips), _schema(schema)
            { }

            const ExtentSchemaPtr &schema() const {
                return _schema;
            }

        private:
            ExtentSchemaPtr _schema; ///< Schema of the table being synced
        };

        /** Helper object for notifying the log reader. */
        struct Wait {
            bool notified = false;
            std::condition_variable condition;
        };

    private:
        template<class T>
        using DbMap = absl::flat_hash_map<uint64_t, T>;

        template<class T>
        using TableMap = absl::flat_hash_map<uint64_t, T>;

        template<class T>
        using PgXidMap = absl::flat_hash_map<uint32_t, T>;

        /** Mutex to protect access. */
        mutable std::mutex _mutex;

        /** Used to track all of the tables that have completed their sync. */
        DbMap<TableMap<std::shared_ptr<XidRecord>>> _table_map;

        /** Used to track all of the table syncs operating at a given snapshot XID. */
        DbMap<PgXidMap<std::shared_ptr<XidRecord>>> _sync_map;

        /** db-> table indicating that a resync was issued but it hasn't been picked up by the copy
          thread yet. */
        std::map<uint64_t, std::map<uint64_t, std::set<XidLsn>>> _resync_map;

        /** Entry is added here when a copy for the table is in-flight but hasn't completed. */
        DbMap<TableMap<std::shared_ptr<Inflight>>> _inflight_map;

        /** PgLogReader waits for copy to start here. */
        DbMap<std::shared_ptr<Wait>> _wait_map;
    };

    class SyncTrackerRunner : public ServiceRunner {
    public:
        SyncTrackerRunner() : ServiceRunner("SyncTracker") {}

        ~SyncTrackerRunner() override = default;

        bool start() override
        {
            SyncTracker::get_instance();
            return true;
        }

        void stop() override
        {
            SyncTracker::shutdown();
        }
    };

}

