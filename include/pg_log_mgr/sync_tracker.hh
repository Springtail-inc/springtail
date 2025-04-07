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
         * Marks that the LogParser has issued a resync request for the given table so that
         * mutations can be ignored.  Once picked up by the copy thread, it is moved to the inflight
         * map.
         *
         * @return true if this is the first table from this sync-set
         */
        bool mark_resync(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Marks that the coppy thread has started the COPY request for this table.  This record is
         * replaced by a full XidRecord when add_sync() is called from the TABLE_SYNC_MSG message
         * coming through the log.
         *
         * @return true if this is the first table from this sync-set
         */
        void mark_inflight(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Add the metadata for a given table sync into the tracker.
         *
         * @return true if this is the first table from this sync-set
         */
        bool add_sync(const PgXactMsg::TableSyncMsg &sync_msg);

        /**
         * Check if there are any sync'd tables to swap/commit.
         * @param db_id The database to check.
         * @param pg_xid The pg_xid of the current transaction.
         * @return An optional XidReady containing the swap/commit details if available.
         */
        std::shared_ptr<committer::XidReady> check_commit(uint64_t db_id, uint32_t pg_xid);

        /**
         * Clears any tables that were part of the swap/commit.
         * @param db_id The database to clear.
         * @param commit_msg The XidReady containing the swap/commit details.
         */
        void clear_tables(uint64_t db_id, const committer::XidReady &commit_msg);

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
        std::pair<bool, bool> should_skip(uint64_t db_id, uint64_t table_id, uint32_t pg_xid) const;

    private:
        using TablePair = std::pair<int32_t, std::shared_ptr<proto::CopyTableInfo>>;

        /**
         * Internal class representing the XID metadata for an individual table sync.
         */
        class XidRecord {
        public:
            explicit XidRecord(const PgXactMsg::TableSyncMsg &sync_msg)
                : _pg_xid(sync_msg.pg_xid),
                  _xmax(sync_msg.xmax),
                  _inflight(sync_msg.xips.begin(), sync_msg.xips.end()),
                  _tids(sync_msg.tids)
            { }

            /**
             * Returns true if the given XID should be skipped given the stored metadata.
             */
            bool
            should_skip(uint32_t pg_xid) const
            {
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

            /**
             * Retrieve the list of tables that were part of this sync.
             */
            const std::vector<TablePair> &tids() const {
                return _tids;
            }

            /**
             * Retrieve the PG xid at which this sync occurred.
             */
            const uint32_t pg_xid() const {
                return _pg_xid;
            }

        private:
            uint32_t _pg_xid; ///< The PG xid at which the sync occurred
            uint32_t _xmax; ///< The XMAX at postgres for the sync transaction
            std::set<uint32_t> _inflight; ///< The in-flight PG xids for the sync txn
            std::vector<TablePair> _tids; ///< The table ids being synced and their associated RPC data
        };

    private:
        /** Mutex to protect access to the _sync_map */
        mutable boost::shared_mutex _mutex;

        /** db -> table -> XidRecord containing table sync details. */
        std::map<uint64_t, std::map<uint64_t, std::shared_ptr<XidRecord>>> _table_map;

        /** db -> pgxid -> XidRecord. */
        std::map<uint64_t, std::map<uint32_t, std::shared_ptr<XidRecord>>> _sync_map;

        /** db -> target XID of sync. */
        std::map<uint64_t, uint64_t> _target_xid_map;

        /** db-> table indicating that a resync was issued but it hasn't been picked up by the copy
            thread yet. */
        std::map<uint64_t, std::map<uint64_t, std::set<XidLsn>>> _resync_map;

        /** db-> table indicating that a copy for the table is in-flight but hasn't completed,
            meaning we haven't seen the TABLE_SYNC_MSG log entry for the table yet. */
        std::map<uint64_t, std::set<uint64_t>> _inflight_map;
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

