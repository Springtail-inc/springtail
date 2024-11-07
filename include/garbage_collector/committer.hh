#pragma once

#include <thread>

#include <boost/thread.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <common/concurrent_queue.hh>
#include <common/constants.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>

#include <redis/redis_ddl.hh>
#include <redis/redis_containers.hh>

#include <garbage_collector/xid_ready.hh>

#include <sys_tbl_mgr/table.hh>
#include <write_cache/write_cache_client.hh>
#include <xid_mgr/xid_mgr_client.hh>

namespace springtail::gc {
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
        Committer(uint32_t worker_count)
            : _redis(fmt::format(redis::QUEUE_GC_XID_READY, Properties::get_db_instance_id())),
              _parser_notify(fmt::format(redis::QUEUE_GC_PARSER_NOTIFY, Properties::get_db_instance_id())),
              _worker_count(worker_count)
        {
            _xid_mgr = XidMgrClient::get_instance();
            _write_cache = WriteCacheClient::get_instance();
            _worker_id = fmt::format("{}_{}_0", THREAD_TYPE, THREAD_MAIN);
        }

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
         * The structure that defines a worker job.
         */
        struct WorkerEntry {
            MutableTablePtr table;
            uint64_t extent_id;
            uint64_t xid;
            uint64_t txid; ///< The XID at which a truncate took place.  Zero if none.
            uint64_t tlsn; ///< The LSN at which a truncate took place.  Zero if none.
            bool do_finalize;

            WorkerEntry(MutableTablePtr table, uint64_t extent_id, uint64_t xid, uint64_t txid, uint64_t tlsn)
                : table(table),
                  extent_id(extent_id),
                  xid(xid),
                  txid(txid),
                  tlsn(tlsn),
                  do_finalize(false)
            { }

            WorkerEntry(MutableTablePtr table, uint64_t xid)
                : table(table),
                  extent_id(constant::UNKNOWN_EXTENT),
                  xid(xid),
                  txid(0),
                  tlsn(0),
                  do_finalize(true)
            { }
        };

        /**
         * The worker thread main loop.
         */
        void _run_worker(int thread_id);

        /**
         * Worker helper function to process a finalize() on a given table.
         */
        void _process_finalize(MutableTablePtr table, uint64_t xid);

        /**
         * Worker helper function to process mutations to a given extent ID.
         */
        void _process_rows(MutableTablePtr table, uint64_t extent_id, uint64_t xid,
                           uint64_t txid, uint64_t tlsn);

        /**
         * Worker helper function to process mutations to a table with no primary key.
         */
        void _process_rows_no_primary(MutableTablePtr table, uint64_t xid,
                                      uint64_t txid, uint64_t tlsn);

        /**
         * Helper function to find the enclosing page for a key given an ordered set of contiguous
         * pages.
         */
        using SafePageIter = std::vector<StorageCache::SafePagePtr>::iterator;
        SafePageIter _find_page(std::vector<StorageCache::SafePagePtr>& pages,
                                         TuplePtr key, ExtentSchemaPtr schema);

        /**
         * Shifts the provided metadata to start at the new future XID.  Returns true if the
         * metadata was modified, false otherwise.
         */
        bool _shift_to_xid(SchemaMetadata &meta, const XidLsn &xid);

    private:
        XidMgrClient *_xid_mgr; ///< Pointer to the XidMgr client singleton.
        WriteCacheClient *_write_cache; ///< Pointer to the WriteCache client singleton.

        RedisQueue<XidReady> _redis; ///< The redis queue to communicate between the LogParser and the Committer.

        /** Queue for notify messages from the GC-2 committer back to the GC-1 after a table sync commit. */
        RedisQueue<XidReady> _parser_notify;

        RedisDDL _redis_ddl; ///< The interfaces to manage the DDL statements in Redis.
        std::string _worker_id; ///< Unique worker ID for the Committer.

        uint32_t _worker_count;
        ConcurrentQueue<WorkerEntry> _worker_queue; ///< The queue of work for the worker threads.
        std::vector<std::thread> _worker_threads; ///< The worker threads.

        std::atomic<uint64_t> _shutdown = false; ///< Causes the committer to shut down when set to true.

        boost::mutex _mutex; ///< Mutex to protect internal maps.
        boost::condition_variable _cv; ///< Condition variable to notify from the workers back to the main loop

        /** Map from TID -> the number of outstanding extents to process.  A value of 0 indicates
            that all extents have been processed.  A value of -1 indicates that the table has been
            finalized. */
        std::map<uint64_t, int64_t> _tid_count;

        /** Cache of mutable tables that are in-flight. */
        std::map<uint64_t, MutableTablePtr> _table_map;

        /** The most recently completed XID by db_id.  Note: if we are in a table sync, this may be
            ahead of the most recently committed XID at the XidMgr. */
        std::map<uint64_t, uint64_t> _completed_xids;

        /** The set of databases that are currently not committing XIDs because they are in a table
            sync state. */
        std::set<uint64_t> _block_commit;
    };
}
