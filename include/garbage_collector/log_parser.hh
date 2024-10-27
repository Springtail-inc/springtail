#pragma once

#include <boost/thread.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <fmt/format.h>

#include <common/concurrent_queue.hh>
#include <common/counter.hh>
#include <common/redis.hh>
#include <common/redis_ddl.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>

#include <garbage_collector/xid_ready.hh>

#include <pg_log_mgr/pg_redis_xact.hh>

#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_msg_stream.hh>

#include <storage/field.hh>

#include <write_cache/write_cache_client.hh>

namespace springtail::gc {

    /**
     * Class to handle parsing the log and creating entries for the write cache.
     *
     * The logic for the GC log parser is as follows:
     *   1. Receive a message from the PG log manager that an XID is ready to be processed
     *   2. Scan the data in the log for that XID
     *   3. Generate write cache entries from the log entries by looking up the affected extent_id for each entry
     *   4. Send the data to the write cache
     *
     * The work of the LogParser is performed in two phases.  In the first phase, a Reader accepts
     * a message about a complete XID from the log manager, which scans the logs for mutations which
     * as passed individually to the second phase.  In the second phase, a Parser accepts mutations
     * from the Reader, performs a lookup for the affected extent IDs, and submits the mutations to
     * the write cache.  Because this work is highly parallelizable, we maintain a pool of Reader
     * and Parser threads to perform the work.  The only blocking operations are schema changes,
     * which can cause XID processing to be blocked until those mutations are found and processed
     * within the log.
     */
    class LogParser {
    public:
        LogParser(uint32_t reader_count,
                  uint32_t parser_count)
            : _parser_queue(std::make_shared<ParserQueue>()),
              _reader(_parser_queue),
              _reader_threads(reader_count),
              _parser(_parser_queue),
              _parser_threads(parser_count)
        { }

        /**
         * Start the LogParser service.  Performs any cleanup from a previously failed run,
         * registers with the coordinator, and begins the service threads.
         */
        void run();

        /**
         * Shuts down the service threads and de-registers them from the coordinator.
         */
        void shutdown();

        /** Perform cleanup on a failed thread. */
        void cleanup();

    private:

        /** Holds the state for processing an XID. */
        struct State {
            std::shared_ptr<pg_log_mgr::PgXactMsg> msg;
            pg_log_mgr::PgXactMsg::XactMsg &entry; ///< The XID entry from the PG log manager.
            bool process_as_stream; ///< True if the XID is in STREAM mode.
            CounterPtr mutation_count; ///< The number of mutations outstanding to the parsers.
            uint64_t lsn; ///< Maintains the LSN for each mutation within this XID.

            State(const State &state) = default;
            explicit State(std::shared_ptr<pg_log_mgr::PgXactMsg> msg)
                : msg(msg),
                  entry(std::get<pg_log_mgr::PgXactMsg::XactMsg>(msg->msg)),
                  process_as_stream(false),
                  mutation_count(std::make_shared<Counter>()),
                  lsn(0)
            { }
        };
        using StatePtr = std::shared_ptr<State>;

        /**
         * Maintains two structures:
         * 1) The dependency map of which XIDs contain schema changes for which table OIDs
         * 2) A backlog of XIDs that are blocked on schema changes to a given OID in earlier XIDs
         *
         * Using these structures, we determine when an XID can continue processing and make it
         * available to the reader threads.
         */
        class Backlog {
        public:
            /**
             * Checks if the backlog is empty.
             * @return true if the backlog is empty, false otherwise
             */
            bool empty() const;

            /**
             * Checks if the given XID should block when mutating the given table OID.
             * @param db_id The DB of the table this request is accessing.
             * @param oid The OID of the table this request is accessing.
             * @param xid The XID of this request.
             * @return true if the request should block, false otherwise.
             */
            bool check(uint64_t db_id, uint64_t oid, uint64_t xid);

            /**
             * Adds the blocked request to the backlog.  Indicates which XID it is, which OID it's
             * blocked on, and the request details.
             * @param db_id The DB this XID is blocking on.
             * @param oid The OID this XID is blocking on.
             * @param xid The XID of this request.
             * @param entry The request details.
             */
            void push(uint64_t db_id, uint64_t oid, uint64_t xid, StatePtr entry);

            /**
             * Retreives a request that was previously blocked but is no longer blocked.
             * @return The request details, nullptr if there are no ready requests.
             */
            StatePtr pop();

            /**
             * Updates the XID / table dependencies for a given DB from Redis.
             */
            void update_deps(uint64_t db_id);

            /**
             * Clears any requests blocked on the provided XID.
             * @param db_id The DB of the XID that completed.
             * @param xid The XID that completed, allowing other blocked requests to proceed.
             */
            void clear_dep(uint64_t db_id, uint64_t xid);

            /**
             * Clears any requests blocked on the provided table.  Used to unblock processing that
             * was blocked on a table that has been selected for resync.
             * @param db_id The DB of the table.
             * @param oid The OID of the table.
             */
            void clear_table(uint64_t db_id, uint64_t oid);

        private:
            /**
             * Unblocks any XIDs that were waiting on the table.  Optionally only releases XIDs
             * before a provided "next dependent XID," allowing earlier XIDs to proceed since they
             * can't be dependent on changes in later XIDs.
             * @param db_id The DB of the table.
             * @param oid The OID of the table.
             * @param next_dep_xid If non-zero, used to limit the set of XIDs that are unblocked.
             */
            void _unblock_xids(uint64_t db_id, uint64_t oid, uint64_t next_dep_xid = 0);

        private:
            using DbXid = std::pair<uint64_t, uint64_t>;
            using DbOid = std::pair<uint64_t, uint64_t>;

            /** Guard on access to the members. */
            mutable boost::shared_mutex _mutex;

            /** List of requests that are ready to be processed. */
            std::list<StatePtr> _ready;

            /** Map of blocked XIDs -- XID -> <OID, Request> */
            std::map<DbXid, std::pair<uint64_t, StatePtr>> _backlog;

            /** Map from a given table OID to the set of blocked XIDs. */
            std::map<DbOid, std::set<uint64_t>> _oid_backlog;

            /** Map of table dependencies -- OID -> <ordered set of XIDs> */
            std::map<DbOid, std::set<uint64_t>> _table_deps;

            /** Map of XID to the OIDs it modifies for removing dependencies. */
            std::map<DbXid, std::set<uint64_t>> _xid_map;

            /** The last XID that was requested from Redis for each DB. */
            std::map<uint64_t, uint64_t> _last_requested_xid;

            /** The interface to Redis to retrieve the table dependencies for each DB. */
            std::map<uint64_t, pg_log_mgr::RSSOidValue> _oid_set;
        };
        using BacklogPtr = std::shared_ptr<Backlog>;

        /**
         * A structure to track the metadata around a table sync.  Used to determine if individual
         * mutations should be skipped due to an ongoing table sync.
         */
        struct SyncTracker {
        public:
            /**
             * Marks that the LogParser has issued a resync request for the given table so that
             * mutations can be ignored.  This record is replaced by a full XidRecord when
             * add_sync() is called from the message coming through the log.
             *
             * @return true if this is the first table from this sync-set
             */
            bool mark_resync(uint64_t db_id, uint64_t table_id);

            /**
             * Add the metadata for a given table sync into the tracker.
             *
             * @return true if this is the first table from this sync-set
             */
            bool add_sync(const pg_log_mgr::PgXactMsg::TableSyncMsg &sync_msg);

            /**
             * Remove a given table from the sync tracker.  Called after we have passed all of the
             * skipable transaction records.
             *
             * @return 0 if still in progress, otherwise the max assigned XID among the syncs
             */
            uint64_t clear_syncs(uint64_t db_id, uint32_t pg_xid);

            /**
             * Checks if mutations at the given table + xid should be skipped due to an ongoing table sync.
             */
            bool should_skip(uint64_t db_id, uint64_t table_id, uint32_t pg_xid) const;

            /**
             * Checks if the tracker has any entries for a given database.
             * 
             * @return 0 if still in progress, otherwise the max XID seen among the syncs
             */
            uint64_t get_xid_if_empty(uint64_t db_id);

        private:
            /**
             * Internal class representing the XID metadata for an individual table sync.
             */
            class XidRecord {
            public:
                XidRecord(const pg_log_mgr::PgXactMsg::TableSyncMsg &sync_msg)
                    : _xmax(sync_msg.xmax),
                      _inflight(sync_msg.xips.begin(), sync_msg.xips.end())
                { }

                /**
                 * Returns true if the given XID should be skipped given the stored metadata.
                 */
                bool
                should_skip(uint32_t pg_xid) const
                {
                    // do a guess-timate if the pgxid wrapped ahead of xmax
                    if (pg_xid < (2 << 26) && _xmax > (2 << 30)) {
                        // we assume that the pg_xid is ahead of xmax
                        return false;
                    }

                    // now check if xmax wrapped ahead of the pgxid
                    if (_xmax < (2 << 26) && pg_xid > (2 << 30)) {
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

            private:
                uint32_t _xmax;
                std::set<uint32_t> _inflight;
            };

        private:
            /** Mutex to protect access to the _sync_map */
            mutable boost::shared_mutex _mutex;

            /** db -> table -> XidRecord containing table sync details. */
            std::map<uint64_t, std::map<uint64_t, std::shared_ptr<XidRecord>>> _sync_map;

            /** db-> table indicating that a resync was issued but we haven't seen the
                TABLE_SYNC_MSG log entry for the table yet. */
            std::map<uint64_t, std::set<uint64_t>> _resync_map;

            /** db -> xid hold the max target XID seen in the sync-set for a given db. */
            std::map<uint64_t, uint64_t> _max_xid;
        };


        /** A message from a Reader to a Parser. */
        struct ParserEntry {
            PgMsgPtr msg;
            CounterPtr counter;
            uint64_t xid;
            uint64_t lsn;
            uint64_t table_id;
            uint64_t db_id;

            ParserEntry(PgMsgPtr msg, CounterPtr counter, uint64_t xid, uint64_t lsn, uint64_t table_id, uint64_t db_id)
                : msg(msg), counter(counter), xid(xid), lsn(lsn), table_id(table_id), db_id(db_id)
            { }
        };
        using ParserQueue = ConcurrentQueue<ParserEntry>;
        using ParserQueuePtr = std::shared_ptr<ParserQueue>;

        /**
         * Reads XID commit messages and processes them by reading the individual WAL entries from
         * stable storage and handing the individual mutations to the Parser threads for processing.
         * If a mutation is blocked on a schema change from an earlier XID then we place the XID
         * commit onto a backlog until those schema changes have been processed.  For this reason,
         * the Reader threads first attempt to get XID commit messages from the backlog, and then
         * from the RedisQueue.
         */
        class Reader {
        public:
            Reader(ParserQueuePtr parser_queue)
                : _shutdown(false),
                  _gc_queue(fmt::format(redis::QUEUE_GC_XID_READY, Properties::get_db_instance_id())),
                  _parser_notify(fmt::format(redis::QUEUE_GC_PARSER_NOTIFY, Properties::get_db_instance_id())),
                  _reader_queue(fmt::format(redis::QUEUE_PG_TRANSACTIONS, Properties::get_db_instance_id())),
                  _parser_queue(parser_queue)
            { }

            /**
             * Main loop for the Reader worker threads.
             */
            void run(int thread_id);

            /**
             * Signals shutdown to the main loop.
             */
            void shutdown();

        private:
            /**
             * Checks if the provided XID should be excluded from processing based on it being part
             * of an aborted sub-transaction.
             * 
             * @return Returns true if the XID was aborted, false otherwise.
             */
            bool _check_aborted_xid(StatePtr state, uint64_t pg_xid);

            /**
             * Checks if the provided XID should be blocked based on it's use of the table OID.  If
             * it should block, then it adds the state to the backlog and returns true.
             *
             * @return Returns true if the XID was blocked, false otherwise.
             */
            bool _check_backlog(StatePtr state, uint64_t oid, uint64_t offset);

            /**
             * Process a mutation message (insert, update, delete, truncate).  If the mutation
             * processing must be delayed on an earlier schema change, adds the entry to the backlog
             * and returns a flag indicating that the processing should be blocked.
             */
            bool _process_mutation(StatePtr state, uint64_t rel_id, PgMsgPtr msg, uint64_t offset);

        private:
            /** The filter to use at the start of processing. */
            static const std::vector<char> START_FILTER;

            /** The filter to use after seeing a BEGIN message. */
            static const std::vector<char> BEGIN_FILTER;

            /** The filter to use after seeing a STREAM_START message. */
            static const std::vector<char> STREAM_START_FILTER;

            /** The filter to use after seeing a STREAM_STOP message. */
            static const std::vector<char> STREAM_STOP_FILTER;

        private:
            /** Shutdown flag. */
            std::atomic<bool> _shutdown;

            /** Queue for XID messages to the GC-2 committer. */
            RedisQueue<XidReady> _gc_queue;

            /** Queue for notify messages from the GC-2 committer back to the GC-1 after a table sync commit. */
            RedisQueue<XidReady> _parser_notify;

            /** Queue to pass messages from the PgLogMgr to the Reader threads. */
            RedisQueue<pg_log_mgr::PgXactMsg> _reader_queue;

            /** A backlog queue for blocked XIDs. */
            Backlog _backlog;

            /** Queue of mutations for the Parser threads. */
            ParserQueuePtr _parser_queue;

            /** Reader for the PG log files. */
            PgMsgStreamReader _reader;

            /** For managing the DDL statements in Redis. */
            RedisDDL _redis_ddl;

            /** Metadata for in-flight table syncs. */
            SyncTracker _sync_tracker;

            /** Mutex to control the critical sections of the reader run() loop. */
            boost::mutex _mutex;

            /** Map of XID -> condition variable.  Used to ensure XIDs are passed to the Committer in-order. */
            std::map<uint64_t, boost::condition_variable> _xid_map;
        };


        /**
         * Parses individual messages, identifies which table_id and extent_id they impact, and
         * sends the complete mutation to the write cache.
         */
        class Parser {
        public:
            explicit Parser(ParserQueuePtr parser_queue)
                : _parser_queue(parser_queue)
            { }

            /**
             * Main loop for the Parser worker threads.  Will shutdown automatically when the queue
             * is shutdown and drained.
             */
            void run(int thread_id);

        protected:
            /**
             * Packs the provided Postgres tuple into an Extent with a single row containing the data.
             *
             */
            std::shared_ptr<MutableTuple> _pack_extent(ExtentPtr extent, const PgMsgTupleData &data,
                                                       ExtentSchemaPtr schema);

        private:
            ParserQueuePtr _parser_queue;
        };
        using ParserPtr = std::shared_ptr<Parser>;

    private:
        /** The queue between the Reader objects and Parser objects. */
        ParserQueuePtr _parser_queue;

        /** The reader object. */
        Reader _reader;

        /** A set of reader threads.  Thread operation defined by Reader. */
        std::vector<std::thread> _reader_threads;

        /** The parser object. */
        Parser _parser;

        /** A pool of parser threads that perform lookups for individual mutations. */
        std::vector<std::thread> _parser_threads;
    };

}
