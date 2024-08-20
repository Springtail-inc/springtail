#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <common/concurrent_queue.hh>
#include <common/counter.hh>
#include <common/redis.hh>
#include <common/redis_ddl.hh>
#include <common/redis_types.hh>

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
            : _readers(reader_count),
              _parsers(parser_count)
        {
            _backlog = std::make_shared<Backlog>();
            _parser_queue = std::make_shared<ParserQueue>();

            for (auto &reader : _readers) {
                reader = std::make_shared<Reader>(redis::QUEUE_PG_TRANSACTIONS, _backlog, _parser_queue);
            }

            for (auto &parser : _parsers) {
                parser = std::make_shared<Parser>(_parser_queue);
            }
        }

        void run()
        {
            for (auto &reader : _readers) {
                _reader_threads.push_back(std::thread(&Reader::run, reader.get()));
            }

            for (auto &parser : _parsers) {
                _parser_threads.push_back(std::thread(&Parser::run, parser.get()));
            }
        }

        void shutdown()
        {
            // signal the readers to shutdown
            for (auto &reader : _readers) {
                reader->shutdown();
            }

            // wait for the readers to complete
            for (auto &thread : _reader_threads) {
                thread.join();
            }

            // once the readers have completed, signal the queue to shutdown
            _parser_queue->shutdown();

            // wait for the parsers to complete
            for (auto &thread : _parser_threads) {
                thread.join();
            }
        }

    private:

        /** Holds the state for processing an XID. */
        struct State {
            std::shared_ptr<pg_log_mgr::PgRedisXactValue> entry; ///< The XID entry from the PG log manager.
            bool process_as_stream; ///< True if the XID is in STREAM mode.
            CounterPtr mutation_count; ///< The number of mutations outstanding to the parsers.
            uint64_t lsn; ///< Maintains the LSN for each mutation within this XID.

            State(const State &state) = default;
            explicit State(std::shared_ptr<pg_log_mgr::PgRedisXactValue> entry)
                : entry(entry),
                  process_as_stream(false),
                  mutation_count(std::make_shared<Counter>()),
                  lsn(0)
            { }
        };
        typedef std::shared_ptr<State> StatePtr;

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

        typedef ConcurrentQueue<ParserEntry> ParserQueue;
        typedef std::shared_ptr<ParserQueue> ParserQueuePtr;


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
            Reader(const std::string &pg_queue_key,
                   BacklogPtr backlog,
                   ParserQueuePtr parser_queue)
                : _shutdown(false),
                  _pg_queue(pg_queue_key),
                  _gc_queue(redis::QUEUE_GC_XID_READY),
                  _backlog(backlog),
                  _parser_queue(parser_queue)
            {
                // XXX it's potentially expensive to construct a generator each time
                _worker_id = boost::uuids::to_string(boost::uuids::random_generator()());
            }

            /**
             * Main loop for the Reader worker threads.
             */
            void run();

            /**
             * Signals shutdown to the main loop.
             */
            void shutdown();

        private:
            /**
             * Checks if the provided XID should be excluded from processing based on the aborted sub-transactions.
             * @return Returns true if the XID should be processed, false otherwise.
             */
            bool _check_xid(uint64_t pg_xid);

            /**
             * Checks if the provided XID should be blocked based on it's use of the table OID.  If
             * it should block, then it adds the state to the backlog and returns true.
             *
             * @return Returns true if the XID was blocked, false otherwise.
             */
            bool _check_backlog(uint64_t db_id, uint64_t xid, uint64_t oid,
                                const std::filesystem::path &file, uint64_t offset);

            /**
             * Process a mutation message (insert, update, delete, truncate).  If the mutation
             * processing must be delayed on an earlier schema change, adds the entry to the backlog
             * and returns a flag indicating that the processing should be blocked.
             */
            bool _process_mutation(uint64_t pg_xid, uint64_t xid, uint64_t rel_id, PgMsgPtr msg,
                                   const std::filesystem::path &file, uint64_t offset);

            /**
             * Records the DDL record into Redis to eventually pass to the FDW.
             */
            void _record_ddl(const XidLsn &xid, const std::string &ddl);

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
            /** Unique ID of the worker.  Used for two-phase commit within the Redis queue. */
            std::string _worker_id;

            /** Shutdown flag. */
            bool _shutdown;

            /** Queue for XID messages from the PG log manager. */
            RedisQueue<pg_log_mgr::PgRedisXactValue> _pg_queue;

            /** Queue for XID messages to the GC committer. */
            RedisQueue<XidReady> _gc_queue;

            /** A shared backlog queue for blocked XIDs. */
            BacklogPtr _backlog;

            /** Queue of mutations for the Parser threads. */
            ParserQueuePtr _parser_queue;

            /** Reader for the PG log files. */
            PgMsgStreamReader _reader;

            /** State machine for processing an XID. */
            StatePtr _state;

            /** For managing the DDL statements in Redis. */
            RedisDDL _redis_ddl;
        };
        typedef std::shared_ptr<Reader> ReaderPtr;


        /**
         * Parses individual messages, identifies which table_id and extent_id they impact, and
         * sends the complete mutation to the write cache.
         */
        class Parser {
        public:
            explicit Parser(ParserQueuePtr parser_queue)
                : _parser_queue(parser_queue),
                  _write_cache(WriteCacheClient::get_instance())
            { }

            /**
             * Main loop for the Parser worker threads.  Will shutdown automatically when the queue
             * is shutdown and drained.
             */
            void run();

        protected:
            /**
             * Packs the provided Postgres tuple into an Extent with a single row containing the data.
             *
             */
            std::shared_ptr<MutableTuple> _pack_extent(ExtentPtr extent, const PgMsgTupleData &data,
                                                       ExtentSchemaPtr schema);

        private:
            ParserQueuePtr _parser_queue;
            WriteCacheClient * const _write_cache;
        };
        typedef std::shared_ptr<Parser> ParserPtr;

    private:
        /** A set of reader objects. */
        std::vector<ReaderPtr> _readers;

        /** A set of reader threads.  Thread operation defined by Reader. */
        std::vector<std::thread> _reader_threads;

        /** A set of parser objects. */
        std::vector<ParserPtr> _parsers;

        /** A pool of parser threads that perform lookups for individual mutations. */
        std::vector<std::thread> _parser_threads;

        /** The queue between the Reader objects and Parser objects. */
        ParserQueuePtr _parser_queue;

        /** The shared backlog among the Readers */
        BacklogPtr _backlog;
    };

}
