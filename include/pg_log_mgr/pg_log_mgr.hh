#pragma once

#include <iostream>
#include <memory>
#include <filesystem>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include <fmt/format.h>

#include <common/concurrent_queue.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/filesystem.hh>
#include <common/properties.hh>
#include <common/state_synchronizer.hh>

#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_copy_table.hh>

#include <pg_log_mgr/pg_log_queue.hh>
#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

#include <pg_log_mgr/pg_redis_xact.hh>

#include <redis/db_state_change.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Postgres log manager
     * Manages pipeline of postgres replication messages.
     * - creates replication connection and starts streaming
     * - log writer reads replication stream and writes data into log files
     * - log reader reads log files and parses begin/commit messages to extract transactions
     * - xact logger writes out transaction level logs
     *   - resolves pg xids to springtail xids via xid mgr
     *   - queues request to GC
     */
    class PgLogMgr {
    public:
        /** convenience type for the shared transaction queue */
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgTransaction>>;
        using StringPtr = std::shared_ptr<std::string>;

        /** replication and transaction log prefixes and suffix */
        static constexpr char const * const LOG_PREFIX_REPL = "pg_log_repl_";
        static constexpr char const * const LOG_PREFIX_XACT = "pg_log_xact_";
        static constexpr char const * const LOG_SUFFIX = ".log";

        /** redis worker id for redis sync queue */
        static constexpr char const * const REDIS_WORKER_ID = "pg_log_mgr";

        /** coordinator thread worker ids arg=db_id */
        static constexpr char const * const WRITER_WORKER_ID = "writer_{}";
        static constexpr char const * const READER_WORKER_ID = "reader_{}";
        static constexpr char const * const XACT_WORKER_ID = "xact_{}";

        /**
         * @brief Construct a new Pg Log Mgr object
         * @param db_id db id
         * @param repl_log_path replication log base path
         * @param xact_log_path transaction log base path
         * @param host postgres host
         * @param db_name postgres db name
         * @param user_name postgres user name
         * @param password postgres password
         * @param pub_name publication name
         * @param slot_name replication slot name
         * @param port postgres port
         */
        PgLogMgr(uint64_t db_id,
                 const std::filesystem::path &repl_log_path,
                 const std::filesystem::path &xact_log_path,
                 const std::string &host, const std::string &db_name,
                 const std::string &user_name, const std::string &password,
                 const std::string &pub_name, const std::string &slot_name,
                 int port)
        : _db_id(db_id), _db_instance_id(Properties::get_db_instance_id()),
          _host(host), _db_name(db_name), _user_name(user_name),
          _password(password), _pub_name(pub_name), _slot_name(slot_name), _port(port),
          _pg_conn(_port, _host, _db_name, _user_name, _password, _pub_name, _slot_name),
          _repl_log_path(repl_log_path),
          _xact_queue(std::make_shared<ConcurrentQueue<PgTransaction>>()),
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path),
          _redis_queue(fmt::format(redis::QUEUE_PG_TRANSACTIONS, _db_instance_id)),
          _redis_oid_set(fmt::format(redis::SET_PG_OID_XIDS, _db_instance_id, _db_id)),
          _redis_sync_queue(fmt::format(redis::QUEUE_SYNC_TABLES, _db_instance_id, _db_id))
        {}

        /**
         * @brief Construct a new Pg Log Mgr object (for testing only)
         * @param repl_log_path replication log base path
         * @param xact_log_path transaction log base path
         */
        PgLogMgr(const std::filesystem::path &repl_log_path,
                 const std::filesystem::path &xact_log_path)
        : _db_id(1), _db_instance_id(Properties::get_db_instance_id()),
          _internal_state(STATE_RUNNING),
          _repl_log_path(repl_log_path),
          _xact_queue(std::make_shared<ConcurrentQueue<PgTransaction>>()),
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path),
          _redis_queue(fmt::format(redis::QUEUE_PG_TRANSACTIONS, _db_instance_id)),
          _redis_oid_set(fmt::format(redis::SET_PG_OID_XIDS, _db_instance_id, _db_id)),
          _redis_sync_queue(fmt::format(redis::QUEUE_SYNC_TABLES, _db_instance_id, _db_id))
        {}

        /** Start the pipeline; setup the log reader/writer log files etc. */
        void startup();

        /** Wait for threads */
        void join() {
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "joining threads");
            _writer_thread.join();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "writer thread joined");
            _reader_thread.join();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "reader thread joined");
            _xact_thread.join();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "xact thread joined");
            _table_copy_thread.join();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "copy thread joined");
            _pubsub_thread.join();
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "pubsub thread joined");
        }

        /** Set shutdown flag */
        void shutdown() {
            SPDLOG_DEBUG_MODULE(LOG_PG_LOG_MGR, "shutting down");
            _shutdown = true;

            // set shutdown flag in pg connection repl class
            _pg_conn.shutdown();
        }

    protected:
        /** Helper to create log writer -- one per log file */
        PgLogWriterPtr _create_repl_logger();

        /** Create xact log writer */
        PgXactLogWriterPtr _create_xact_logger();

        /** Process transaction record -- write it to log and to Redis queue */
        void _process_xact(const PgTransactionPtr xact);

    private:
        /** minimum size for log rollover */
        static constexpr int LOG_ROLLOVER_SIZE_BYTES = 128 * 1024 * 1024;

        static constexpr int MAX_REDIS_BATCH_SIZE = 300;

        /** internal state */
        enum StateEnum {
            STATE_STARTUP,      ///< initial state upon startup
            STATE_STARTUP_SYNC, ///< full sync required after startup
            STATE_RUNNING,      ///< running state
            STATE_SYNC_STALL,   ///< stall state during sync
            STATE_SYNCING,      ///< syncing state (doing table copies)
            STATE_REPLAYING,    ///< replaying state (replaying logs)
            STATE_REPLAY_DONE,  ///< replay done, waiting for running from GC
            STATE_STOPPED
        };

        uint64_t _db_id;                      ///< db id
        uint64_t _db_instance_id;             ///< db instance id

        // connection params
        std::string _host;
        std::string _db_name;
        std::string _user_name;
        std::string _password;
        std::string _pub_name;
        std::string _slot_name;
        int _port;

        /** Internal state synchronizer */
        common::StateSynchronizer<StateEnum> _internal_state{STATE_STARTUP};

        PgReplConnection _pg_conn;            ///< postgres replication connection
        int _proto_version;                   ///< postgres protocol version
        std::atomic<bool> _shutdown{false};   ///< shutdown flag

        ///// Startup
        /** init startup, clear out all state */
        void _startup_init();

        /** normal startup from running state */
        uint64_t _startup_running();

        /** Setup streaming and startup threads */
        void _start_streaming(uint64_t lsn = INVALID_LSN);

        ///// Stage 1 of pipeline, writing replication log to disk
        std::thread _writer_thread;           ///< log writer thread
        std::filesystem::path _repl_log_path; ///< replication log base path
        PgLogQueue _logger_queue;             ///< queue between writer and reader

        /** Process data from replication stream in loop, queue path, offsets */
        void _log_writer_thread();

        /** callback from log writer class to update lsn from fsync thread*/
        void _lsn_callback(LSN_t lsn);

        ///// Stage 2 of pipeline, reading replication log and parsing xacts
        std::thread _reader_thread;         ///< log reader thread
        PgTransactionQueuePtr _xact_queue;  ///< queue between reader and xact thread
        PgLogReader _pg_log_reader;         ///< log reader

        /** Consume data from queue, scan log entries and notify GC */
        void _log_reader_thread();

        ///// Stage 3 of pipeline, mapping pg xids to xids; notify GC
        std::filesystem::path _xact_log_path;      ///< xact log base path
        std::filesystem::path _xact_sync_log_file; ///< xact table copy log base path
        std::thread _xact_thread;                  ///< xact worker thread
        std::atomic<uint64_t> _next_xid{0};        ///< next xid in xid range
        RedisQueue<PgXactMsg> _redis_queue;        ///< redis queue for GC
        PgXactLogWriterPtr _xact_logger = nullptr; ///< xact log writer

        /** Redis sorted set for oid to xid mapping */
        RedisSortedSet<PgRedisOidValue> _redis_oid_set;

        LSN_t _last_pushed_lsn = INVALID_LSN;      ///< last pushed lsn to redis queue for GC

        /** transaction worker -- thread fn */
        void _xact_handler_thread();

        /** push transaction to redis queue */
        void _push_xact_to_redis(const PgTransactionPtr xact);

        /** batch push transactions to redis */
        void _push_xacts_to_redis(const std::vector<PgTransactionPtr> &xacts);

        /** notify xact handler to start sync */
        void _notify_xact_start_sync();

        /** Get next xid */
        uint64_t _get_next_xid() {
            return _next_xid.fetch_add(1, std::memory_order_relaxed);
        }

        //// Table copy
        RedisQueue<std::string> _redis_sync_queue; ///< redis queue for table sync
        std::thread _table_copy_thread;            ///< table copy thread

        /** Do the table copies; return the results */
        void _do_table_copies(std::optional<std::vector<uint32_t>> table_ids = std::nullopt);

        /** Copy table thread; waits on table sync queue */
        void _copy_thread();

        /** Process copy table results; insert into redis */
        void _process_copy_results(const std::vector<PgCopyResultPtr> &res);

        /** Replay xaction logs to redis GC queue; blocks other msgs from being queued */
        void _replay_xact_logs();

        //// Redis pub/sub
        std::thread _pubsub_thread;          ///< redis pub/sub thread

        /** Redis pub/sub thread entry point */
        void _redis_pubsub_thread();

        /** Handle state change; callback from Redis pubsub */
        void _handle_external_state_change(const redis::db_state_change::DBState new_state);
    };
    using PgLogMgrPtr = std::shared_ptr<PgLogMgr>;

} // namespace springtail::pg_log_mgr
