#pragma once

#include <iostream>
#include <memory>
#include <filesystem>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

#include <common/concurrent_queue.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/filesystem.hh>

#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_queue.hh>
#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

#include <pg_log_mgr/pg_xact_log_reader.hh>
#include <pg_log_mgr/pg_xact_log_writer.hh>

#include <pg_log_mgr/pg_redis_xact.hh>

namespace springtail {
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

        /** replication and transaction log prefixes and suffix */
        static constexpr char const * const LOG_PREFIX_REPL = "pg_log_repl_";
        static constexpr char const * const LOG_PREFIX_XACT = "pg_log_xact_";
        static constexpr char const * const LOG_SUFFIX = ".log";

        /**
         * @brief Construct a new Pg Log Mgr object
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
        PgLogMgr(const std::filesystem::path &repl_log_path,
                 const std::filesystem::path &xact_log_path,
                 const std::string &host, const std::string &db_name,
                 const std::string &user_name, const std::string &password,
                 const std::string &pub_name, const std::string &slot_name,
                 int port)
        : _host(host), _db_name(db_name), _user_name(user_name), _password(password),
          _pub_name(pub_name), _slot_name(slot_name), _port(port),
          _pg_conn(_port, _host, _db_name, _user_name, _password, _pub_name, _slot_name),
          _repl_log_path(repl_log_path),
          _xact_queue(std::make_shared<ConcurrentQueue<PgTransaction>>()),
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path),
          _redis_queue(redis::QUEUE_PG_TRANSACTIONS),
          _oid_set(redis::SET_PG_OID_XIDS)
        {}

        /**
         * @brief Construct a new Pg Log Mgr object (for testing only)
         * @param repl_log_path replication log base path
         * @param xact_log_path transaction log base path
         */
        PgLogMgr(const std::filesystem::path &repl_log_path,
                 const std::filesystem::path &xact_log_path)
        : _repl_log_path(repl_log_path),
          _xact_queue(std::make_shared<ConcurrentQueue<PgTransaction>>()),
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path),
          _redis_queue(redis::QUEUE_PG_TRANSACTIONS),
          _oid_set(redis::SET_PG_OID_XIDS)
        {}

        /** Start the pipeline; setup the log reader/writer log files etc. */
        void startup();

        /** Setup streaming and startup threads */
        void start_streaming(uint64_t lsn = INVALID_LSN);

        /** Wait for threads */
        void join() {
            _writer_thread.join();
            _reader_thread.join();
            _xact_thread.join();
        }

        /** Set shutdown flag */
        void shutdown() {
            _shutdown = true;
            join();
        }

    protected:
        /** for testing */

        /** Helper to create log writer -- one per log file */
        PgLogWriterPtr _create_repl_logger();

        /** Create xact log writer */
        PgXactLogWriterPtr _create_xact_logger();

        /** Process transaction record -- write it to log and to Redis queue */
        void _process_xact(const PgTransactionPtr xact, const PgXactLogWriterPtr logger);

    private:
        /** minimum size for log rollover */
        static constexpr int LOG_ROLLOVER_SIZE_BYTES = 128 * 1024 * 1024;

        // connection params
        std::string _host;
        std::string _db_name;
        std::string _user_name;
        std::string _password;
        std::string _pub_name;
        std::string _slot_name;
        int _port;

        PgReplConnection _pg_conn;            ///< postgres replication connection
        int _proto_version;                   ///< postgres protocol version
        std::atomic<bool> _shutdown = false;  ///< shutdown flag

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
        std::filesystem::path _xact_log_path; ///< xact log base path
        std::thread _xact_thread;             ///< xact worker thread
        uint64_t _db_id=1;                    ///< db id
        uint64_t _next_xid=0;                 ///< next xid in xid range

        RedisQueue<PgRedisXactValue> _redis_queue; ///< redis queue for GC
        RedisSortedSet<PgRedisOidValue> _oid_set;  ///< redis sorted set for oid to xid mapping

        /** transaction worker -- thread fn */
        void _xact_handler_thread();

        /** push transaction to redis queue */
        void _push_xact_to_redis(const PgTransactionPtr xact);
    };

} // namespace springtail