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

#include <pg_repl/pg_repl_msg.hh>

#include <pg_log_mgr/pg_log_queue.hh>
#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>
#include <pg_log_mgr/pg_xact_handler.hh>

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
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgTransaction>>;

        /** Constructor */
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
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path)
        {}

        /** Setup streaming and startup threads */
        void start_streaming();

        /** Wait for threads */
        void join() {
            _writer_thread.join();
            _reader_thread.join();
            _xact_thread.join();
        }

        /** Set shutdown flag */
        void shutdown() {
            _shutdown = true;
        }

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

        /** postgres connection */
        PgReplConnection _pg_conn;

        /** postgres protocol version */
        int _proto_version;

        /** shutdown flag */
        std::atomic<bool> _shutdown = false;

        /// Stage 1 of pipeline, writing replication log to disk

        /** Process data from replication stream in loop, queue path, offsets */
        void _log_writer();

        /** log writer thread */
        std::thread _writer_thread;

        /** Helper to create log writer -- one per log file */
        PgLogWriterPtr _create_logger();

        /** replication log base path */
        std::filesystem::path _repl_log_path;

        /** queue shared between writer (producer) and reader (consumer threads )*/
        PgLogQueue _logger_queue;

        /** callback from log writer class to update lsn from fsync thread*/
        void _lsn_callback(LSN_t lsn);

        /// Stage 2 of pipeline, reading replication log and parsing xacts

        /** Consume data from queue, scan log entries and notify GC */
        void _log_reader();

        /** log reader thread */
        std::thread _reader_thread;

        /** queue shared between reader and xact_worker; contains PgReplMsgStream::PgTransactionPtr */
        PgTransactionQueuePtr _xact_queue;

        /** log reader */
        PgLogReader _pg_log_reader;


        /// Stage 3 of pipeline, mapping pg xids to xids; notify GC

        /** base path for storing transaction log files */
        std::filesystem::path _xact_log_path;

        /** transaction notification/logging thread */
        std::thread _xact_thread;

        /** transaction worker -- thread fn */
        void _xact_worker();
    };

} // namespace springtail