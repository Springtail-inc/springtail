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
    private:
        /**
         * @brief Queue Entry, encodes start/end offset and filename,
         *        plus number of messages covered by offset range
         */
        struct PgLogQueueEntry {
            uint64_t start_offset;
            uint64_t end_offset;
            std::filesystem::path path;
            int num_messages;

            PgLogQueueEntry(uint64_t start_offset, uint64_t end_offset, const std::filesystem::path &path)
                : start_offset(start_offset), end_offset(end_offset), path(path), num_messages(1)
            {}
        };
        using PgLogQueueEntryPtr = std::shared_ptr<PgLogQueueEntry>;

        /** Queue class between log writer and log reader */
        class PgLogQueue : public ConcurrentQueue<PgLogQueueEntry> {
        public:
            /**
             * @brief Push entry onto queue, try to merge with entry on back of queue if possible
             * @param start_offset file start offset of msg
             * @param end_offset file end offset of msg
             * @param path file pathname
             */
            void push(uint64_t start_offset, uint64_t end_offset, const std::filesystem::path &path)
            {
                std::unique_lock<std::mutex> write_lock{_mutex};

                // get the last enqueued entry and see if we can modify it to include this message as well
                PgLogQueueEntryPtr &entry = _queue.back();
                if (entry->path == path && entry->end_offset == start_offset) {
                    entry->end_offset = end_offset;
                    entry->num_messages++;
                    return;
                }

                // otherwise create and add new entry
                PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(start_offset, end_offset, path);

                _internal_push(new_entry, write_lock);
            }
        };

    public:
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgReplMsgStream::PgTransaction>>;

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
          _xact_queue(std::make_shared<ConcurrentQueue<PgReplMsgStream::PgTransaction>>()),
          _pg_log_reader(_xact_queue), _xact_log_path(xact_log_path)
        {}

        /**
         * @brief Setup streaming and start
         */
        void start_streaming();



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
        std::filesystem::path _xact_log_path;

        std::thread _xact_thread;

        void _xact_worker();

    };

} // namespace springtail