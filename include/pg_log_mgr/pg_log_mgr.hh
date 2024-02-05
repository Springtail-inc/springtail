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

#include <pg_log_mgr/pg_log_writer.hh>
#include <pg_log_mgr/pg_log_reader.hh>

namespace springtail {
    /**
     * @brief Postgres log manager
     * Responsible for storing Postgres logs
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
        /** Constructor */
        PgLogMgr(std::filesystem::path base_path,
                 const std::string &host, const std::string &db_name,
                 const std::string &user_name, const std::string &password,
                 const std::string &pub_name, const std::string &slot_name,
                 int port)
        : _host(host), _db_name(db_name), _user_name(user_name), _password(password),
          _pub_name(pub_name), _slot_name(slot_name), _port(port),
          _pg_conn(_port, _host, _db_name, _user_name, _password, _pub_name, _slot_name),
          _base_path(base_path), _pg_log_reader()
        {}

        /**
         * @brief Setup streaming and start
         */
        void start_streaming();

        /**
         * @brief process data from replication stream in loop, product to queue
         */
        void log_writer();

        /**
         * @brief consume data from queue, scan log entries and notify GC
         */
        void log_reader();

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

        /** base path */
        std::filesystem::path _base_path;

        /** log writer thread */
        std::thread _writer_thread;

        std::thread _reader_thread;

        std::atomic<bool> _shutdown = false;

        /** queue shared between writer (producer) and reader (consumer threads )*/
        PgLogQueue _queue;

        /** Log reader */
        PgLogReader _pg_log_reader;

        /** Helper to create log writer -- one per log file */
        PgLogWriterPtr _create_logger();
    };

} // namespace springtail