#pragma once

#include <memory>
#include <fstream>
#include <filesystem>

#include <common/concurrent_queue.hh>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail {
    /**
     * @brief Log reader class.  Reads logs written by PgLogWriter.  Does minimal parsing
     * to extract begin and commit messages.  Queues those begin/commit messages to a
     * shared queue for logging and to be sent to the GC.
     */
    class PgLogReader {
    public:
        /** convenience type for the shared transaction queue */
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgReplMsgStream::PgTransaction>>;

        /**
         * @brief Construct a new Pg Log Reader object
         * @param queue queue to enqueue parsed xactions for xid logger and GC
         */
        PgLogReader(const PgTransactionQueuePtr queue)
            : _pg_repl_stream(), _queue(queue)
        {}

        /**
         * @brief Process next set of messages from log file
         * @param path file path
         * @param start_offset starting file offset
         * @param end_offset ending offset
         * @param num_messages number of messages to process
         */
        void process_log(const std::filesystem::path &path, uint64_t start_offset,
                         uint64_t end_offset, int num_messages);
    private:
        /** current file path */
        std::filesystem::path _current_path;

        /** current file stream -- shared with pg_repl_msg */
        std::shared_ptr<std::fstream> _stream = nullptr;

        /** current file offset */
        uint64_t _current_offset = 0;

        /** postgres replication stream log parser */
        PgReplMsgStream _pg_repl_stream;

        /** transaction queue -- pg xids extracted from log entries */
        PgTransactionQueuePtr _queue;

        /**
         * @brief Helper to create a new file stream
         * @param path file path
         * @param start_offset starting offset
         */
        void _create_stream(const std::filesystem::path &path, uint64_t start_offset);
    };
}