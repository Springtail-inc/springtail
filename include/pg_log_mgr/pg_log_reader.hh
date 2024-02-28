#pragma once

#include <memory>
#include <fstream>
#include <filesystem>
#include <map>
#include <vector>

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
        using PgTransactionQueuePtr = std::shared_ptr<ConcurrentQueue<PgTransaction>>;

        /**
         * @brief Construct a new Pg Log Reader object
         * @param queue queue to enqueue parsed xactions for xid logger and GC
         */
        PgLogReader(const PgTransactionQueuePtr queue)
            : _queue(queue)
        {}

        /**
         * @brief Process next set of messages from log file
         * @param path file path
         * @param start_offset starting file offset
         * @param num_messages number of messages to process
         */
        void process_log(const std::filesystem::path &path,
                         uint64_t start_offset,
                         int num_messages);
    private:
        /** current file path */
        std::filesystem::path _current_path;

        /** postgres replication stream log parser */
        PgMsgStreamReader _reader;

        /** transaction queue -- pg xids extracted from log entries */
        PgTransactionQueuePtr _queue;

        /** current transaction */
        PgTransactionPtr _current_xact;

        /** Map of in progress transactions if in streaming mode */
        std::map<uint32_t, PgTransactionPtr> _xact_map;

        /** Process begin message */
        void _process_begin(const PgMsgBegin &begin_msg);

        /** Process commit message */
        void _process_commit(const PgMsgCommit &commit_msg);

        /** Process stream start message */
        void _process_stream_start(const PgMsgStreamStart &start_msg);

        /** Process stream commit message */
        void _process_stream_commit(const PgMsgStreamCommit &commit_msg);

        /** Process stream abort message */
        void _process_stream_abort(const PgMsgStreamAbort &abort_msg);

        /** Process ddl change message; add oid to xact oid set */
        void _process_ddl(uint32_t oid, int32_t xid, bool is_streaming);
    };
}