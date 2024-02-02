#pragma once

#include <memory>
#include <fstream>
#include <filesystem>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail {
    class PgLogReader {
    public:
        PgLogReader() : _pg_repl_stream() {}

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

        /**
         * @brief Helper to create a new file stream
         * @param path file path
         * @param start_offset starting offset
         */
        void _create_stream(const std::filesystem::path &path, uint64_t start_offset);

        /**
         * @brief Helper to queue transactions for GC
         * @param xacts list of transactions from parsing replication log
         */
        void _process_xacts(const std::vector<PgReplMsgStream::PgTransactionPtr> xacts);
    };
}