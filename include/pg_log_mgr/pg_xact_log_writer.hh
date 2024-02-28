#pragma once

#include <mutex>
#include <thread>
#include <filesystem>
#include <memory>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail {
    class PgXactLogWriter {
    public:
        static constexpr int PG_XLOG_MIN_FSYNC_MS = 50;

        static constexpr uint32_t PG_XLOG_MAGIC = 0xA3F94B1C;

        PgXactLogWriter(const std::filesystem::path &file);

        /**
         * @brief Log the xact to file
         * @param xact postgres transaction
         * @param xid springtail xid
         */
        void log_data(PgTransactionPtr xact, uint64_t xid);

        /** Close the file */
        void close();

        /**
         * @brief Get size of file
         * @return uint64_t
         */
        uint64_t size() {
            return _offset;
        }

    private:
        /** current file path */
        std::filesystem::path _file;

        /** file descriptor */
        int _fd;

        /** shutdown flag for fsync thread */
        std::atomic<bool> _shutdown = false;

        /** last fsync offset access from fsync thread */
        std::atomic<bool> _need_fsync = false;

        /** current file offset */
        uint64_t _offset = 0;

        std::thread _fsync_thread;

        /** fsync thread */
        void _fsync_worker();
    };
    using PgXactLogWriterPtr = std::shared_ptr<PgXactLogWriter>;
}