#pragma once

#include <mutex>
#include <thread>
#include <filesystem>
#include <memory>

#include <pg_repl/pg_repl_msg.hh>

namespace springtail::pg_log_mgr {

    /**
     * @brief Postgres transaction log writer
     */
    class PgXactLogWriter {
    public:
        static constexpr int PG_XLOG_MIN_FSYNC_MS = 50;

        static constexpr uint8_t PG_XLOG_MAGIC[] = {0xA3, 0xF9, 0x4B};

        /**
         * @brief Construct a new Pg Xact Log Writer object; creates file
         * @param file path dir/name
         */
        PgXactLogWriter(const std::filesystem::path &file);

        ~PgXactLogWriter() { close(); }

        /**
         * @brief Log the committed xact to file
         * @param xact postgres transaction
         * @param xid springtail xid
         */
        void log_commit(PgTransactionPtr xact);

        /**
         * @brief Log a stream message (start or abort), no springtail xid allocated
         * @param xact postgres transaction
         */
        void log_stream_msg(PgTransactionPtr xact);

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