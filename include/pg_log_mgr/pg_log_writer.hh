#pragma once

#include <filesystem>
#include <memory>
#include <atomic>
#include <thread>
#include <queue>
#include <mutex>

#include <fmt/format.h>

#include <pg_repl/pg_types.hh>
#include <pg_repl/pg_repl_connection.hh>

namespace springtail {
    /**
     * Postgres log writer class.  Receives data from Pg replication stream in messages.
     * A message may contain multiple Postgres operations.  A header is written for each
     * message chunk received.  Once a message chunk has been fully written the log_data()
     * call returns true to notify the caller a full message has been written.
    */
    class PgLogWriter {
    public:
        /**
         * @brief Log header
         */
        struct PgLogHeader {
            uint32_t magic;
            uint32_t msg_length;
            LSN_t start_lsn;
            LSN_t end_lsn;
            uint32_t proto_version;

            static constexpr int SIZE = (4 + 4 + 8 + 8 + 4);

            PgLogHeader(uint32_t msg_length, LSN_t start_lsn, LSN_t end_lsn, uint32_t proto_version)
                : magic(PG_LOG_MAGIC), msg_length(msg_length), start_lsn(start_lsn),
                  end_lsn(end_lsn), proto_version(proto_version)
            {}

            PgLogHeader(const char * const buffer) {
                magic = recvint32(buffer);
                msg_length = recvint32(buffer + 4);
                start_lsn = recvint64(buffer + 8);
                end_lsn = recvint64(buffer + 16);
                proto_version = recvint32(buffer + 24);
            }

            void encode_header(char * const buffer) {
                sendint32(magic, buffer);
                sendint32(msg_length, buffer + 4);
                sendint64(start_lsn, buffer + 8);
                sendint64(end_lsn, buffer + 16);
                sendint32(proto_version, buffer + 24);
            }

            std::string to_string() {
                return fmt::format("Header: magic={:#X}, msg_length={}, start_lsn={}, end_lsn={}, proto_version={}",
                                   magic, msg_length, start_lsn, end_lsn, proto_version);
            }
        };

        /** Magic number used in log header */
        static constexpr uint32_t PG_LOG_MAGIC=0xDEFC8193UL;


        /** Size of the log header preceeds each message */
        static constexpr int PG_LOG_HDR_BYTES=PgLogHeader::SIZE;

        /** FSYNC interval, don't fsync more frequently than this */
        static constexpr int PG_LOG_MIN_FSYNC_MS=50;

        /**
         * @brief Construct a new Pg Log Writer object
         * @param file file to be writing
         * @param proto_version protocol version
         */
        PgLogWriter(const std::filesystem::path &file, int proto_version,
                    std::function<void (LSN_t)> lsn_callback_fn);

        /**
         * @brief Add data to log; start of message starts with header.
         * Header contains: PG_LOG_MAGIC 4 B + Msg Length 4B + Starting msg LSN 8B
         * @param data data to add, may be a partial message
         * @return true if full message received
         * @return false if partial message received
         */
        bool log_data(const PgCopyData &data);

        /** Get current offset */
        uint64_t offset() const { return _current_offset; }

        /** Close the file */
        void close();

        /**
         * @brief Get file size
         * @return uint64_t size in bytes
         */
        uint64_t size() const { return _current_offset; }

        /**
         * @brief Get file name
         * @return std::filesystem::path& filename
         */
        std::filesystem::path &filename() { return _file; }

        /**
         * @brief Get the latest synced lsn
         * @return LSN_t LSN
         */
        LSN_t get_latest_synced_lsn() {
            return _latest_synced_lsn.load();
        }

    private:
        struct LsnOffset {
            uint64_t offset;
            LSN_t    lsn;
            LsnOffset(uint64_t offset, LSN_t lsn) : offset(offset), lsn(lsn) {}
        };
        using LsnOffsetPtr = std::shared_ptr<LsnOffset>;

        /** current file path */
        std::filesystem::path _file;

        /** postgres version */
        int _proto_version;

        /** callback for setting the lsn */
        std::function<void (LSN_t)> _lsn_callback_fn;

        /** current offset -- access from fsync thread */
        std::atomic<uint64_t> _current_offset = 0;

        /** offset of end of current message */
        uint64_t _msg_end_offset = 0;

        /** file descriptor */
        int _fd;

        /** shutdown flag for fsync thread */
        std::atomic<bool> _shutdown = false;

        /** last fsync offset access from fsync thread */
        std::atomic<uint64_t> _last_fsync_offset = 0;

        /** latest LSN synced based on offset */
        std::atomic<LSN_t> _latest_synced_lsn = INVALID_LSN;

        /** fsync thread */
        std::thread _fsync_thread;

        std::queue<LsnOffsetPtr> _lsn_queue;
        std::mutex _queue_mutex;

        /** fsync thread */
        void _fsync_worker();

        /** Queue offsets and LSN pairs */
        void _add_lsn_to_queue(uint64_t start_offset, LSN_t start_lsn,
                               uint64_t end_offset, LSN_t end_lsn);

        /** Update latest_synced_lsn based on fsync offset and queue */
        void _update_lsn_from_queue();

        /** Shutdown the fsync thread and join with it */
        void _shutdown_fsync() {
            _shutdown = true;
            _fsync_thread.join(); // may take PG_LOG_MIN_FSYNC_MS time
        }
    };
    using PgLogWriterPtr = std::shared_ptr<PgLogWriter>;
}