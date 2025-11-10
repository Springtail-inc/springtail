#pragma once

#include <memory>
#include <mutex>
#include <filesystem>
#include <vector>

#include <common/concurrent_queue.hh>
#include <common/common.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Queue Entry, encodes start/end offset and filename,
     *        plus number of messages covered by offset range.
     *        Can also hold an in-memory buffer for zero-copy processing.
     */
    struct PgLogQueueEntry {
        enum class Type {
            FILE,          ///< Entry points to file on disk
            MEMORY_BUFFER, ///< Entry contains in-memory buffer
            STALL          ///< Stall message for synchronization
        };

        Type type;                              ///< Type of queue entry (FILE, MEMORY_BUFFER, or STALL)
        uint64_t start_offset;                  ///< Starting offset in file (for FILE type)
        uint64_t end_offset;                    ///< Ending offset in file (for FILE type) or buffer size (for MEMORY_BUFFER)
        std::filesystem::path path;             ///< File path for this entry (needed for XID tracking even with MEMORY_BUFFER)

        // Memory buffer fields for zero-copy processing
        std::shared_ptr<std::vector<char>> memory_buffer;  ///< Shared pointer to in-memory buffer (for MEMORY_BUFFER type)
        size_t buffer_size = 0;                             ///< Size of buffer in bytes

        PgLogQueueEntry() = delete;

        // Constructor for file-based entry
        PgLogQueueEntry(uint64_t start_offset, uint64_t end_offset, const std::filesystem::path &path)
            : type(Type::FILE),
              start_offset(start_offset),
              end_offset(end_offset),
              path(path)
        {}

        // Constructor for memory buffer entry
        PgLogQueueEntry(std::shared_ptr<std::vector<char>> buffer, const std::filesystem::path &path)
            : type(Type::MEMORY_BUFFER),
              start_offset(0),
              end_offset(buffer->size()),  // Set end_offset to buffer size for loop termination
              path(path),
              memory_buffer(std::move(buffer)),
              buffer_size(memory_buffer->size())
        {}

        // Constructor for stall message
        struct StallTag {};
        explicit PgLogQueueEntry(StallTag)
            : type(Type::STALL),
              start_offset(0),
              end_offset(0)
        {}

        PgLogQueueEntry(const PgLogQueueEntry&) = delete;
        PgLogQueueEntry(PgLogQueueEntry&&) = default;
        const PgLogQueueEntry& operator=(const PgLogQueueEntry&) = delete;
    };
    using PgLogQueueEntryPtr = std::shared_ptr<PgLogQueueEntry>;

    /**
     * @brief Queue class between log writer and log reader with memory pressure management
     *
     * The queue uses high and low watermarks to manage memory pressure from in-memory buffers:
     * - When memory usage exceeds the high watermark, the writer should switch from pushing
     *   memory buffers (MEMORY_BUFFER entries) to pushing file references (FILE entries).
     * - The writer continues using file references until memory usage drops below the low
     *   watermark, at which point it can resume pushing memory buffers for zero-copy processing.
     * - This hysteresis prevents thrashing between modes when memory usage hovers near a threshold.
     */
    class PgLogQueue : public ConcurrentQueue<PgLogQueueEntry> {
    public:
        // Default memory pressure watermarks (in bytes)
        static constexpr size_t DEFAULT_MEMORY_HIGH_WATERMARK = 128 * 1024 * 1024;  // 128 MB
        static constexpr size_t DEFAULT_MEMORY_LOW_WATERMARK = 64 * 1024 * 1024;    // 64 MB

        /**
         * @brief Construct a PgLogQueue with specified memory watermarks
         * @param memory_high_watermark High watermark in bytes (writer switches to file mode when exceeded)
         * @param memory_low_watermark Low watermark in bytes (writer switches back to buffer mode when below)
         */
        explicit PgLogQueue(size_t memory_high_watermark = DEFAULT_MEMORY_HIGH_WATERMARK,
                            size_t memory_low_watermark = DEFAULT_MEMORY_LOW_WATERMARK)
            : _memory_high_watermark(memory_high_watermark),
              _memory_low_watermark(memory_low_watermark)
        {}
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
            if (!_queue.empty()) {
                PgLogQueueEntryPtr &entry = _queue.back();
                if (entry->path == path && entry->end_offset == start_offset) {
                    entry->end_offset = end_offset;
                    return;
                }
            }

            // otherwise create and add new entry
            PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(start_offset, end_offset, path);

            _internal_push(new_entry, write_lock);
        }

        void push(const std::vector<PgLogQueueEntry> &entries) {
            std::unique_lock<std::mutex> write_lock{_mutex};
            for (const auto& entry: entries) {
                PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(entry.start_offset, entry.end_offset, entry.path);
                _internal_push(std::move(new_entry), write_lock);
            }
        }

        /**
         * @brief Push memory buffer entry onto queue
         * @param buffer shared pointer to memory buffer (must not be nullptr)
         * @param path file path where the data was written to disk
         */
        void push_buffer(std::shared_ptr<std::vector<char>> buffer, const std::filesystem::path &path)
        {
            CHECK(buffer != nullptr) << "Buffer must not be null";
            size_t buffer_size = buffer->size();
            PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(std::move(buffer), path);

            std::unique_lock<std::mutex> write_lock{_mutex};
            _total_memory_bytes += buffer_size;
            _internal_push(std::move(new_entry), write_lock);
        }

        /**
         * @brief Used to push a stall message onto the queue
         */
        void push_stall()
        {
            auto v = std::make_shared<PgLogQueueEntry>(PgLogQueueEntry::StallTag{});
            std::unique_lock<std::mutex> write_lock{_mutex};
            _internal_push(std::move(v), write_lock);
        }

        /**
         * @brief Pop entry from queue and update memory tracking
         * @param seconds timeout in seconds (0 = wait indefinitely)
         * @return shared pointer to queue entry, or nullptr if queue is empty
         */
        PgLogQueueEntryPtr pop(uint32_t seconds = 0) {
            PgLogQueueEntryPtr entry = ConcurrentQueue<PgLogQueueEntry>::pop(seconds);
            if (entry && entry->type == PgLogQueueEntry::Type::MEMORY_BUFFER) {
                _total_memory_bytes -= entry->buffer_size;
            }
            return entry;
        }

        /**
         * @brief Check if memory usage is above high watermark
         * @return true if above high watermark
         */
        bool is_memory_pressure_high() const {
            return _total_memory_bytes >= _memory_high_watermark;
        }

        /**
         * @brief Check if memory usage is below low watermark
         * @return true if below low watermark
         */
        bool is_memory_pressure_low() const {
            return _total_memory_bytes <= _memory_low_watermark;
        }

        /**
         * @brief Get current total memory usage in bytes
         * @return total memory bytes
         */
        size_t get_memory_usage() const {
            return _total_memory_bytes;
        }

    private:
        size_t _memory_high_watermark;  ///< High watermark for memory pressure (bytes)
        size_t _memory_low_watermark;   ///< Low watermark for memory pressure (bytes)
        std::atomic<size_t> _total_memory_bytes{0};  ///< Total memory used by buffers in queue
    };
} // namespace springtail::pg_log_mgr
