#pragma once

#include <memory>
#include <mutex>
#include <filesystem>

#include <common/concurrent_queue.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Queue Entry, encodes start/end offset and filename,
     *        plus number of messages covered by offset range
     */
    struct PgLogQueueEntry {
        uint64_t start_offset;
        uint64_t end_offset;
        std::filesystem::path path;
        int num_messages;
        bool is_stall_message;

        PgLogQueueEntry(uint64_t start_offset, uint64_t end_offset, const std::filesystem::path &path)
            : start_offset(start_offset), end_offset(end_offset),
              path(path), num_messages(1), is_stall_message(false)
        {}

        explicit PgLogQueueEntry(bool stall) : is_stall_message(stall) {}
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
            for (auto entry: entries) {
                PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(entry.start_offset, entry.end_offset, entry.path);
                _internal_push(new_entry, write_lock);
            }
        }

        /**
         * @brief Used to push a stall message onto the queue
         */
        void push_stall()
        {
            std::unique_lock<std::mutex> write_lock{_mutex};
            _internal_push(std::make_shared<PgLogQueueEntry>(true), write_lock);
        }
    };
} // namespace springtail::pg_log_mgr
