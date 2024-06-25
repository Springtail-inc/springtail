#pragma once

#include <memory>
#include <mutex>
#include <filesystem>

#include <common/concurrent_queue.hh>

namespace springtail {
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
            if (!_queue.empty()) {
                PgLogQueueEntryPtr &entry = _queue.back();
                if (entry->path == path && entry->end_offset == start_offset) {
                    entry->end_offset = end_offset;
                    entry->num_messages++;
                    return;
                }
            }

            // otherwise create and add new entry
            PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(start_offset, end_offset, path);

            _internal_push(new_entry, write_lock);
        }
    };
}
