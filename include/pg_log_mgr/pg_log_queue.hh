#pragma once

#include <memory>
#include <mutex>
#include <filesystem>
#include <chrono>

#define PROFILE_INGEST
#include <pg_repl/pg_repl_instrument.hh>

#if defined(PROFILE_INGEST) && !defined(PROFILE_INGEST_ENABLED)
static_assert(false, "This error means that <logging.hh> was included before PROFILE_INGEST is set.");
#endif

#include <common/concurrent_queue.hh>
#include <common/common.hh>

namespace springtail::pg_log_mgr {
    /**
     * @brief Queue Entry, encodes start/end offset and filename,
     *        plus number of messages covered by offset range
     */
    struct PgLogQueueEntry {
        using clock = std::chrono::steady_clock;

        uint64_t start_offset;
        uint64_t end_offset;
        std::filesystem::path path;
        int num_messages;
        bool is_stall_message;

        INSTRUMENT_INGEST_DATA(clock::time_point, ts_created)
        INSTRUMENT_INGEST_DATA(clock::time_point, ts_pop)
        INSTRUMENT_INGEST_DATA(size_t, queue_size)

        PgLogQueueEntry() = delete;

        PgLogQueueEntry(uint64_t start_offset, uint64_t end_offset, const std::filesystem::path &path)
            : start_offset(start_offset), end_offset(end_offset),
              path(path), num_messages(1), is_stall_message(false)
        {
            INSTRUMENT_INGEST( { ts_created = clock::now();
                    queue_size = 0;
                    } )
        }

        explicit PgLogQueueEntry(bool stall) : is_stall_message(stall)
        {
            INSTRUMENT_INGEST( { ts_created = clock::now();
                    queue_size = 0;
                    } )
        }

        PgLogQueueEntry(const PgLogQueueEntry&) = delete;
        PgLogQueueEntry(PgLogQueueEntry&&) = default;
        const PgLogQueueEntry& operator=(const PgLogQueueEntry&) = delete;
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
            [[maybe_unused]] int queue_size = 0;
            INSTRUMENT_INGEST({queue_size = size();})
            std::unique_lock<std::mutex> write_lock{_mutex};
            for (const auto& entry: entries) {
                PgLogQueueEntryPtr new_entry = std::make_shared<PgLogQueueEntry>(entry.start_offset, entry.end_offset, entry.path);
                new_entry->num_messages = entry.num_messages;
                INSTRUMENT_INGEST( {new_entry->queue_size = queue_size++;})
                _internal_push(std::move(new_entry), write_lock);
            }
        }

        /**
         * @brief Used to push a stall message onto the queue
         */
        void push_stall()
        {
            auto v = std::make_shared<PgLogQueueEntry>(true);
            INSTRUMENT_INGEST( {v->queue_size = size();})

            std::unique_lock<std::mutex> write_lock{_mutex};
            _internal_push(std::move(v), write_lock);
        }
    };
} // namespace springtail::pg_log_mgr
