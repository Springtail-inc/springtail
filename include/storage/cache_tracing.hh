#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <boost/thread.hpp>
#include <nlohmann/json.hpp>

#include <common/circular_buffer.hh>
#include <storage/extent.hh>

namespace springtail {

    /**
     * Cache tracing and statistics subsystem for tracking DataCache behavior.
     * Always tracks statistics (hits, misses, calls) and optionally writes
     * detailed trace events to disk when trace file generation is enabled.
     *
     * Maintains both lifetime totals and per-minute metrics for the last 5 minutes.
     */
    class CacheTracer {
    public:
        /** Metrics for a single minute of cache operation (stored in circular buffer). */
        struct MinuteMetrics {
            uint64_t cache_hits{0};
            uint64_t cache_misses{0};
            std::chrono::system_clock::time_point timestamp;
        };
        /** Trace event types for cache behavior tracking. */
        enum class EventType : uint8_t {
            HIT = 0,      ///< Cache hit event
            MISS = 1,     ///< Cache miss event (disk read required)
            FLUSH = 2,    ///< Extent flush to disk event
            EVICTION = 3  ///< Extent eviction from cache event
        };

        /**
         * Constructs a CacheTracer by reading configuration from Properties.
         */
        CacheTracer();

        ~CacheTracer();

        /**
         * Records a cache hit event.
         * @param filename Path to the data file
         * @param extent_id The extent identifier
         * @param cache_id Cache ID for dirty/mutable extents (0 for clean)
         */
        void record_hit(const std::filesystem::path& filename,
                       uint64_t extent_id,
                       uint64_t cache_id);

        /**
         * Records a cache miss event.
         * @param filename Path to the data file
         * @param extent_id The extent identifier
         * @param cache_id Cache ID for dirty/mutable extents (0 for clean)
         */
        void record_miss(const std::filesystem::path& filename,
                        uint64_t extent_id,
                        uint64_t cache_id);

        /**
         * Records a cache flush event.
         * @param filename Path to the data file
         * @param extent_id The extent identifier
         * @param cache_id Cache ID for dirty/mutable extents
         */
        void record_flush(const std::filesystem::path& filename,
                         uint64_t extent_id,
                         uint64_t cache_id);

        /**
         * Records a cache eviction event.
         * @param filename Path to the data file
         * @param extent_id The extent identifier
         * @param cache_id Cache ID for dirty/mutable extents
         */
        void record_eviction(const std::filesystem::path& filename,
                            uint64_t extent_id,
                            uint64_t cache_id);

        /**
         * Returns true if the given event type should be traced.
         */
        bool should_trace(EventType event_type) const {
            return (_event_mask & (1u << static_cast<uint8_t>(event_type))) != 0;
        }

        /**
         * Flushes any remaining trace data to disk.
         */
        void flush();

        /**
         * Returns cache metrics including lifetime totals and per-minute history.
         */
        nlohmann::json get_cache_metrics() const;

    private:
        /**
         * Internal helper to write a trace record if tracing is enabled.
         */
        void _write_trace_record(EventType event_type,
                                const std::filesystem::path& filename,
                                uint64_t extent_id,
                                uint64_t cache_id);

        /**
         * Internal helper to flush a trace extent to disk.
         * @param extent_to_flush The extent to flush (allows flushing old extent while creating new one)
         */
        void _flush_trace_extent(ExtentPtr extent_to_flush) noexcept;

        /**
         * Gets the current timestamp in microseconds since epoch.
         */
        static uint64_t _get_timestamp_us();

        /**
         * Creates a new trace extent with the appropriate schema.
         */
        void _create_trace_extent();

    private:
        /**
         * Background thread for rotating per-minute metrics.
         */
        void _metrics_rotation_thread();

    private:
        bool _trace_enabled;                    ///< Whether to write trace files
        uint8_t _event_mask;                    ///< Bitmask of enabled event types
        std::filesystem::path _output_path;     ///< Base path for trace output files
        uint64_t _flush_threshold;              ///< Size threshold for flushing trace extent

        // Trace file generation (only used if _trace_enabled)
        boost::mutex _mutex;                    ///< Protects trace state
        ExtentPtr _current_trace; ///< Current extent collecting traces
        ExtentSchemaPtr _schema;  ///< Schema for trace entries
        uint64_t _trace_sequence;               ///< Sequence number for trace files
        std::vector<std::future<std::shared_ptr<IOResponseAppend>>> _pending_flushes; ///< Pending async flush operations

        // Statistics tracking (always enabled)
        std::atomic<uint64_t> _total_cache_hits{0};    ///< Total lifetime cache hits
        std::atomic<uint64_t> _total_cache_misses{0};  ///< Total lifetime cache misses

        // Per-minute metrics tracking
        mutable boost::mutex _metrics_mutex;           ///< Protects metrics buffer
        CircularBuffer<MinuteMetrics> _minute_metrics{5}; ///< Last 5 minutes of metrics
        std::atomic<uint64_t> _current_minute_hits{0}; ///< Current minute cache hits
        std::atomic<uint64_t> _current_minute_misses{0}; ///< Current minute cache misses
        std::chrono::system_clock::time_point _current_minute_start; ///< Start time of current minute
        std::thread _metrics_thread;                   ///< Background thread for metrics rotation
        std::atomic<bool> _shutdown_metrics{false};    ///< Signal to shutdown metrics thread
        boost::condition_variable _metrics_cond;       ///< Condition variable for metrics thread
    };

} // namespace springtail
