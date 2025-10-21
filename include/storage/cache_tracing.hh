#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include <boost/thread.hpp>

#include <storage/extent.hh>

namespace springtail {

    /**
     * Cache tracing subsystem for tracking DataCache behavior.
     * Collects trace events (hits, misses, flushes, evictions) and writes them
     * to disk asynchronously when the trace buffer reaches a threshold.
     *
     * Note: This object should only be instantiated when tracing is enabled.
     * Use std::optional at the call site to avoid overhead when disabled.
     */
    class CacheTracer {
    public:
        /** Trace event types for cache behavior tracking. */
        enum class EventType : uint8_t {
            HIT = 0,      ///< Cache hit event
            MISS = 1,     ///< Cache miss event (disk read required)
            FLUSH = 2,    ///< Extent flush to disk event
            EVICTION = 3  ///< Extent eviction from cache event
        };

        /**
         * Constructs a CacheTracer based on configuration properties.
         * @param event_mask Bitmask of enabled event types (bit N = EventType N)
         * @param output_path Path to write trace files
         * @param flush_threshold Size in bytes at which to flush trace extent
         */
        CacheTracer(uint8_t event_mask,
                   const std::filesystem::path& output_path,
                   uint64_t flush_threshold);

        ~CacheTracer();

        /**
         * Records a cache trace event.
         * @param event_type Type of event (hit, miss, flush, eviction)
         * @param filename Path to the data file
         * @param extent_id The extent identifier
         * @param cache_id Cache ID for dirty/mutable extents (0 for clean)
         */
        void record_event(EventType event_type,
                         const std::filesystem::path& filename,
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

    private:
        /**
         * Internal helper to flush the current trace extent to disk.
         */
        void _flush_trace_extent();

        /**
         * Gets the current timestamp in microseconds since epoch.
         */
        static uint64_t _get_timestamp_us();

        /**
         * Creates a new trace extent with the appropriate schema.
         */
        void _create_trace_extent();

    private:
        uint8_t _event_mask;                    ///< Bitmask of enabled event types
        std::filesystem::path _output_path;     ///< Base path for trace output files
        uint64_t _flush_threshold;              ///< Size threshold for flushing trace extent

        boost::mutex _mutex;                    ///< Protects trace state
        std::shared_ptr<Extent> _current_trace; ///< Current extent collecting traces
        std::shared_ptr<class ExtentSchema> _schema; ///< Schema for trace entries
        uint64_t _trace_sequence;               ///< Sequence number for trace files

        std::vector<std::future<std::shared_ptr<struct IOResponseAppend>>> _pending_flushes; ///< Pending async flush operations
    };

} // namespace springtail
