#include <storage/cache_tracing.hh>

#include <absl/log/log.h>
#include <fmt/format.h>
#include <unistd.h>
#include <sstream>

#include <common/constants.hh>
#include <common/json.hh>
#include <common/properties.hh>
#include <storage/io.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

extern char *program_invocation_name;

namespace springtail {

    CacheTracer::CacheTracer()
        : _trace_sequence(0)
    {
        // Read configuration from Properties
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
        _trace_enabled = Json::get_or<bool>(json, "cache_trace_enabled", false);
        _event_mask = 0;

        // Initialize per-minute metrics - start at beginning of current minute
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm* tm_now = std::localtime(&time_t_now);
        tm_now->tm_sec = 0; // Round down to start of minute
        _current_minute_start = std::chrono::system_clock::from_time_t(std::mktime(tm_now));

        // Start metrics rotation thread
        _metrics_thread = std::thread(&CacheTracer::_metrics_rotation_thread, this);
        pthread_setname_np(_metrics_thread.native_handle(), "CacheMetrics");

        // Only initialize trace file generation if enabled
        if (_trace_enabled) {
            // Parse trace event types
            std::string event_types_str = Json::get_or<std::string>(json, "cache_trace_events", "hit,miss,flush,evict");

            // Build event mask from comma-separated list
            std::istringstream iss(event_types_str);
            std::string event_type;
            while (std::getline(iss, event_type, ',')) {
                // Trim whitespace
                event_type.erase(0, event_type.find_first_not_of(" \t"));
                event_type.erase(event_type.find_last_not_of(" \t") + 1);

                if (event_type == "hit") {
                    _event_mask |= (1u << static_cast<uint8_t>(EventType::HIT));
                } else if (event_type == "miss") {
                    _event_mask |= (1u << static_cast<uint8_t>(EventType::MISS));
                } else if (event_type == "flush") {
                    _event_mask |= (1u << static_cast<uint8_t>(EventType::FLUSH));
                } else if (event_type == "evict") {
                    _event_mask |= (1u << static_cast<uint8_t>(EventType::EVICTION));
                } else if (!event_type.empty()) {
                    LOG_WARN("Unknown cache trace event type: {}", event_type);
                }
            }

            std::string output_path_str = Json::get_or<std::string>(json, "cache_trace_output_path", "./cache_traces");
            _flush_threshold = Json::get_or<uint64_t>(json, "cache_trace_flush_size", 1048576); // 1MB default

            try {
                // Get process name and PID for unique subdirectory
                std::string process_name = std::filesystem::path(program_invocation_name).filename().string();
                pid_t pid = getpid();

                // Create unique subdirectory: output_path/process_name_pid/
                std::string subdir = fmt::format("{}_{}", process_name, pid);
                std::filesystem::path trace_dir = std::filesystem::path(output_path_str) / subdir;

                // Create the trace directory
                std::filesystem::create_directories(trace_dir);

                // Store the full path for trace files
                _output_path = trace_dir / "trace.dat";

                LOG_INFO("CacheTracer initialized: trace_enabled=true, output={}, threshold={} bytes, event_mask=0x{:x}",
                         _output_path.string(), _flush_threshold, _event_mask);

                // Create the initial trace extent
                _create_trace_extent();
            } catch (const std::exception& e) {
                LOG_ERROR("Failed to initialize cache tracing directory: {}. Disabling trace file generation.", e.what());
                _trace_enabled = false;
            }
        } else {
            LOG_INFO("CacheTracer initialized: trace_enabled=false (statistics only)");
        }
    }

    CacheTracer::~CacheTracer()
    {
        // Shutdown metrics thread
        _shutdown_metrics = true;
        _metrics_cond.notify_all();
        if (_metrics_thread.joinable()) {
            _metrics_thread.join();
        }

        // Flush any remaining trace data (only if trace enabled)
        if (_trace_enabled) {
            flush();

            // Wait for all pending async flushes to complete
            for (auto& future : _pending_flushes) {
                try {
                    future.wait();
                } catch (const std::exception& e) {
                    LOG_ERROR("Error waiting for trace flush: {}", e.what());
                }
            }
        }

        // Print final statistics
        uint64_t total_hits = _total_cache_hits.load();
        uint64_t total_misses = _total_cache_misses.load();
        uint64_t total_lookups = total_hits + total_misses;

        if (total_lookups > 0) {
            double hit_rate = (100.0 * total_hits) / total_lookups;
            LOG_INFO("DataCache Statistics:");
            LOG_INFO("  Total cache lookups:   {}", total_lookups);
            LOG_INFO("  Cache hits:            {} ({:.2f}%)", total_hits, hit_rate);
            LOG_INFO("  Cache misses:          {} ({:.2f}%)", total_misses, 100.0 - hit_rate);
        }

        LOG_INFO("CacheTracer shutdown");
    }

    void CacheTracer::record_hit(const std::filesystem::path& filename,
                                 uint64_t extent_id,
                                 uint64_t cache_id)
    {
        // Update statistics
        ++_total_cache_hits;
        ++_current_minute_hits;

        // Write trace record if enabled
        if (_trace_enabled && should_trace(EventType::HIT)) {
            _write_trace_record(EventType::HIT, filename, extent_id, cache_id);
        }
    }

    void CacheTracer::record_miss(const std::filesystem::path& filename,
                                  uint64_t extent_id,
                                  uint64_t cache_id)
    {
        // Update statistics
        ++_total_cache_misses;
        ++_current_minute_misses;

        // Write trace record if enabled
        if (_trace_enabled && should_trace(EventType::MISS)) {
            _write_trace_record(EventType::MISS, filename, extent_id, cache_id);
        }
    }

    void CacheTracer::record_flush(const std::filesystem::path& filename,
                                   uint64_t extent_id,
                                   uint64_t cache_id)
    {
        // Write trace record if enabled (no statistics for flush)
        if (_trace_enabled && should_trace(EventType::FLUSH)) {
            _write_trace_record(EventType::FLUSH, filename, extent_id, cache_id);
        }
    }

    void CacheTracer::record_eviction(const std::filesystem::path& filename,
                                      uint64_t extent_id,
                                      uint64_t cache_id)
    {
        // Write trace record if enabled (no statistics for eviction)
        if (_trace_enabled && should_trace(EventType::EVICTION)) {
            _write_trace_record(EventType::EVICTION, filename, extent_id, cache_id);
        }
    }

    void CacheTracer::_write_trace_record(EventType event_type,
                                          const std::filesystem::path& filename,
                                          uint64_t extent_id,
                                          uint64_t cache_id)
    {
        boost::unique_lock lock(_mutex);

        // Clean up completed async flushes to avoid growing the vector indefinitely
        _pending_flushes.erase(
            std::remove_if(_pending_flushes.begin(), _pending_flushes.end(),
                          [](auto& f) {
                              return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
                          }),
            _pending_flushes.end()
        );

        // Append a new row to the trace extent
        auto row = _current_trace->append();

        // Set field values using the schema's mutable fields
        auto fields = _schema->get_mutable_fields();

        // Field 0: event_type (uint8_t)
        (*fields)[0]->set_uint8(&row, static_cast<uint8_t>(event_type));

        // Field 1: timestamp (uint64_t)
        (*fields)[1]->set_uint64(&row, _get_timestamp_us());

        // Field 2: filename (text)
        (*fields)[2]->set_text(&row, filename.string());

        // Field 3: extent_id (uint64_t)
        (*fields)[3]->set_uint64(&row, extent_id);

        // Field 4: cache_id (uint64_t)
        (*fields)[4]->set_uint64(&row, cache_id);

        // Check if we need to flush (query the extent for its current size)
        if (_current_trace->byte_count() >= _flush_threshold) {
            // Flush current extent asynchronously and create a new one
            _flush_trace_extent();
            _create_trace_extent();
        }
    }

    void CacheTracer::flush()
    {
        if (_trace_enabled) {
            boost::unique_lock lock(_mutex);
            if (_current_trace && _current_trace->row_count() > 0) {
                _flush_trace_extent();
                _create_trace_extent();
            }
        }
    }

    void CacheTracer::_metrics_rotation_thread()
    {
        while (!_shutdown_metrics) {
            // Wait for 60 seconds or until shutdown
            boost::unique_lock lock(_metrics_mutex);
            _metrics_cond.wait_for(lock, boost::chrono::seconds(60),
                                  [this]() { return _shutdown_metrics.load(); });

            if (_shutdown_metrics) {
                break;
            }

            // Rotate the current minute into the circular buffer
            auto now = std::chrono::system_clock::now();

            // Create a snapshot of current minute metrics
            MinuteMetrics snapshot;
            snapshot.cache_hits = _current_minute_hits.exchange(0);
            snapshot.cache_misses = _current_minute_misses.exchange(0);
            snapshot.timestamp = _current_minute_start;

            // Add to circular buffer if there was activity
            if (snapshot.cache_hits > 0 || snapshot.cache_misses > 0) {
                // Make space if buffer is full
                if (_minute_metrics.size() >= 5) {
                    _minute_metrics.next(); // Remove oldest
                }
                _minute_metrics.put(snapshot);
            }

            // Update current minute start time
            _current_minute_start = now;
        }
    }

    void CacheTracer::_flush_trace_extent()
    {
        if (!_current_trace || _current_trace->row_count() == 0) {
            return;
        }

        // Generate output filename with sequence number
        std::string filename = fmt::format("{}.{:06d}", _output_path.string(), _trace_sequence);
        std::filesystem::path output_file(filename);

        LOG_INFO("Flushing cache trace extent: {} rows, {} bytes to {}",
                 _current_trace->row_count(),
                 _current_trace->byte_count(),
                 output_file.string());

        try {
            // Open file for writing
            auto handle = IOMgr::get_instance()->open(output_file, IOMgr::APPEND, true);

            // Capture the extent to flush (move ownership to the async callback)
            auto extent_to_flush = _current_trace;

            // Perform async flush and store the future
            auto future = extent_to_flush->async_flush(handle);
            _pending_flushes.push_back(std::move(future));

            _trace_sequence++;
        } catch (const std::exception& e) {
            LOG_ERROR("Failed to flush cache trace extent: {}", e.what());
        }
    }

    uint64_t CacheTracer::_get_timestamp_us()
    {
        auto now = std::chrono::system_clock::now();
        auto duration = now.time_since_epoch();
        return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    }

    nlohmann::json CacheTracer::get_cache_metrics() const
    {
        nlohmann::json result;

        boost::unique_lock lock(_metrics_mutex);

        // Sum up the last 5 minutes from circular buffer plus current minute
        uint64_t hits_last_5min = 0;
        uint64_t misses_last_5min = 0;

        for (size_t i = 0; i < _minute_metrics.size(); ++i) {
            hits_last_5min += _minute_metrics[i].cache_hits;
            misses_last_5min += _minute_metrics[i].cache_misses;
        }

        // Add current (incomplete) minute
        hits_last_5min += _current_minute_hits.load();
        misses_last_5min += _current_minute_misses.load();

        uint64_t lookups_last_5min = hits_last_5min + misses_last_5min;

        result["cache_hits"] = hits_last_5min;
        result["cache_misses"] = misses_last_5min;
        result["total_lookups"] = lookups_last_5min;
        if (lookups_last_5min > 0) {
            result["hit_rate"] = (100.0 * hits_last_5min) / lookups_last_5min;
        } else {
            result["hit_rate"] = 0.0;
        }

        return result;
    }

    void CacheTracer::_create_trace_extent()
    {
        // Define the schema for trace entries
        std::vector<SchemaColumn> columns = {
            SchemaColumn("event_type", 0, SchemaType::UINT8, 0, false),    // Event type (HIT, MISS, FLUSH, EVICTION)
            SchemaColumn("timestamp", 1, SchemaType::UINT64, 0, false),    // Timestamp in microseconds
            SchemaColumn("filename", 2, SchemaType::TEXT, 0, false),       // Data file path
            SchemaColumn("extent_id", 3, SchemaType::UINT64, 0, false),    // Extent ID
            SchemaColumn("cache_id", 4, SchemaType::UINT64, 0, false)      // Cache ID (0 for clean extents)
        };

        // Create the schema
        _schema = std::make_shared<ExtentSchema>(columns);

        // Create extent header
        ExtentHeader header(ExtentType(), 0, _schema->row_size(), _schema->field_types());

        // Create new extent
        _current_trace = std::make_shared<Extent>(header);
    }

} // namespace springtail
