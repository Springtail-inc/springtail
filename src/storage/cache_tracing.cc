#include <storage/cache_tracing.hh>

#include <absl/log/log.h>
#include <iomanip>
#include <sstream>

#include <common/constants.hh>
#include <storage/io.hh>
#include <storage/schema.hh>
#include <storage/field.hh>

namespace springtail {

    CacheTracer::CacheTracer(uint8_t event_mask,
                             const std::filesystem::path& output_path,
                             uint64_t flush_threshold)
        : _event_mask(event_mask),
          _output_path(output_path),
          _flush_threshold(flush_threshold),
          _trace_sequence(0)
    {
        LOG_INFO("CacheTracer initialized: output={}, threshold={} bytes, event_mask=0x{:x}",
                 output_path.string(), flush_threshold, event_mask);

        // Create the output directory if it doesn't exist
        if (_output_path.has_parent_path()) {
            std::filesystem::create_directories(_output_path.parent_path());
        }

        // Create the initial trace extent
        _create_trace_extent();
    }

    CacheTracer::~CacheTracer()
    {
        // Flush any remaining trace data
        flush();

        // Wait for all pending async flushes to complete
        for (auto& future : _pending_flushes) {
            try {
                future.wait();
            } catch (const std::exception& e) {
                LOG_ERROR("Error waiting for trace flush: {}", e.what());
            }
        }

        LOG_INFO("CacheTracer shutdown");
    }

    void CacheTracer::record_event(EventType event_type,
                                   const std::filesystem::path& filename,
                                   uint64_t extent_id,
                                   uint64_t cache_id)
    {
        // Check if this event type should be traced
        if (!should_trace(event_type)) {
            return;
        }

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
        boost::unique_lock lock(_mutex);
        if (_current_trace && _current_trace->row_count() > 0) {
            _flush_trace_extent();
            _create_trace_extent();
        }
    }

    void CacheTracer::_flush_trace_extent()
    {
        if (!_current_trace || _current_trace->row_count() == 0) {
            return;
        }

        // Generate output filename with sequence number
        std::ostringstream filename;
        filename << _output_path.string() << "." << std::setw(6) << std::setfill('0') << _trace_sequence;
        std::filesystem::path output_file(filename.str());

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
