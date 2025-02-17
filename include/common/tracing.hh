#pragma once

#include <string_view>
#include <memory>

#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/recordable.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>

#include <common/logging.hh>

namespace springtail {

// xid_mgr counter metrics
constexpr std::string_view XID_MGR_RECORD_DDL_CHANGE_CALLS = "xid_mgr_record_ddl_change_calls";
constexpr std::string_view XID_MGR_GET_PARTITION_CALLS = "xid_mgr_get_partition_calls";
constexpr std::string_view XID_MGR_GET_COMMITTED_XID_CALLS = "xid_mgr_get_committed_xid_calls";
constexpr std::string_view XID_MGR_COMMIT_XID_CALLS = "xid_mgr_commit_xid_calls";

// xid_mgr histogram metrics
constexpr std::string_view XID_MGR_COMMIT_XID_LATENCIES = "xid_mgr_commit_xid_latencies";
constexpr std::string_view XID_MGR_RECORD_DDL_CHANGE_LATENCIES = "xid_mgr_record_ddl_change_latencies";

// storage cache counter metrics
constexpr std::string_view STORAGE_CACHE_GET_CALLS = "storage_cache_get_calls";
constexpr std::string_view STORAGE_CACHE_GET_CACHE_MISSES = "storage_cache_get_cache_misses";
constexpr std::string_view STORAGE_CACHE_PUT_CALLS = "storage_cache_put_calls";
constexpr std::string_view STORAGE_CACHE_FLUSH_CALLS = "storage_cache_flush_calls";
constexpr std::string_view STORAGE_CACHE_DROP_CALLS = "storage_cache_drop_calls";

// storage cache histogram metrics
constexpr std::string_view STORAGE_CACHE_GET_LATENCIES = "storage_cache_get_latencies";
constexpr std::string_view STORAGE_CACHE_WRITE_LATENCIES = "storage_cache_write_latencies";
constexpr std::string_view STORAGE_CACHE_FLUSH_LATENCIES = "storage_cache_flush_latencies";
constexpr std::string_view STORAGE_CACHE_DROP_LATENCIES = "storage_cache_drop_latencies";

namespace tracing {
// counter metrics
inline const std::vector<std::pair<std::string_view, std::string_view>> _counter_metrics = {
    // xid_mgr counter metrics
    {XID_MGR_RECORD_DDL_CHANGE_CALLS, "Total number of XID record DDL change calls"},
    {XID_MGR_GET_PARTITION_CALLS, "Total number of XID get partition calls"},
    {XID_MGR_GET_COMMITTED_XID_CALLS, "Total number of XID get committed xid calls"},
    {XID_MGR_COMMIT_XID_CALLS, "Total number of XID commit xid calls"},
    
    // storage cache counter metrics
    {STORAGE_CACHE_GET_CALLS, "Total number of storage cache get calls"},
    {STORAGE_CACHE_GET_CACHE_MISSES, "Total number of storage cache get cache misses"},
    {STORAGE_CACHE_PUT_CALLS, "Total number of storage cache put calls"},
    {STORAGE_CACHE_FLUSH_CALLS, "Total number of storage cache flush calls"},
    {STORAGE_CACHE_DROP_CALLS, "Total number of storage cache drop calls"}
};

// histogram metrics
inline const std::vector<std::pair<std::string_view, std::string_view>> _histogram_metrics = {
    // xid_mgr histogram metrics
    {XID_MGR_COMMIT_XID_LATENCIES, "Latency of XID commits"},
    {XID_MGR_RECORD_DDL_CHANGE_LATENCIES, "Latency of XID record DDL change calls"},

    // storage cache histogram metrics
    {STORAGE_CACHE_GET_LATENCIES, "Latency of storage cache get calls"},
    {STORAGE_CACHE_WRITE_LATENCIES, "Latency of storage cache write calls"},
    {STORAGE_CACHE_FLUSH_LATENCIES, "Latency of storage cache flush calls"},
    {STORAGE_CACHE_DROP_LATENCIES, "Latency of storage cache drop calls"}
};

/** Convenience name */
using SpanPtr = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;

class SpdlogExporter : public opentelemetry::sdk::trace::SpanExporter {
public:
    std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
    {
        return std::make_unique<opentelemetry::sdk::trace::SpanData>();
    }

    bool Shutdown(std::chrono::microseconds timeout) noexcept override { return true; }

    // Export a batch of spans (but we log each span individually in this example)
    opentelemetry::sdk::common::ExportResult Export(
        const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
            spans) noexcept override
    {
        for (const auto& span : spans) {
            const auto* span_data =
                dynamic_cast<const opentelemetry::sdk::trace::SpanData*>(span.get());
            log_span(*span_data);  // Log each span using spdlog
        }
        return opentelemetry::sdk::common::ExportResult::kSuccess;
    }

private:
    void log_span(const opentelemetry::sdk::trace::SpanData& span)
    {
        // Log basic span information
        SPDLOG_INFO("Span Name: {}", span.GetName().data());

        // You can log other properties (e.g., span attributes, start/end times, etc.)
        const auto& attributes = span.GetAttributes();
        for (const auto& [key, value] : attributes) {
            std::string value_str;

            // Handle different attribute types
            if (std::holds_alternative<std::string>(value)) {
                value_str = std::get<std::string>(value);
            } else if (std::holds_alternative<int64_t>(value)) {
                value_str = std::to_string(std::get<int64_t>(value));
            } else if (std::holds_alternative<int32_t>(value)) {
                value_str = std::to_string(std::get<int32_t>(value));
            } else if (std::holds_alternative<double>(value)) {
                value_str = std::to_string(std::get<double>(value));
            } else if (std::holds_alternative<bool>(value)) {
                value_str = std::to_string(std::get<bool>(value));
            } else {
                value_str = fmt::format("Unsupported Type: {}", value.index());
            }

            SPDLOG_INFO("Attribute - {}: {}", key, value_str);
        }

        // Span's start and end time can be logged if needed (if available)
        auto start_time = span.GetStartTime();
        auto duration = span.GetDuration();
        SPDLOG_INFO(
            "Start Time: {} | Duration: {}",
            std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch())
                .count(),
            std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
    }
};

/**
 * @brief Initialize the tracing system
 */
void init_tracing_and_metrics(std::string_view component_name);

/**
 * @brief Shutdown the tracing system
 */
void shutdown_tracing_and_metrics();

/**
 * @brief Retrieve the otel Tracer by name.
 */
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer(const std::string_view& name);


/**
 * @brief Register the metrics
 */
void _register_metrics();

/**
 * @brief Register the default counters
 */
void _register_counters();

/**
 * @brief Register the default histograms
 */
void _register_histograms();

/**
 * @brief Register a counter
 */
void _register_counter(std::string_view name, std::string_view description, std::string_view unit);

/**
 * @brief Register a histogram
 */
void _register_histogram(std::string_view name, std::string_view description, std::string_view unit);

/**
 * @brief Increment a counter
 */
void increment_counter(std::string_view name);

/**
 * @brief Record a histogram
 */
void record_histogram(std::string_view name, double value);
}  // namespace springtail::tracing
}  // namespace springtail