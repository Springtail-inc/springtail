#pragma once

#include <string_view>
#include <memory>
#include <unordered_map>

#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/trace/tracer.h>

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>

#include <common/singleton.hh>
#include <common/metric_constants.hh>
#include <common/logging.hh>

namespace springtail::tracing {
/** Convenience name */
using SpanPtr = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;

/**
 * @brief A custom span exporter that logs OpenTelemetry spans using spdlog
 * 
 * This exporter implements the OpenTelemetry SpanExporter interface to export
 * tracing spans by logging them through spdlog. It processes each span's name,
 * attributes, and other properties and outputs them as structured log messages.
 */
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

class TracingAndMetrics : public Singleton<TracingAndMetrics> {
    friend class Singleton<TracingAndMetrics>;
public:
    /**
    * @brief Initialize the tracing system
    */
    void init(std::string_view component_name);

    /**
     * @brief Increment a counter
     */
    void increment_counter(std::string_view name,
        const std::unordered_map<std::string, std::string>& attributes = {});

    /**
     * @brief Record a histogram
     */
    void record_histogram(std::string_view name, double value,
        const std::unordered_map<std::string, std::string>& attributes = {});

    /**
    * @brief Retrieve the otel Tracer by name.
    */
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer(const std::string_view& name);

    /**
     * @brief Get the db id xid map
     */
    std::unordered_map<std::string, std::string> get_db_id_xid_map(uint64_t db_id, uint64_t xid);

private:
    TracingAndMetrics() = default;
    ~TracingAndMetrics() override = default;
    void _internal_shutdown() override;

    opentelemetry::context::Context _context;

    /**
    * @brief Map of counter names to their corresponding counters
    */
    std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>> _counters;

    /**
    * @brief Map of histogram names to their corresponding histograms
    */
    std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>> _histograms;

    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> _meter_provider;

    void _init_metrics(const opentelemetry::sdk::resource::Resource& resource);
    void _init_tracing(const opentelemetry::sdk::resource::Resource& resource);
    opentelemetry::sdk::resource::Resource _create_default_otel_resource(std::string_view component_name);

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
     * @brief Create a uint64 counter with the given name, description, and unit
     * @param name The name of the counter
     * @param description The description of the counter
     * @param unit The unit of the counter
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>
    _create_uint64_counter(const std::string name, const std::string description, const std::string unit);

    /**
     * @brief Create a double histogram with the given name, description, and unit
     * @param name The name of the histogram
     * @param description The description of the histogram
     * @param unit The unit of the histogram
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>
    _create_double_histogram(const std::string name, const std::string description, const std::string unit);

    /**
     * @brief Register a counter with the given name, description, and unit
     * @param name The name of the counter
     * @param description The description of the counter
     * @param unit The unit of the counter
     */
    void _register_counter(std::string_view name, std::string_view description, std::string_view unit);

    /**
     * @brief Register a histogram with the given name, description, and unit
     * @param name The name of the histogram
     * @param description The description of the histogram
     * @param unit The unit of the histogram
     */
    void _register_histogram(std::string_view name, std::string_view description, std::string_view unit);

};

inline void increment_counter(std::string_view name,
    const std::unordered_map<std::string, std::string>& attributes = {}) {
    TracingAndMetrics::get_instance()->increment_counter(name, attributes);
}

inline void record_histogram(std::string_view name, double value,
    const std::unordered_map<std::string, std::string>& attributes = {}) {
    TracingAndMetrics::get_instance()->record_histogram(name, value, attributes);
}

inline std::unordered_map<std::string, std::string> get_db_id_xid_map(uint64_t db_id, uint64_t xid) {
    return TracingAndMetrics::get_instance()->get_db_id_xid_map(db_id, xid);
}

inline opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer(const std::string_view& name) {
    return TracingAndMetrics::get_instance()->tracer(name);
}


}  // namespace springtail::tracing