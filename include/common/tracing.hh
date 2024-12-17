#pragma once

#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/sdk/trace/recordable.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>

#include <common/logging.hh>

namespace springtail::tracing {
    class SpdlogExporter : public opentelemetry::sdk::trace::SpanExporter {
    public:
        std::unique_ptr<opentelemetry::sdk::trace::Recordable>
        MakeRecordable() noexcept override
        {
            return std::make_unique<opentelemetry::sdk::trace::SpanData>();
        }

        bool
        Shutdown(std::chrono::microseconds timeout) noexcept override
        {
            return true;
        }

        // Export a batch of spans (but we log each span individually in this example)
        opentelemetry::sdk::common::ExportResult
        Export(const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>> &spans) noexcept override
        {
            for (const auto& span : spans) {
                const auto* span_data = dynamic_cast<const opentelemetry::sdk::trace::SpanData*>(span.get());
                log_span(*span_data); // Log each span using spdlog
            }
            return opentelemetry::sdk::common::ExportResult::kSuccess;
        }

    private:
        void
        log_span(const opentelemetry::sdk::trace::SpanData& span)
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
            SPDLOG_INFO("Start Time: {} | Duration: {}",
                        std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch()).count(),
                        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
        }
    };

    /**
     * @brief Initialize the tracing system
     */
    void init_tracing();

    /**
     * @brief Shutdown the tracing system
     */
    void shutdown_tracing();

    /**
     * @brief Retrieve the otel Tracer by name.
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer(const std::string_view &name);
}
