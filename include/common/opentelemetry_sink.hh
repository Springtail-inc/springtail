#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/baggage/baggage.h>
#include <opentelemetry/baggage/baggage_context.h>
#include <opentelemetry/common/macros.h>
#include <opentelemetry/common/timestamp.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/context/context_value.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/version.h>
#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/logs/processor.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor.h>
#include <iostream>
#include <mutex>
#include <string>

template<typename Mutex>
class OpenTelemetrySink : public spdlog::sinks::base_sink<Mutex>
{
public:
    OpenTelemetrySink(const std::string& logger_name, const std::string& endpoint) 
    {
        // Configure the OTLP exporter
        opentelemetry::exporter::otlp::OtlpGrpcLogRecordExporterOptions options;
        options.endpoint = endpoint;

        // Create the OTLP log exporter
        auto exporter = std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>(
            new opentelemetry::exporter::otlp::OtlpGrpcLogRecordExporter(options));

        // Create a processor with the exporter
        auto processor = std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor>(
            new opentelemetry::sdk::logs::SimpleLogRecordProcessor(std::move(exporter)));

        // Create and set the logger provider
        auto provider = std::shared_ptr<opentelemetry::logs::LoggerProvider>(
            new opentelemetry::sdk::logs::LoggerProvider(std::move(processor)));
        opentelemetry::logs::Provider::SetLoggerProvider(provider);

        // Get the logger with required parameters
        _logger = provider->GetLogger(
            logger_name,                    // logger name
            "",                            // library name
            OPENTELEMETRY_SDK_VERSION,     // library version
            "",                            // schema URL
            opentelemetry::common::KeyValueIterableView<std::map<std::string, std::string>>{
                {}  // empty attributes
            }
        );
    }

protected:
    void sink_it_(const spdlog::details::log_msg &msg) override
    {
        // Convert spdlog level to OpenTelemetry severity
        opentelemetry::logs::Severity severity;
        switch (msg.level) {
            case spdlog::level::trace:
                severity = opentelemetry::logs::Severity::kTrace;
                break;
            case spdlog::level::debug:
                severity = opentelemetry::logs::Severity::kDebug; 
                break;
            case spdlog::level::info:
                severity = opentelemetry::logs::Severity::kInfo;
                break;
            case spdlog::level::warn:
                severity = opentelemetry::logs::Severity::kWarn;
                break;
            case spdlog::level::err:
                severity = opentelemetry::logs::Severity::kError;
                break;
            case spdlog::level::critical:
                severity = opentelemetry::logs::Severity::kFatal;
                break;
            default:
                severity = opentelemetry::logs::Severity::kInfo;
        }

        // Format the log message
        spdlog::memory_buf_t formatted;
        spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
        std::string log_message{formatted.data(), formatted.size()};

        std::vector<std::pair<std::string, std::string>> attributes = get_context_attributes(msg);
        auto attributes_view = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};

        // Send to OpenTelemetry with source information
        _logger->Log(
            severity,
            opentelemetry::nostd::string_view{log_message},
            attributes_view
        );
    }

    std::vector<std::pair<std::string, std::string>>
    get_context_attributes(const spdlog::details::log_msg &msg)
    {
        std::string ctx_xid_str = "-1";
        std::string ctx_db_id_str = "-1";

        auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
        auto baggage_entries = opentelemetry::baggage::GetBaggage(ctx);

        baggage_entries->GetValue("xid", ctx_xid_str);
        baggage_entries->GetValue("db_id", ctx_db_id_str);

        // Create attributes
        std::vector<std::pair<std::string, std::string>> attributes = {
            {"source_file", msg.source.filename ? msg.source.filename : ""},
            {"source_line", std::to_string(msg.source.line)},
            {"source_func", msg.source.funcname ? msg.source.funcname : ""},
            {"meta", "extra_meta"},
            {"xid", ctx_xid_str},
            {"db_id", ctx_db_id_str}
        };

        return attributes;
    }

    void flush_() override {
        // OpenTelemetry handles flushing internally
    }

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger> _logger;
};

using OpenTelemetrySinkMt = OpenTelemetrySink<std::mutex>;