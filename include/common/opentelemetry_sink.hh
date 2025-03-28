#pragma once

#include <common/properties.hh>

#include <spdlog/spdlog.h>
#include <spdlog/pattern_formatter.h>

#include <spdlog/sinks/sink.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor.h>

#include <mutex>
#include <string>
#include <utility>

class OpenTelemetrySink : public spdlog::sinks::sink
{
public:
    OpenTelemetrySink(const std::string& logger_name, const std::string& endpoint) :
        _formatter(spdlog::details::make_unique<spdlog::pattern_formatter>())
    {
        // Configure the OTLP exporter
        opentelemetry::exporter::otlp::OtlpHttpLogRecordExporterOptions options;
        options.url = endpoint;

        // Create the OTLP log exporter
        auto exporter = std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>(
            new opentelemetry::exporter::otlp::OtlpHttpLogRecordExporter(options));

        // Create a processor with the exporter
        auto processor = std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor>(
            new opentelemetry::sdk::logs::BatchLogRecordProcessor(std::move(exporter)));

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

        // Source properties
        _attributes.emplace_back("source_file", "");
        _attributes.emplace_back("source_line", "");
        _attributes.emplace_back("source_func", "");

        // Instance properties
        _attributes.emplace_back("db_instance_id", std::to_string(springtail::Properties::get_db_instance_id()));
        _attributes.emplace_back("organization_id", springtail::Properties::get_organization_id());
        _attributes.emplace_back("account_id", springtail::Properties::get_account_id());

    }

private:
    static inline opentelemetry::logs::Severity _severity_map[spdlog::level::n_levels] = {
        opentelemetry::logs::Severity::kTrace,
        opentelemetry::logs::Severity::kDebug,
        opentelemetry::logs::Severity::kInfo,
        opentelemetry::logs::Severity::kWarn,
        opentelemetry::logs::Severity::kError,
        opentelemetry::logs::Severity::kFatal,
        // TODO: not sure if we should map spdlog::level::off to kInfo
        opentelemetry::logs::Severity::kInfo
    };

public:
    void
    log(const spdlog::details::log_msg &msg) override
    {
        // only log messages with allowed log level
        if (msg.level < level_) {
            return;
        }

        opentelemetry::logs::Severity severity;
        if (msg.level < spdlog::level::n_levels) {
            severity = _severity_map[msg.level];
        } else {
            severity = opentelemetry::logs::Severity::kInfo;
        }

        // Format the log message
        spdlog::memory_buf_t formatted;
        _formatter->format(msg, formatted);
        std::string log_message{formatted.data(), formatted.size()};

        std::vector<std::pair<std::string, std::string>> attributes = _get_context_attributes(msg);
        auto attributes_view = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};

        // Send to OpenTelemetry with source information
        _logger->Log(
            severity,
            opentelemetry::nostd::string_view{log_message},
            attributes_view
        );
    }

private:
    std::vector<std::pair<std::string, std::string>>
    _get_context_attributes(const spdlog::details::log_msg &msg)
    {
        std::vector<std::pair<std::string, std::string>> attributes = _attributes;
        attributes[0].second = std::move(msg.source.filename ? msg.source.filename : "");
        std::string line_str = std::to_string(msg.source.line);
        attributes[1].second = std::move(line_str);
        attributes[2].second = std::move(msg.source.funcname ? msg.source.funcname : "");

        // Transaction properties
        for (const auto& key : springtail::logging::get_context_variables()) {
            attributes.emplace_back(key.first, key.second);
        }

        return attributes;
    }

    void inline _set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) {
        _formatter = std::move(sink_formatter);
    }


    void inline _set_pattern(const std::string &pattern) {
        _set_formatter(spdlog::details::make_unique<spdlog::pattern_formatter>(pattern));
    }

public:
    void
    flush() override {
            // OpenTelemetry handles flushing internally
    }

    void
    set_pattern(const std::string &pattern) override
    {
        std::lock_guard<std::mutex> lock(_formatter_mutex);
        _set_pattern(pattern);

    }

    void
    set_formatter(std::unique_ptr<spdlog::formatter> sink_formatter) override
    {
        std::lock_guard<std::mutex> lock(_formatter_mutex);
        _set_formatter(std::move(sink_formatter));
    }

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger> _logger;
    std::unique_ptr<spdlog::formatter> _formatter;
    std::vector<std::pair<std::string, std::string>> _attributes;
    std::mutex _formatter_mutex;
};
