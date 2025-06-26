#include <memory>
#include <utility>

#include <opentelemetry/baggage/baggage_context.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/multi_span_processor.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/semconv/service_attributes.h>
#include <opentelemetry/trace/provider.h>

#include <common/json.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>

namespace springtail::open_telemetry {

void
OpenTelemetry::get_context_variables(opentelemetry::nostd::function_ref<bool(opentelemetry::nostd::string_view, opentelemetry::nostd::string_view)> callback)
{
    auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto baggage = opentelemetry::baggage::GetBaggage(ctx);
    std::unordered_map<std::string, std::string> attributes;

    // Iterate over all the baggage entries and populate the attributes object
    baggage->GetAllEntries(callback);
}

std::unique_ptr<opentelemetry::context::Token>
OpenTelemetry::set_context_variables(const std::unordered_map<std::string, std::string>& attributes)
{
    auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto baggage = opentelemetry::baggage::GetBaggage(ctx);

    // Iterate over attributes and set baggage values
    for (const auto& attribute : attributes) {
        baggage = baggage->Set(attribute.first, attribute.second);
    }

    auto updated_context = opentelemetry::baggage::SetBaggage(ctx, baggage);
    return opentelemetry::context::RuntimeContext::Attach(updated_context);
}

std::unique_ptr<opentelemetry::context::Token>
OpenTelemetry::set_context_variable(const std::string &attr_key, const std::string &attr_value)
{
    auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();

    auto baggage = opentelemetry::baggage::GetBaggage(ctx);
    baggage = baggage->Set(attr_key, attr_value);

    auto updated_context = opentelemetry::baggage::SetBaggage(ctx, baggage);
    return opentelemetry::context::RuntimeContext::Attach(updated_context);
}

void
OpenTelemetry::SpdlogExporter::_log_span(const opentelemetry::sdk::trace::SpanData& span)
{
    // Log basic span information
    std::string log_str = fmt::format("Span Name: {}", span.GetName().data());

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

        log_str += fmt::format("\n      Attribute - {}: {}", key, value_str);
    }

    OpenTelemetry::get_instance()->get_context_variables([&log_str](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
        log_str += fmt::format("\n      Attribute - {}: {}", std::string(key), std::string(value));
        return true;
    });

    // Span's start and end time can be logged if needed (if available)
    auto start_time = span.GetStartTime();
    auto duration = span.GetDuration();
    log_str += fmt::format("\n      Start Time: {} | Duration: {}",
        std::chrono::duration_cast<std::chrono::milliseconds>(start_time.time_since_epoch())
            .count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count());
    LOG_INFO("{}", log_str);
}

opentelemetry::sdk::resource::Resource
OpenTelemetry::_create_default_otel_resource(std::string_view component_name)
{
    // Define global resource attributes
    char *service_name = std::getenv("SERVICE_NAME");
    char *instance_key = std::getenv("INSTANCE_KEY");
    ::opentelemetry::sdk::resource::ResourceAttributes resource_attributes = {
        {"instance_id", std::to_string(springtail::Properties::get_db_instance_id())},
        {"organization_id", springtail::Properties::get_organization_id()},
        {"account_id", springtail::Properties::get_account_id()},
        {"service_name", (service_name != nullptr) ? service_name : "springtail"},
        {"instance_key", (instance_key != nullptr) ? instance_key : "unknown"}

    };
    if (!component_name.empty()) {
        resource_attributes[opentelemetry::semconv::service::kServiceName] = std::string(component_name);
    }
    return ::opentelemetry::sdk::resource::Resource::Create(resource_attributes);
}

void
OpenTelemetry::_init_metrics(const ::opentelemetry::sdk::resource::Resource& resource)
{
    opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions options;
    if (_otel_remote) {
        // host ex: http://otel_collector, port ex: 4318
        options.url = fmt::format("{}:{}/v1/metrics", *_host, *_port);
        LOG_INFO("Enabling OTel metrics over HTTP: {}", options.url);
    }
    ::opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_options;
    if (_metrics_export_interval_millis) {
        reader_options.export_interval_millis = std::chrono::milliseconds(*_metrics_export_interval_millis);
    }
    if (_metrics_export_timeout_millis) {
        reader_options.export_timeout_millis = std::chrono::milliseconds(*_metrics_export_timeout_millis);
    }

    auto exporter = opentelemetry::exporter::otlp::OtlpHttpMetricExporterFactory::Create(options);
    auto reader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
        std::move(exporter), reader_options);

    auto context = opentelemetry::sdk::metrics::MeterContextFactory::Create();
    context->AddMetricReader(std::move(reader));

    auto base_meter_provider =
        opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(context));
    _meter_provider = std::unique_ptr<::opentelemetry::sdk::metrics::MeterProvider>(
        static_cast<::opentelemetry::sdk::metrics::MeterProvider*>(base_meter_provider.release()));

    // Set as the global meter provider
    ::opentelemetry::metrics::Provider::SetMeterProvider(
        std::dynamic_pointer_cast<::opentelemetry::metrics::MeterProvider>(_meter_provider));

    // register counters
    for (const auto &counter : metrics::_counter_metrics) {
        _register_counter(counter.first, counter.second, "calls");
    }

    // register histograms
    for (const auto &histogram : metrics::_histogram_metrics) {
        _register_histogram(histogram.first, histogram.second, "ms");
    }
}

void
OpenTelemetry::_init_tracing(const opentelemetry::sdk::resource::Resource& resource)
{
    auto multi_processor = std::make_unique<opentelemetry::sdk::trace::MultiSpanProcessor>(
        std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>>{});

    if (!_otel_remote) {
        // create the SPDLOG exporter
        auto log_exporter = std::make_unique<OpenTelemetry::SpdlogExporter>();
        std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> log_processor =
            std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(std::move(log_exporter));
        LOG_INFO("Enabling OTel logging");
        multi_processor->AddProcessor(std::move(log_processor));
    }

    // check if we should send to an otlp server
    if (_otel_remote) {
        opentelemetry::exporter::otlp::OtlpHttpExporterOptions options;
        // host ex: http://otel_collector, port ex: 4318
        options.url = fmt::format("{}:{}/v1/traces", *_host, *_port);

        std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> otlp_exporter =
            std::make_unique<opentelemetry::exporter::otlp::OtlpHttpExporter>(options);

        opentelemetry::sdk::trace::BatchSpanProcessorOptions batch_options;
        batch_options.max_queue_size = 2048;
        batch_options.schedule_delay_millis = std::chrono::milliseconds(5000);
        batch_options.max_export_batch_size = 512;

        std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> otlp_processor =
            std::make_unique<opentelemetry::sdk::trace::BatchSpanProcessor>(
                std::move(otlp_exporter), batch_options);

        LOG_INFO("Enabling OTel over HTTP: {}", options.url);
        multi_processor->AddProcessor(std::move(otlp_processor));
    }

    std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor =
        std::move(multi_processor);
    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
        std::make_shared<opentelemetry::sdk::trace::TracerProvider>(std::move(processor), resource);
    opentelemetry::trace::Provider::SetTracerProvider(provider);
}

void
OpenTelemetry::_init_logging(const opentelemetry::sdk::resource::Resource& resource)
{
    // Check OpenTelemetry configuration
    if (_otel_remote) {
        std::string log_endpoint = fmt::format("{}:{}/v1/logs", *_host, *_port);

        // Configure the OTLP exporter
        opentelemetry::exporter::otlp::OtlpHttpLogRecordExporterOptions options;
            options.url = log_endpoint;

        // Create the OTLP log exporter
        auto exporter = std::unique_ptr<opentelemetry::sdk::logs::LogRecordExporter>(
            new opentelemetry::exporter::otlp::OtlpHttpLogRecordExporter(options));

        // Create a processor with the exporter
        auto processor = std::unique_ptr<opentelemetry::sdk::logs::LogRecordProcessor>(
            new opentelemetry::sdk::logs::BatchLogRecordProcessor(std::move(exporter)));

        // Create and set the logger provider
        auto provider = std::shared_ptr<opentelemetry::logs::LoggerProvider>(
            new opentelemetry::sdk::logs::LoggerProvider(std::move(processor), resource));
        opentelemetry::logs::Provider::SetLoggerProvider(provider);

        // Get the logger with required parameters
        _logger = provider->GetLogger(
            "springtail",                   // logger name
            "",                            // library name
            OPENTELEMETRY_SDK_VERSION,     // library version
            "",                            // schema URL
            {}                             // empty attributes
        );

        _remote_log_level_value = logging::Logger::get_log_level_from_string(_remote_log_level);

        // Source properties
        _log_attributes.emplace_back("source_file", "");
        _log_attributes.emplace_back("source_line", "");
        _log_attributes.emplace_back("source_func", "");

        LOG_INFO("Enabling OTel logging sink with endpoint: {}", log_endpoint);
    } else {
        LOG_INFO("OpenTelemetry logging sink disabled via configuration");
    }
}

/** Log severity mapping from SPDLOG to OTEL */
static opentelemetry::logs::Severity severity_map[spdlog::level::n_levels] = {
    opentelemetry::logs::Severity::kTrace,
    opentelemetry::logs::Severity::kDebug,
    opentelemetry::logs::Severity::kInfo,
    opentelemetry::logs::Severity::kWarn,
    opentelemetry::logs::Severity::kError,
    opentelemetry::logs::Severity::kFatal,
    // TODO: not sure if we should map spdlog::level::off to kInfo
    opentelemetry::logs::Severity::kInfo
};

void
OpenTelemetry::_log(const spdlog::details::log_msg &msg)
{
    // only log messages with allowed log level
    if (msg.level < _remote_log_level_value) {
        return;
    }

    opentelemetry::logs::Severity severity;
    if (msg.level < spdlog::level::n_levels) {
        severity = severity_map[msg.level];
    } else {
        severity = opentelemetry::logs::Severity::kInfo;
    }

    // Format the log message
    spdlog::memory_buf_t formatted;
    get_instance()->_formatter.format(msg, formatted);
    std::string log_message{formatted.data(), formatted.size()};

    std::vector<std::pair<std::string, std::string>> attributes = _log_attributes;
    attributes[0].second = std::move(msg.source.filename ? msg.source.filename : "");
    std::string line_str = std::to_string(msg.source.line);
    attributes[1].second = std::move(line_str);
    attributes[2].second = std::move(msg.source.funcname ? msg.source.funcname : "");

    get_context_variables([&attributes](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
        attributes.push_back(std::make_pair(std::string(key), std::string(value)));
        return true;
    });

    auto attributes_view = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};

    // Send to OpenTelemetry with source information
    _logger->Log(severity, opentelemetry::nostd::string_view{log_message}, attributes_view);
}

void
OpenTelemetry::init(std::string_view component_name)
{
    // check the otel properties
    auto json = Properties::get(Properties::OTEL_CONFIG);
    LOG_INFO("OTel: {}", json.dump());
    _otel_enabled = Json::get_or<bool>(json, "enabled", false);
    _otel_remote = Json::get_or<bool>(json, "remote", false);
    _metrics_export_interval_millis = Json::get<int>(json, "metrics_export_interval_millis");
    _metrics_export_timeout_millis = Json::get<int>(json, "metrics_export_timeout_millis");
    _remote_log_level = Json::get_or<std::string>(json, "remote_log_level", "info");
    _host = Json::get<std::string>(json, "host");
    _port = Json::get<int>(json, "port");

    if (_otel_enabled) {
        if (_otel_remote && (!_host.has_value() || !_port.has_value())) {
            LOG_CRITICAL("Missing OTEL definitions for host and/or port");
        }
        auto resource = _create_default_otel_resource(component_name);
        _init_metrics(resource);
        _init_tracing(resource);
        _init_logging(resource);
    } else {
        // use the Noop provider to drop all collected metrics
        LOG_INFO("Disabling OTel via NoopTracer/MeterProvider");
        std::shared_ptr<opentelemetry::trace::TracerProvider> trace_provider =
            std::make_shared<opentelemetry::trace::NoopTracerProvider>();
        opentelemetry::trace::Provider::SetTracerProvider(std::move(trace_provider));
        std::shared_ptr<opentelemetry::metrics::MeterProvider> meter_provider =
            std::make_shared<opentelemetry::metrics::NoopMeterProvider>();
        opentelemetry::metrics::Provider::SetMeterProvider(std::move(meter_provider));
    }
    _inited_flag = true;
}

void
OpenTelemetry::flush()
{
    if (_inited_flag && !_shutdown_flag && get_instance()->_otel_enabled && get_instance()->_otel_remote) {
        get_instance()->_meter_provider->ForceFlush();
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider> trace_provider =
            opentelemetry::trace::Provider::GetTracerProvider();
        dynamic_cast<opentelemetry::sdk::trace::TracerProvider *>(trace_provider.get())->ForceFlush();
        auto logger_provider = opentelemetry::logs::Provider::GetLoggerProvider();
        dynamic_cast<opentelemetry::sdk::logs::LoggerProvider *>(logger_provider.get())->ForceFlush();
    }
}

void
OpenTelemetry::_internal_shutdown()
{
    flush();
    opentelemetry::trace::Provider::SetTracerProvider({});
    opentelemetry::metrics::Provider::SetMeterProvider({});
    _meter_provider.reset();
    _shutdown_flag = true;
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
OpenTelemetry::_tracer(const std::string_view& name)
{
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(name.data());
}

void
OpenTelemetry::_increment_counter(std::string_view name)
{
    if (!_otel_enabled) {
        return;
    }
    std::vector<std::pair<std::string, std::string>> attributes;
    get_context_variables([&attributes](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
        attributes.push_back(std::make_pair(std::string(key), std::string(value)));
        return true;
    });

    auto attributes_view = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};
    auto counter = _counters[name];
    if(counter){
        counter->Add(1, attributes_view, opentelemetry::context::RuntimeContext::GetCurrent());
    } else {
        LOG_ERROR("Counter '{}' not found", name);
    }
}

void
OpenTelemetry::_record_histogram(std::string_view name, double value)
{
    if (!_otel_enabled) {
        return;
    }
    std::vector<std::pair<std::string, std::string>> attributes;
    get_context_variables([&attributes](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
        attributes.push_back(std::make_pair(std::string(key), std::string(value)));
        return true;
    });

    auto attributes_view = opentelemetry::common::KeyValueIterableView<decltype(attributes)>{attributes};
    auto histogram = _histograms[name];
    if(histogram){
        histogram->Record(value, attributes, opentelemetry::context::RuntimeContext::GetCurrent());
    } else {
        LOG_ERROR("Histogram '{}' not found", name);
    }
}

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>
OpenTelemetry::_create_uint64_counter(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateUInt64Counter(name, description, unit);
}

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>
OpenTelemetry::_create_double_histogram(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateDoubleHistogram(name, description, unit);
}

void OpenTelemetry::_register_counter(std::string_view name, std::string_view description, std::string_view unit)
{
    _counters[name] = _create_uint64_counter(std::string(name), std::string(description), std::string(unit));
}

void OpenTelemetry::_register_histogram(std::string_view name, std::string_view description, std::string_view unit)
{
    _histograms[name] = _create_double_histogram(std::string(name), std::string(description), std::string(unit));
}

}  // namespace springtail::tracing
