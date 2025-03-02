#include <opentelemetry/common/key_value_iterable_view.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_metric_exporter.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/multi_span_processor.h>
#include <opentelemetry/sdk/trace/simple_processor.h>

#include <common/json.hh>
#include <common/properties.hh>
#include <common/tracing.hh>
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h"
#include "opentelemetry/sdk/metrics/meter_context.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"

namespace springtail::tracing {

static opentelemetry::context::Context _context;

/**
 * @brief Map of counter names to their corresponding counters
 */
static std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>> counters;

/**
 * @brief Map of histogram names to their corresponding histograms
 */
static std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>> histograms;

static std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> meter_provider;

static opentelemetry::sdk::resource::Resource
create_default_otel_resource(std::string_view component_name)
{
    auto json = Properties::get(Properties::ORG_CONFIG);
    opentelemetry::sdk::resource::ResourceAttributes resource_attributes;
    auto organization_id = Json::get<std::string>(json, "organization_id");
    if (organization_id) {
        resource_attributes["springtail.organization_id"] = *organization_id;
    }
    auto account_id = Json::get<std::string>(json, "account_id");
    if (account_id) {
        resource_attributes["springtail.account_id"] = *account_id;
    }
    auto db_instance_id = Json::get<uint64_t>(json, "db_instance_id");
    if (db_instance_id) {
        resource_attributes["springtail.db_instance_id"] = std::to_string(*db_instance_id);
    }
    if (!component_name.empty()) {
        resource_attributes["service.name"] = std::string(component_name);
    }
    return opentelemetry::sdk::resource::Resource::Create(resource_attributes);
}

/**
 * @brief Register the metrics
 */
void _register_metrics(){
    // register the counters
    _register_counters();

    // register the histograms
    _register_histograms(); 
}

/**
 * @brief Register the counters
 */
void _register_counters(){
    for (const auto &counter : metrics::_counter_metrics) {  
        _register_counter(counter.first, counter.second, "calls");
    }
}

/**
 * @brief Register the histograms
 */
void _register_histograms(){
    for (const auto &histogram : metrics::_histogram_metrics) {
        _register_histogram(histogram.first, histogram.second, "ms");
    }
}

static void
init_metrics(const opentelemetry::sdk::resource::Resource& resource)
{
    // check if we should send to an otlp server
    auto json = Properties::get(Properties::OTEL_CONFIG);
    auto host = Json::get<std::string>(json, "metrics_host");
    auto port = Json::get<int>(json, "metrics_port");

    opentelemetry::exporter::otlp::OtlpGrpcMetricExporterOptions options;
    if (host && port) {
        options.endpoint = fmt::format("http://{}:{}", *host, *port);
        SPDLOG_INFO("Enabling OTel metrics over gRPC");
    }
    opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions reader_options;
    auto export_interval_millis = Json::get<int>(json, "metrics_export_interval_millis");
    if (export_interval_millis) {
        reader_options.export_interval_millis = std::chrono::milliseconds(*export_interval_millis);
    }
    auto export_timeout_millis = Json::get<int>(json, "metrics_export_timeout_millis");
    if (export_timeout_millis) {
        reader_options.export_timeout_millis = std::chrono::milliseconds(*export_timeout_millis);
    }

    auto exporter = opentelemetry::exporter::otlp::OtlpGrpcMetricExporterFactory::Create(options);
    auto reader = opentelemetry::sdk::metrics::PeriodicExportingMetricReaderFactory::Create(
        std::move(exporter), reader_options);

    auto context = opentelemetry::sdk::metrics::MeterContextFactory::Create();
    context->AddMetricReader(std::move(reader));

    auto base_meter_provider =
        opentelemetry::sdk::metrics::MeterProviderFactory::Create(std::move(context));
    meter_provider = std::unique_ptr<opentelemetry::sdk::metrics::MeterProvider>(
        static_cast<opentelemetry::sdk::metrics::MeterProvider*>(base_meter_provider.release()));

    // Set as the global meter provider
    opentelemetry::metrics::Provider::SetMeterProvider(
        std::dynamic_pointer_cast<opentelemetry::metrics::MeterProvider>(meter_provider));

    // register the metrics
    _register_metrics();

}

static void
init_tracing(const opentelemetry::sdk::resource::Resource& resource)
{
    auto multi_processor = std::make_unique<opentelemetry::sdk::trace::MultiSpanProcessor>(
        std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>>{});

    // create the SPDLOG exporter
    auto log_exporter = std::make_unique<SpdlogExporter>();
    std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> log_processor =
        std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(std::move(log_exporter));

    SPDLOG_INFO("Enabling OTel logging");
    multi_processor->AddProcessor(std::move(log_processor));

    // check if we should send to an otlp server
    auto json = Properties::get(Properties::OTEL_CONFIG);
    auto host = Json::get<std::string>(json, "host");
    auto port = Json::get<int>(json, "port");
    if (host && port) {
        opentelemetry::exporter::otlp::OtlpGrpcExporterOptions options;
        options.endpoint = fmt::format("http://{}:{}", *host, *port);

        std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> otlp_exporter =
            std::make_unique<opentelemetry::exporter::otlp::OtlpGrpcExporter>(options);
        std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> otlp_processor =
            std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(
                std::move(otlp_exporter));

        SPDLOG_INFO("Enabling OTel over gRPC");
        multi_processor->AddProcessor(std::move(otlp_processor));
    }

    std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor =
        std::move(multi_processor);
    std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
        std::make_shared<opentelemetry::sdk::trace::TracerProvider>(std::move(processor));
    opentelemetry::trace::Provider::SetTracerProvider(provider);
}

void
init_tracing_and_metrics(std::string_view component_name)
{
    // check the otel properties
    auto json = Properties::get(Properties::OTEL_CONFIG);
    SPDLOG_INFO("OTel: {}", json.dump());
    bool enabled = Json::get_or<bool>(json, "enabled", true);

    if (enabled) {
        auto resource = create_default_otel_resource(component_name);
        init_metrics(resource);
        init_tracing(resource);
    } else {
        // use the Noop provider to drop all collected metrics
        SPDLOG_INFO("Disabling OTel via NoopTracer/MeterProvider");
        std::shared_ptr<opentelemetry::trace::TracerProvider> trace_provider =
            std::make_shared<opentelemetry::trace::NoopTracerProvider>();
        opentelemetry::trace::Provider::SetTracerProvider(std::move(trace_provider));
        std::shared_ptr<opentelemetry::metrics::MeterProvider> meter_provider =
            std::make_shared<opentelemetry::metrics::NoopMeterProvider>();
        opentelemetry::metrics::Provider::SetMeterProvider(std::move(meter_provider));
    }
}

void
shutdown_tracing_and_metrics()
{
    opentelemetry::trace::Provider::SetTracerProvider({});
    if (meter_provider) {
        meter_provider->Shutdown();
    }
    opentelemetry::metrics::Provider::SetMeterProvider({});
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
tracer(const std::string_view& name)
{
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(name.data());
}

std::unordered_map<std::string, std::string>
_set_default_attributes(const std::unordered_map<std::string, std::string>& input_attributes)
{
    auto attributes = input_attributes;

    if (attributes.empty()) {
        attributes = std::unordered_map<std::string, std::string>();
    }

    auto db_instance_id = Properties::get_db_instance_id();
    auto organization_id = Properties::get_organization_id();
    auto account_id = Properties::get_account_id();
    
    attributes["organization_id"] = organization_id;
    attributes["account_id"] = account_id;
    attributes["db_instance_id"] = std::to_string(db_instance_id);

    return attributes;
}

std::unordered_map<std::string, std::string>
get_db_id_xid_map(uint64_t db_id, uint64_t xid)
{
    return std::unordered_map<std::string, std::string>{
        {"db_id", std::to_string(db_id)},
        {"xid", std::to_string(xid)}
    };
}

/**
 * @brief Increment a counter
 * @param name The name of the counter
 * @param attributes The attributes to record
 */
void
increment_counter(std::string_view name, const std::unordered_map<std::string, std::string>& attributes)
{
    auto counter = counters[name];
    if(counter){
        counter->Add(1, _set_default_attributes(attributes), _context);
    } else {
        SPDLOG_ERROR("Counter '{}' not found", name);
    }
}

/**
 * @brief Record a value in the histogram
 * @param name The name of the histogram
 * @param value The value to record
 * @param attributes The attributes to record
 */
void
record_histogram(std::string_view name, double value, const std::unordered_map<std::string, std::string>& attributes)
{
    auto histogram = histograms[name];
    if(histogram){
        histogram->Record(value, _set_default_attributes(attributes), _context);
    } else {
        SPDLOG_ERROR("Histogram '{}' not found", name);
    }
}

/**
 * @brief Create a uint64 counter with the given name, description, and unit
 * @param name The name of the counter
 * @param description The description of the counter
 * @param unit The unit of the counter
 */
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>
_create_uint64_counter(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateUInt64Counter(name, description, unit);
}

/**
 * @brief Create a double histogram with the given name, description, and unit
 * @param name The name of the histogram
 * @param description The description of the histogram
 * @param unit The unit of the histogram
 */
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>
_create_double_histogram(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateDoubleHistogram(name, description, unit);
}

/**
 * @brief Register a counter with the given name, description, and unit
 * @param name The name of the counter
 * @param description The description of the counter
 * @param unit The unit of the counter
 */
void _register_counter(std::string_view name, std::string_view description, std::string_view unit)
{
    counters[name] = _create_uint64_counter(std::string(name), std::string(description), std::string(unit));
}

/**
 * @brief Register a histogram with the given name, description, and unit
 * @param name The name of the histogram
 * @param description The description of the histogram
 * @param unit The unit of the histogram
 */
void _register_histogram(std::string_view name, std::string_view description, std::string_view unit)
{
    histograms[name] = _create_double_histogram(std::string(name), std::string(description), std::string(unit));
}

}  // namespace springtail::tracing
