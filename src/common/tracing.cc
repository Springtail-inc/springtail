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

static std::map<std::string, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>> counters;

static std::map<std::string, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>> histograms;

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

void
increment_counter(std::string name)
{
    auto counter = counters[name];
    if(counter){
        // Increment the counter
        counter->Add(1);
    } else {
        SPDLOG_ERROR("Counter '{}' not found", name);
    }
}

void record_histogram(std::string name, double value)
{
    auto histogram = histograms[name];
    if(histogram){
        histogram->Record(value);
    } else {
        SPDLOG_ERROR("Histogram '{}' not found", name);
    }
}

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>
create_uint64_counter(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateUInt64Counter(name.data(), description.data(), unit.data());
}

opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>
create_double_histogram(const std::string name, const std::string description, const std::string unit)
{
    auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(name);
    return meter->CreateDoubleHistogram(name.data(), description.data(), unit.data());
}

void register_counter(const std::string name, const std::string description, const std::string unit)
{
    counters[name] = create_uint64_counter(name, description, unit);
}

void register_histogram(const std::string name, const std::string description, const std::string unit)
{
    histograms[name] = create_double_histogram(name, description, unit);
}

}  // namespace springtail::tracing
