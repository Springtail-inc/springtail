#include <stdio.h>

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/exporters/ostream/metric_exporter_factory.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/aggregation/aggregation_config.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h>
#include <opentelemetry/sdk/metrics/instruments.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/metric_reader.h>
#include <opentelemetry/sdk/metrics/push_metric_exporter.h>
#include <opentelemetry/sdk/metrics/state/filtered_ordered_attribute_map.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/view.h>
#include <opentelemetry/sdk/metrics/view/view_factory.h>

#include <common/logging.hh>
#include <common/exception.hh>
#include <common/metrics.hh>

namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace exportermetrics = opentelemetry::exporter::metrics;

namespace springtail {
    Metrics *Metrics::_instance = nullptr;
    std::once_flag Metrics::_init_flag;
    std::once_flag Metrics::_shutdown_flag;

    Metrics::Metrics()
    {
         // Initialize and set the global MeterProvider
        metrics_sdk::PeriodicExportingMetricReaderOptions options;
        options.export_interval_millis = std::chrono::milliseconds(1000);
        options.export_timeout_millis  = std::chrono::milliseconds(500);

        auto exporter = exportermetrics::OStreamMetricExporterFactory::Create();

        auto reader =
            metrics_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter), options);

        auto provider = opentelemetry::sdk::metrics::MeterProviderFactory::Create();

        provider->AddMetricReader(std::move(reader));
    }
} // namespace springtail