#include <opentelemetry/exporters/otlp/otlp_grpc_exporter.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/multi_span_processor.h>

#include <common/json.hh>
#include <common/properties.hh>
#include <common/tracing.hh>

namespace springtail::tracing {
    void init_tracing() {
        // check the otel properties
        auto json = Properties::get(Properties::OTEL_CONFIG);
        bool enabled = Json::get_or<bool>(json, "enabled", true);

        if (enabled) {
            auto multi_processor =
                std::make_unique<opentelemetry::sdk::trace::MultiSpanProcessor>(std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>>{});

            // create the SPDLOG exporter
            auto log_exporter = std::make_unique<SpdlogExporter>();
            std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> log_processor =
                std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(std::move(log_exporter));
            multi_processor->AddProcessor(std::move(log_processor));

            // check if we should send to an otlp server
            auto host = Json::get<std::string>(json, "host");
            auto port = Json::get<int>(json, "host");
            if (host && port) {
                opentelemetry::exporter::otlp::OtlpGrpcExporterOptions options;
                options.endpoint = fmt::format("http://{}:{}", *host, *port);

                std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> otlp_exporter =
                    std::make_unique<opentelemetry::exporter::otlp::OtlpGrpcExporter>(options);
                std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> otlp_processor =
                    std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(std::move(otlp_exporter));

                multi_processor->AddProcessor(std::move(otlp_processor));
            }

            std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor> processor = std::move(multi_processor);
            std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
                std::make_shared<opentelemetry::sdk::trace::TracerProvider>(std::move(processor));
            opentelemetry::trace::Provider::SetTracerProvider(provider);
        } else {
            // use the Noop provider to drop all collected metrics
            std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
                std::make_shared<opentelemetry::trace::NoopTracerProvider>();
            opentelemetry::trace::Provider::SetTracerProvider(std::move(provider));
        }
    }

    void shutdown_tracing() {
        std::shared_ptr<opentelemetry::trace::TracerProvider> none;
        opentelemetry::trace::Provider::SetTracerProvider(none);
    }

    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
    tracer(const std::string_view &name)
    {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        return provider->GetTracer(name.data());
    }
}
