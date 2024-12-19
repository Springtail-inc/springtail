#include <common/tracing.hh>

namespace springtail::tracing {
    void init_tracing() {
        auto exporter = std::make_unique<SpdlogExporter>();
        auto processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
        std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
            opentelemetry::sdk::trace::TracerProviderFactory::Create(std::move(processor));

        //set the global trace provider
        opentelemetry::trace::Provider::SetTracerProvider(provider);
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
