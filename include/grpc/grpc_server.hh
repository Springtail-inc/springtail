#pragma once

#include <string>

#include <grpcpp/server_context.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/semconv/incubating/rpc_attributes.h>

#include <common/logging.hh>

namespace springtail {

template<typename T>
class GrpcServerCarrier : public opentelemetry::context::propagation::TextMapCarrier {
public:
    explicit GrpcServerCarrier(T* context) : context_(context) {}
    GrpcServerCarrier() = default;

    opentelemetry::nostd::string_view Get(
        opentelemetry::nostd::string_view key) const noexcept override
    {
        auto it = context_->client_metadata().find({key.data(), key.size()});
        if (it != context_->client_metadata().end()) {
            return it->second.data();
        }
        return "";
    }

    void Set(opentelemetry::nostd::string_view /* key */,
             opentelemetry::nostd::string_view /* value */) noexcept override
    {
        // Not required for server
    }

    T* context_ = nullptr;
};

// RAII wrapper for OpenTelemetry spans in gRPC server methods
template<typename T>
class ServerSpan {
public:
    ServerSpan(T* server_context,
               const std::string& service_name,
               const std::string& method_name)
    {
        namespace trace = opentelemetry::trace;
        namespace context = opentelemetry::context;
        namespace semconv = opentelemetry::semconv;

        // Set up span options
        trace::StartSpanOptions options;
        options.kind = trace::SpanKind::kServer;

        // Extract context from gRPC metadata
        GrpcServerCarrier carrier(server_context);
        auto prop =
            context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto current_ctx = context::RuntimeContext::GetCurrent();
        auto new_context = prop->Extract(carrier, current_ctx);

        // Create the span
        std::string span_name = service_name + "/" + method_name;
        auto tracer = open_telemetry::OpenTelemetry::tracer("grpc");

        span_ = tracer->StartSpan(span_name,
                                  {{semconv::rpc::kRpcSystem, "grpc"},
                                   {semconv::rpc::kRpcService, service_name},
                                   {semconv::rpc::kRpcMethod, method_name}},
                                  options);

        // Set the span as active
        scope_ = tracer->WithActiveSpan(span_);
    }

    ~ServerSpan()
    {
        if (span_) {
            span_->End();
        }
    }

    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span() { return span_; }

private:
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
    std::optional<opentelemetry::trace::Scope> scope_;
};

}  // namespace springtail
