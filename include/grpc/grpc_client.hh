#pragma once

#include <fmt/format.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/semconv/incubating/rpc_attributes.h>
#include <opentelemetry/trace/provider.h>

#include <chrono>
#include <string_view>
#include <thread>

#include <boost/core/demangle.hpp>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/service_register.hh>
#include <fmt/format.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <nlohmann/json.hpp>

namespace springtail {

// Helper struct to hold both status and result
template <typename T>
struct StatusOr {
    grpc::Status status;
    T result;

    // Intentionally non-explicit to allow implicit conversion from grpc::Status
    explicit(false) StatusOr(grpc::Status s) : status(s) {}
    StatusOr(grpc::Status s, T r) : status(s), result(r) {}
};

// Special case for void return type
using StatusOnly = StatusOr<bool>;

namespace grpc_client {

class GrpcClientCarrier : public opentelemetry::context::propagation::TextMapCarrier {
public:
    explicit GrpcClientCarrier(grpc::ClientContext* context) : context_(context) {}

    opentelemetry::nostd::string_view Get(
        opentelemetry::nostd::string_view /* key */) const noexcept override
    {
        return "";
    }

    void Set(opentelemetry::nostd::string_view key,
             opentelemetry::nostd::string_view value) noexcept override
    {
        context_->AddMetadata(std::string(key), std::string(value));
    }

private:
    grpc::ClientContext* context_;
};

inline bool
should_retry(const grpc::Status& status)
{
    return status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED ||
           status.error_code() == grpc::StatusCode::INTERNAL ||
           status.error_code() == grpc::StatusCode::ABORTED ||
           status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
           status.error_code() == grpc::StatusCode::UNAVAILABLE;
}

// Template method for retrying RPC calls
template <typename Func>
grpc::Status
retry_rpc_status(std::string_view service, std::string_view operation, Func&& func)
{
    using namespace std::chrono;
    namespace trace = opentelemetry::trace;
    namespace context = opentelemetry::context;
    namespace semconv = opentelemetry::semconv;

    auto tracer = open_telemetry::OpenTelemetry::tracer("grpc");
    // Create span for this RPC attempt
    trace::StartSpanOptions options;
    options.kind = trace::SpanKind::kClient;

    auto span = tracer->StartSpan(
        fmt::format("{}/{}", service, operation),
        {
            {semconv::rpc::kRpcSystem, "grpc"},
            {semconv::rpc::kRpcService, std::string(service)},
            {semconv::rpc::kRpcMethod, std::string(operation)},
        },
        options);

    int attempts = 0;
    milliseconds backoff(100);             // Start with 100ms
    const milliseconds max_backoff(5000);  // Max 5 seconds
    const int max_attempts = 50;

    auto current_ctx = context::RuntimeContext::GetCurrent();

    while (true) {

        grpc::ClientContext client_context;
        GrpcClientCarrier carrier(&client_context);
        auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        propagator->Inject(carrier, current_ctx);

        // Inject context into gRPC metadata
        auto scope = tracer->WithActiveSpan(span);

        // Make the RPC call
        auto status = func(&client_context);

        span->SetAttribute(semconv::rpc::kRpcGrpcStatusCode,
            static_cast<int32_t>(status.error_code()));

        if (status.ok()) {
            span->SetStatus(trace::StatusCode::kOk);
            span->End();
            return status;
        }

        attempts++;
        if (!should_retry(status) || attempts >= max_attempts) {
            LOG_WARN("{}: {} failed after {} attempts: {}", service, operation, attempts,
                        status.error_message());
            span->SetStatus(trace::StatusCode::kError, status.error_message());
            span->End();
            return status;
        }

        LOG_WARN("{}: {} attempt {} failed, retrying in {}ms: {}", service, operation, attempts,
                    backoff.count(), status.error_message());

        span->SetStatus(trace::StatusCode::kError, fmt::format("Attempt {} failed, retrying: {}",
                                                               attempts, status.error_message()));
        std::this_thread::sleep_for(backoff);
        backoff = std::min(backoff * 2, max_backoff);
    }
}

// Template method for retrying RPC calls with exception handling
template <typename Func>
void
retry_rpc(std::string_view service, std::string_view operation, Func&& func)
{
    auto status = retry_rpc_status(service, operation, std::forward<Func>(func));
    if (!status.ok()) {
        throw Error(fmt::format("RPC call {} failed: {} on server due to exception", operation, status.error_message()));
    }
}

std::shared_ptr<grpc::Channel> create_channel(std::string_view service,
                                              const std::string& server_hostname,
                                              const nlohmann::json& rpc_json);

}  // namespace grpc_client
template <typename T>
class GrpcClientRunner : public ServiceRunner {
public:
    GrpcClientRunner() : ServiceRunner(boost::core::demangle(typeid(T).name())) {}

    bool start() override
    {
        T::get_instance();
        return true;
    }

    void stop() override
    {
        T::shutdown();
    }
};
}  // namespace springtail
