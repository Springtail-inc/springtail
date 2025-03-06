#pragma once

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

template <typename T>
class GrpcClient {
protected:
    GrpcClient() { _type_name = boost::core::demangle(typeid(T).name()); }

    virtual ~GrpcClient() = default;

    // Helper to check if status should be retried
    bool should_retry(const grpc::Status& status) const
    {
        return status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED ||
               status.error_code() == grpc::StatusCode::UNAVAILABLE;
    }

    // Template method for retrying RPC calls with exception handling
    template <typename Func>
    void retry_rpc(Func&& func, std::string_view operation)
    {
        auto status = retry_rpc_status(std::forward<Func>(func), operation);
        if (!status.ok()) {
            throw Error(fmt::format("{} failed: {}", operation, status.error_message()));
        }
    }

    // Template method for retrying RPC calls
    // Returns grpc::Status
    template <typename Func>
    auto retry_rpc_status(Func&& func, std::string_view operation)
    {
        using namespace std::chrono;

        int attempts = 0;
        milliseconds backoff(100);             // Start with 100ms
        const milliseconds max_backoff(5000);  // Max 5 seconds
        const int max_attempts = 50;

        while (true) {
            auto status = func();

            if (status.ok()) {
                return status;
            }

            attempts++;
            if (!should_retry(status) || attempts >= max_attempts) {
                SPDLOG_WARN("{}: {} failed after {} attempts: {}", _type_name, operation, attempts,
                            status.error_message());
                return status;
            }

            SPDLOG_WARN("{}: {} attempt {} failed, retrying in {}ms: {}", _type_name, operation,
                        attempts, backoff.count(), status.error_message());

            std::this_thread::sleep_for(backoff);
            backoff = std::min(backoff * 2, max_backoff);
        }
    }

    std::shared_ptr<grpc::Channel> create_channel(const std::string& server_hostname,
                                                  const nlohmann::json& rpc_json)
    {
        bool ssl = Json::get_or<bool>(rpc_json, "ssl", false);

        grpc::ChannelArguments args;
        std::shared_ptr<grpc::ChannelCredentials> creds;
        if (ssl) {
            std::string cert_file_path;
            Json::get_to<std::string>(rpc_json, "client_cert", cert_file_path);
            if (cert_file_path.empty() || !std::filesystem::exists(cert_file_path)) {
                SPDLOG_ERROR("{}: Invalid configuration for certificate file {}", _type_name,
                             cert_file_path);
                throw Error("Certificate file path is misconfigured");
            }

            std::string key_file_path;
            Json::get_to<std::string>(rpc_json, "client_key", key_file_path);
            if (key_file_path.empty() || !std::filesystem::exists(key_file_path)) {
                SPDLOG_ERROR("{}: Invalid configuration for key file {}", _type_name,
                             key_file_path);
                throw Error("Key file path is misconfigured");
            }

            std::string trusted_file_path;
            Json::get_to<std::string>(rpc_json, "client_trusted", trusted_file_path);
            if (trusted_file_path.empty() || !std::filesystem::exists(trusted_file_path)) {
                SPDLOG_ERROR("{}: Invalid configuration for trusted certificates file {}",
                             _type_name, trusted_file_path);
                throw Error("Trusted certificates file path is misconfigured");
            }

            grpc::SslCredentialsOptions ssl_opts;
            ssl_opts.pem_root_certs = read_file_contents(trusted_file_path);
            ssl_opts.pem_private_key = read_file_contents(key_file_path);
            ssl_opts.pem_cert_chain = read_file_contents(cert_file_path);

            creds = grpc::SslCredentials(ssl_opts);
            args.SetSslTargetNameOverride("springtail_server");  // This must match CN in the server cert
        } else {
            creds = grpc::InsecureChannelCredentials();
        }

        int port;
        Json::get_to<int>(rpc_json, "server_port", port);
        std::string server_addr = server_hostname + ":" + std::to_string(port);

        SPDLOG_DEBUG("{}: Creating channel to {} with SSL: {}", _type_name, server_addr, ssl);
        return grpc::CreateCustomChannel(server_addr, creds, args);
    }

private:
    std::string read_file_contents(const std::string& path)
    {
        std::ifstream file(path);
        if (!file.is_open()) {
            throw Error("Failed to open file: " + path);
        }
        return std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    }

protected:
    std::string _type_name;
};

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