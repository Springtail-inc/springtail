#include <grpc/grpc_server_manager.hh>

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <common/json.hh>
#include <common/logging.hh>

#include <grpcpp/server_builder.h>

namespace springtail {

void
GrpcServerManager::init(const nlohmann::json& rpc_json)
{
    _port = Json::get_or<int>(rpc_json, "server_port", 50051);
    _worker_thread_count = Json::get_or<int>(rpc_json, "server_worker_threads", 1);
    _ssl = Json::get_or<bool>(rpc_json, "ssl", false);

    if (_ssl) {
        std::string cert_file_path = Json::get_or<std::string>(rpc_json, "server_cert", "");
        std::string key_file_path = Json::get_or<std::string>(rpc_json, "server_key", "");
        std::string trusted_file_path = Json::get_or<std::string>(rpc_json, "server_trusted", "");

        if (cert_file_path.empty() || key_file_path.empty() || trusted_file_path.empty() ||
            !std::filesystem::exists(cert_file_path) || !std::filesystem::exists(key_file_path) ||
            !std::filesystem::exists(trusted_file_path)) {
            throw std::runtime_error("Invalid SSL configuration");
        }

        _server_cert = read_file_contents(cert_file_path);
        _server_key = read_file_contents(key_file_path);
        _root_cert = read_file_contents(trusted_file_path);
    }
}

std::string
GrpcServerManager::read_file_contents(const std::string& path)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + path);
    }

    return std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
}

void
GrpcServerManager::addService(grpc::Service* service)
{
    _services.push_back(service);
}

void
GrpcServerManager::startup()
{
    grpc::ServerBuilder builder;
    std::string address = "0.0.0.0:" + std::to_string(_port);

    // Configure resource quota
    grpc::ResourceQuota rq;
    LOG_INFO("Setting gRPC server max threads to {}", _worker_thread_count);
    rq.SetMaxThreads(_worker_thread_count);
    builder.SetResourceQuota(rq);
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

    if (_ssl) {
        grpc::SslServerCredentialsOptions::PemKeyCertPair key_cert_pair = {_server_key,
                                                                           _server_cert};
        grpc::SslServerCredentialsOptions opts;
        opts.pem_root_certs = _root_cert;
        opts.pem_key_cert_pairs.push_back(key_cert_pair);
        opts.client_certificate_request =
            GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_BUT_DONT_VERIFY;
        auto creds = grpc::SslServerCredentials(opts);
        builder.AddListeningPort(address, creds);
    } else {
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    }

    // Register all services with the builder
    for (auto service : _services) {
        builder.RegisterService(service);
    }

    _server = builder.BuildAndStart();
    if (!_server) {
        throw std::runtime_error("Failed to start GRPC server");
    }
    LOG_INFO("Server listening on {}", address);
}

void
GrpcServerManager::shutdown()
{
    if (_server) {
        _server->Shutdown();
        _server.reset();
    }
}

}  // namespace springtail
