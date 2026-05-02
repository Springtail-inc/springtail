#pragma once

#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <nlohmann/json.hpp>

namespace springtail {

class GrpcServerManager {
public:
    GrpcServerManager() = default;
    void init(const nlohmann::json& rpc_json);

    // Register any number of services.
    void addService(grpc::Service* service);

    void startup();
    void shutdown();

private:
    std::unique_ptr<grpc::ServerBuilder> _create_builder(const std::string& address);
    static std::string read_file_contents(const std::string& path);

    int _worker_thread_count = 0;
    int _port = 0;
    bool _ssl = false;
    std::string _server_key;
    std::string _server_cert;
    std::string _root_cert;
    std::unique_ptr<grpc::Server> _server;
    std::vector<grpc::Service*> _services;
};

}  // namespace springtail