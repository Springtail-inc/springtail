#include <filesystem>
#include <fstream>

#include <common/grpc_client.hh>

namespace springtail::grpc_client {

namespace {
std::string
read_file_contents(const std::string& path)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        throw Error("Failed to open file: " + path);
    }
    return std::string(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
}
}  // anonymous namespace

std::shared_ptr<grpc::Channel>
create_channel(std::string_view service,
               const std::string& server_hostname,
               const nlohmann::json& rpc_json)
{
    bool ssl = Json::get_or<bool>(rpc_json, "ssl", false);
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (ssl) {
        std::string cert_file_path;
        Json::get_to<std::string>(rpc_json, "server_cert", cert_file_path);
        if (cert_file_path.empty() || !std::filesystem::exists(cert_file_path)) {
            SPDLOG_ERROR("{}: Invalid configuration for certificate file {}", service,
                         cert_file_path);
            throw Error("Certificate file path is misconfigured");
        }

        std::string key_file_path;
        Json::get_to<std::string>(rpc_json, "server_key", key_file_path);
        if (key_file_path.empty() || !std::filesystem::exists(key_file_path)) {
            SPDLOG_ERROR("{}: Invalid configuration for key file {}", service, key_file_path);
            throw Error("Key file path is misconfigured");
        }

        std::string trusted_file_path;
        Json::get_to<std::string>(rpc_json, "server_trusted", trusted_file_path);
        if (trusted_file_path.empty() || !std::filesystem::exists(trusted_file_path)) {
            SPDLOG_ERROR("{}: Invalid configuration for trusted certificates file {}", service,
                         trusted_file_path);
            throw Error("Trusted certificates file path is misconfigured");
        }

        grpc::SslCredentialsOptions ssl_opts;
        ssl_opts.pem_root_certs = read_file_contents(trusted_file_path);
        ssl_opts.pem_private_key = read_file_contents(key_file_path);
        ssl_opts.pem_cert_chain = read_file_contents(cert_file_path);

        creds = grpc::SslCredentials(ssl_opts);
    } else {
        creds = grpc::InsecureChannelCredentials();
    }
    grpc::ChannelArguments args;
    int port = 0;
    Json::get_to<int>(rpc_json, "server_port", port);
    std::string server_addr = server_hostname + ":" + std::to_string(port);
    SPDLOG_DEBUG("{}: Creating channel to {} with SSL: {}", service, server_addr, ssl);
    return grpc::CreateCustomChannel(server_addr, creds, args);
}

}  // namespace springtail::grpc_client
