#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <proxy/server.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

static ProxyServerPtr server = nullptr;

static void
handle_sigint(int signal)
{
    if (server != nullptr) {
        server->shutdown();
    }
}

int main(int argc, char* argv[])
{
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "proxy.pid";
    }
    springtail_init("proxy", pidfile, LOG_PROXY);

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
    int num_threads = Json::get_or<int>(json, "threads", 4);
    int port = Json::get_or<int>(json, "port", 8888);

    int log_level = Json::get_or<int>(json, "log_level", 1);

    // setup ssl config
    bool enable_ssl = Json::get_or<bool>(json, "enable_ssl", false);
    std::filesystem::path certificate = Json::get_or<std::filesystem::path>(json, "cert", "");
    std::filesystem::path key = Json::get_or<std::filesystem::path>(json, "key", "");
    if (enable_ssl &&
        (!std::filesystem::exists(certificate) || !std::filesystem::exists(key))) {
        throw Error("Certificate/key file does not exist and ssl is enabled");
    }

    if (!enable_ssl) {
        SPDLOG_INFO("SSL Disabled");
    }

    // setup the mode
    LoggerPtr logger = nullptr;
    ProxyServer::MODE server_mode = ProxyServer::MODE::NORMAL;
    std::string mode = Json::get_or<std::string>(json, "mode", "normal");
    if (mode == "shadow") {
        std::filesystem::path log = Json::get_or<std::filesystem::path>(json, "shadow_log_path", "");
        if (log.empty()) {
            throw Error("shadow_log_path is not defined");
        }
        std::fstream log_file;
        log_file.open(log, std::ios::out | std::ios::trunc | std::ios::binary);
        log_file.close();

        SPDLOG_INFO("Logging initialized to: {}", log.string());
        logger = std::make_shared<Logger>(log, 1024*1024*100, 5);

        server_mode = ProxyServer::MODE::SHADOW;
    } else if (mode == "normal") {
        server_mode = ProxyServer::MODE::NORMAL;
    } else if (mode == "primary") {
        server_mode = ProxyServer::MODE::PRIMARY;
    } else {
        throw Error("Invalid mode specified");
    }

    ProxyServer *server = ProxyServer::get_instance();
    server->init(port, num_threads, certificate, key, server_mode, enable_ssl, logger);
    server->set_log_level(log_level);

    server->run();
}
