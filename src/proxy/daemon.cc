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
    std::filesystem::path certificate;
    std::filesystem::path key;
    std::filesystem::path log;
    int port;
    int num_threads;
    bool enable_ssl = false;
    ProxyServer::MODE server_mode = ProxyServer::MODE::NORMAL;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("daemonize", "Start the server as a daemon");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "proxy.pid";
    }
    springtail_init("proxy", pidfile);

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
    Json::get_to<int>(json, "threads", num_threads, 4);
    Json::get_to<int>(json, "port", port, 8888);

    // setup ssl config
    Json::get_to<bool>(json, "enable_ssl", enable_ssl, false);
    Json::get_to<std::filesystem::path>(json, "cert", certificate);
    Json::get_to<std::filesystem::path>(json, "key", key);
    if (enable_ssl &&
        (!std::filesystem::exists(certificate) || !std::filesystem::exists(key))) {
        throw Error("Certificate/key file does not exist and ssl is enabled");
    }

    if (!enable_ssl) {
        SPDLOG_INFO("SSL Disabled");
    }

    // setup the mode
    std::string mode;
    LoggerPtr logger = nullptr;
    Json::get_to<std::string>(json, "mode", mode, "normal");
    if (mode == "shadow") {
        Json::get_to<std::filesystem::path>(json, "log", log);
        if (!std::filesystem::exists(log)) {
            throw Error("Log file does not exist and shadow mode is enabled");
        }

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

    server = std::make_shared<ProxyServer>(port, num_threads, certificate, key, server_mode, enable_ssl, logger);

    server->run();
    server->cleanup();
}
