#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common_init.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <proxy/server.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

static void
handle_sigint(int signal)
{
    ProxyServer *server = ProxyServer::get_instance();
    server->notify_shutdown();
}

class ProxyRunner : public ServiceRunner {
    public:
        explicit ProxyRunner(bool force_shadow, bool force_primary) :
            ServiceRunner("ProxyServer"),
            _force_shadow(force_shadow),
            _force_primary(force_primary) {}

        bool start() override {
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
            std::filesystem::path log = Json::get_or<std::filesystem::path>(json, "shadow_log_path", "");
            if (!log.empty()) {
                std::fstream log_file;
                try {
                    // create and truncate the file
                    log_file.open(log, std::ios::out | std::ios::trunc | std::ios::binary);
                    log_file.close();
                } catch (const std::ios_base::failure &e) {
                    throw Error(fmt::format("Error creating shadow log file {}: {}", log, e.what()));
                }

                SPDLOG_INFO("Logging initialized to: {}", log.string());
                logger = std::make_shared<Logger>(log, 1024*1024*100, 5);
            } else {
                SPDLOG_INFO("Shadow logging disabled: log={}", log.string());
            }

            ProxyServer::MODE server_mode = ProxyServer::MODE::NORMAL;
            std::string mode = Json::get_or<std::string>(json, "mode", "normal");

            // overrides from command line (for debugging)
            if (_force_primary) {
                mode = "primary";
            } else if (_force_shadow) {
                mode = "shadow";
            }

            if (mode == "shadow") {
                server_mode = ProxyServer::MODE::SHADOW;
                CHECK_NE(logger, nullptr);
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
            return true;
        }

        void stop() override {
            ProxyServer::shutdown();
        }
    private:
        bool _force_shadow{false};
        bool _force_primary{false};
    };

int main(int argc, char* argv[])
{
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");
    desc.add_options()("primary", "Force mode to primary, overriding redis");
    desc.add_options()("shadow", "Force mode to shadow, overriding redis");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    bool force_primary = false;
    bool force_shadow = false;

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("primary")) {
        force_primary = true;
    }

    if (vm.count("shadow")) {
        force_shadow = true;
    }

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "proxy.pid";
    }

    std::vector<ServiceRunner *> runners = {
        new TermSignalRunner(handle_sigint),
        new ProxyRunner(force_shadow, force_primary)
    };

    springtail_init(runners, !pidfile.has_value(), "proxy", pidfile, LOG_PROXY);

    ProxyServer::get_instance()->run();

    springtail_shutdown();
}
