#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
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
    bool shadow_mode = false;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("ssl,s", boost::program_options::value<bool>(&enable_ssl)->default_value(false), "Enable SSL")
        ("shadow,S", boost::program_options::value<bool>(&shadow_mode)->default_value(false), "Shadow mode")
        ("port,p", boost::program_options::value<int>(&port)->default_value(8888), "Proxy port number")
        ("threads,n", boost::program_options::value<int>(&num_threads)->default_value(4), "Number of threads")
        ("cert,c", boost::program_options::value<std::filesystem::path>(&certificate)->default_value(std::filesystem::path("cert.pem")), "Certificate file")
        ("key,k", boost::program_options::value<std::filesystem::path>(&key)->default_value(std::filesystem::path("key.pem")), "Key file")
        ("log,l", boost::program_options::value<std::filesystem::path>(&log)->default_value(std::filesystem::path("/tmp/springtail/proxy.log")), "Log file")
        ("daemonize", "Start the server as a daemon");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (!enable_ssl) {
        SPDLOG_INFO("SSL Disabled");
    }

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "proxy.pid";
    }
    springtail_init("proxy", pidfile);

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    std::cout << "Logging initialized to: " << log << std::endl;
    LoggerPtr logger = std::make_shared<Logger>(log, 1024*1024*100, 5);

    server = std::make_shared<ProxyServer>(port, num_threads, certificate, key, shadow_mode, enable_ssl, logger);

    server->run();
}
