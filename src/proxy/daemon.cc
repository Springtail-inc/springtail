#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <proxy/server.hh>
#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/session.hh>

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

static void setup(ProxyServerPtr server)
{
    // add primary
    server->set_primary(std::make_shared<DatabaseInstance>(Session::Type::PRIMARY, "localhost", 5432));

    // add replica
    server->add_replica(std::make_shared<DatabaseInstance>(Session::Type::REPLICA, "localhost", 5432));

    // add replicated database
    server->add_replicated_database("test");

    // add test user for test db with trust
    server->add_user("test");

    // add test user for test db with md5
    std::string username = "test_md5";
    std::string passwd = "test";
    char md5[36]; // md5sum('pwd'+'user') = md5+digest
    pg_md5_encrypt(passwd.c_str(), username.c_str(), strlen(username.c_str()), md5);
    md5[35] = '\0'; // null terminate
    uint32_t salt;
    get_random_bytes((uint8_t*)&salt, 4);
    SPDLOG_DEBUG_MODULE(LOG_PROXY, "Adding MD5 user: {}, md5: {}, salt: {}", username, md5, salt);
    server->add_user("test_md5", md5, salt);

    // add user for test db with scram
    server->add_user("test_scram", "SCRAM-SHA-256$4096:tb3ZKGGBQOq0eocVNWBbrw==$JrwngrAnMVC0BDQqxK6bREhwqi+ngU6ShRUmswgASLI=:8yAuc+PJJZ1L62803po41jTWmZp5JGwquWQZm6SCvsg=");
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

    springtail_init("proxy");

    // daemonize the process
    if (vm.count("daemonize")) {
        common::daemonize("/var/springtail/proxy.pid");
    }

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);


    std::cout << "Logging initialized to: " << log << std::endl;
    LoggerPtr logger = std::make_shared<Logger>(log, 1024*1024*100, 5);

    server = std::make_shared<ProxyServer>(port, num_threads, certificate, key, shadow_mode, enable_ssl, logger);

    setup(server);

    server->run();
}
