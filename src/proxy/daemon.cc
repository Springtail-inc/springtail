#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <common/properties.hh>
#include <common/json.hh>
#include <proxy/server.hh>
#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/session.hh>
#include <proxy/exception.hh>

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
    nlohmann::json primary_config = Properties::get_primary_db_config();
    auto host = Json::get<std::string>(primary_config, "host");
    auto port = Json::get<uint16_t>(primary_config, "port");
    if (host.has_value() && port.has_value()) {
        server->set_primary(std::make_shared<DatabaseInstance>(Session::Type::PRIMARY, host.value(), port.value()));
    } else {
        SPDLOG_ERROR("Could not find the value for primary database either host or port");
        throw ProxyServerError();
    }

<<<<<<< HEAD
    std::vector<std::string> fwd_id_list = Properties::get_fdw_ids();
    for (const auto & fwd_id: fwd_id_list) {
        nlohmann::json fwd_config = Properties::get_fdw_config(fwd_id);
        auto host = Json::get<std::string>(fwd_config, "host");
        auto port = Json::get<uint16_t>(fwd_config, "port");
=======
    // TODO: rename fwd_id to fdw_id, naming mistake
    std::vector<std::string> fdw_id_list = Properties::get_fdw_ids();
    for (const auto & fdw_id: fdw_id_list) {
        nlohmann::json fdw_config = Properties::get_fdw_config(fdw_id);
        auto host = Json::get<std::string>(fdw_config, "host");
        auto port = Json::get<uint16_t>(fdw_config, "port");
>>>>>>> 450b2e3 (changed proxy server to get database info from properties)
        if (host.has_value() && port.has_value()) {
            // add replica
            server->add_replica(std::make_shared<DatabaseInstance>(Session::Type::REPLICA, host.value(), port.value()));
        } else {
<<<<<<< HEAD
            SPDLOG_ERROR("Could not find the value for replica database {} either host or port", fwd_id);
=======
            SPDLOG_ERROR("Could not find the value for replica database {} either host or port", fdw_id);
>>>>>>> 450b2e3 (changed proxy server to get database info from properties)
            throw ProxyServerError();
        }
    }

    // add replicated database
    std::map<uint64_t, std::string> db_list = Properties::get_databases();
    for (const auto& db_pair: db_list) {
        server->add_replicated_database(std::get<1>(db_pair));
    }

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

    setup(server);

    server->run();
}
