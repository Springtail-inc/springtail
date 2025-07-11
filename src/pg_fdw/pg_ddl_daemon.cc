#include <pg_fdw/pg_ddl_mgr.hh>

#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

int main(int argc, char *argv[])
{
    std::optional<std::string> socket_hostname = {};
    std::string socket_host_str;

    std::string username;
    std::string password;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");
    desc.add_options()("username,u", po::value<std::string>(&username)->required(), "DDL Postgres username");
    desc.add_options()("password,p", po::value<std::string>(&password)->required(), "DDL Postgres password");
    desc.add_options()("socket,s", po::value<std::string>(&socket_host_str), "Unix domain socket path for Postgresql");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    po::notify(vm);

    if (!socket_host_str.empty()) {
        socket_hostname = socket_host_str;
    }

    // initialize the springtail subsystems
    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "pg_ddl_mgr.pid";
    }

    // check if the socket path is valid
    if (!socket_host_str.empty()) {
        // check that the socket path is valid and readable
        socket_hostname = socket_host_str;
        std::filesystem::path socket_path(*socket_hostname);
        if (!std::filesystem::exists(socket_path) || !std::filesystem::is_directory(socket_path)) {
            std::cerr << "Error: socket path does not exist: " << *socket_hostname << std::endl;
            socket_hostname = std::nullopt;
        } else {
            try {
                // Try to iterate over the directory
                for (const auto& entry : std::filesystem::directory_iterator(socket_path)) {
                    (void)entry;  // We don't actually need the entries
                }
            } catch (const std::filesystem::filesystem_error& e) {
                std::cerr << "Error: socket path is not readable: " << *socket_hostname << std::endl;
                socket_hostname = std::nullopt;
            }
        }
    }

    springtail_store_arguments(ServiceId::PgDDLMgrId,
        {
            {"username", std::any(username)},
            {"password", std::any(password)},
            {"hostname", std::any(socket_hostname)}
        });

    springtail_init_daemon("pg_ddl_mgr", pidfile, LOG_ALL ^ (LOG_STORAGE | LOG_CACHE));
    PgDDLMgr::start();
    springtail_daemon_run();

    springtail::springtail_shutdown();

    return 0;
}
