#include <pg_fdw/pg_ddl_mgr.hh>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>

using namespace springtail;

namespace {
    void
    handle_sigint(int signal)
    {
        pg_fdw::PgDDLMgr *ddl_mgr = pg_fdw::PgDDLMgr::get_instance();
        if (ddl_mgr != nullptr) {
            std::cout << "Shutting down DDL Mgr" << std::endl;
            ddl_mgr->shutdown();
        }
    }
}

int main(int argc, char *argv[])
{
    std::optional<std::string> socket_hostname = {};
    std::string socket_host_str;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("daemonize", "Start the server as a daemon")
        ("socket,s", boost::program_options::value<std::string>(&socket_host_str), "Unix domain socket path for Postgresql")
    ;

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

    if (!socket_host_str.empty()) {
        socket_hostname = socket_host_str;
    }

    // initialize the springtail subsystems
    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "pg_ddl_mgr.pid";
    }
    springtail::springtail_init("pg_ddl_mgr", pidfile, LOG_ALL);

    // get the DDL Mgr instance
    pg_fdw::PgDDLMgr *ddl_mgr = pg_fdw::PgDDLMgr::get_instance();

    // register the SIGINT handler; do this before starting the main thread
    std::signal(SIGINT, handle_sigint);

    // start the ddl main thread
    std::string fdw_id = Properties::get_fdw_id();

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

    ddl_mgr->startup(fdw_id, socket_hostname);

    // wait for shutdown; wait for main thread to join
    ddl_mgr->wait_shutdown();

    return 0;
}