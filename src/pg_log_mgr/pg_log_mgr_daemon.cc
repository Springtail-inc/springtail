#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>

using namespace springtail;

int main(int argc, char *argv[])
{
    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");

    std::string vaccumer_namespace = "pg_log_mgr";

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    po::notify(vm);

    // initialize the springtail subsystems
    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "pg_log_mgr.pid";
    }

    springtail_store_arguments(ServiceId::VacuumerId,
        {
            {"vacuum_global_ns", std::any(vaccumer_namespace)}
        });
    springtail_init_daemon("pg_log_mgr", pidfile,
                           LOG_ALL ^ (LOG_PG_REPL | LOG_STORAGE | LOG_CACHE));
    pg_log_mgr::PgLogCoordinator::get_instance()->init();

    springtail_daemon_run();

    springtail_shutdown();

    return 0;
}
