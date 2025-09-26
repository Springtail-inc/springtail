#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>

#include <pg_log_mgr/committer.hh>
#include <pg_log_mgr/pg_log_coordinator.hh>
#include <pg_log_mgr/sync_tracker.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include <storage/vacuumer.hh>

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
    bool daemonize = false;
    if (vm.count("daemonize")) {
        daemonize = true;
    }

    springtail_store_arguments(ServiceId::VacuumerId,
        {
            {"vacuum_global_ns", std::any(vaccumer_namespace)}
        });
    springtail_init_daemon(argv[0], daemonize,
                           LOG_ALL ^ (LOG_PG_REPL | LOG_STORAGE | LOG_CACHE));

    pg_log_mgr::PgLogCoordinator::get_instance()->init();

    springtail_daemon_run();

    springtail_shutdown();

    return 0;
}
