#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <sys_tbl_mgr/client.hh>
#include <pg_log_mgr/committer.hh>

using namespace springtail;

namespace {

    void
    handle_sigint(int signal)
    {
        pg_log_mgr::PgLogCoordinator *log_co = pg_log_mgr::PgLogCoordinator::get_instance();
        if (log_co != nullptr) {
            log_co->notify_shutdown();
        }
        sys_tbl_mgr::Client::shutdown();
        XidMgrClient::shutdown();
    }
}

int main(int argc, char *argv[])
{
    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");

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
    springtail::springtail_init("pg_log_mgr", pidfile);

    pg_log_mgr::PgLogCoordinator *log_co = pg_log_mgr::PgLogCoordinator::get_instance();

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    log_co->init();

    log_co->wait_shutdown();

    pg_log_mgr::PgLogCoordinator::shutdown();
    sys_tbl_mgr::Client::shutdown();
    XidMgrClient::shutdown();

    return 0;
}
