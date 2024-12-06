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
        pg_log_mgr::PgLogCoordinator *log_co = pg_log_mgr::PgLogCoordinator::get_instance();
        if (log_co != nullptr) {
            log_co->notify_shutdown();
        }
    }
}

int main(int argc, char *argv[])
{
    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("daemonize", "Start the server as a daemon")
    ;

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

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

    return 0;
}
