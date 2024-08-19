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
            log_co->shutdown();
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

    // daemonize the process
    if (vm.count("daemonize")) {
        common::daemonize("/var/springtail/pg_log_mgr.pid");
    }

    springtail_init();

    pg_log_mgr::PgLogCoordinator *log_co = pg_log_mgr::PgLogCoordinator::get_instance();

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    // get the set of db ids for this instance
    std::map<uint64_t, std::string> db_ids = Properties::get_databases();
    for (auto &db: db_ids) {
        uint64_t db_id = db.first;
        log_co->add_database(db_id);
    }

    log_co->wait_shutdown();

    return 0;
}
