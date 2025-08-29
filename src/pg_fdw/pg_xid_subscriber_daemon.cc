#include <pg_fdw/pg_xid_subscriber_mgr.hh>

#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>

#include <xid_mgr/xid_mgr_client.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

int main(int argc, char *argv[])
{
    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start it as a daemon.");

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
        pidfile = "pg_xid_subscriber.pid";
    }

    springtail::springtail_init_daemon("pg_xid_subscriber", pidfile);
    PgXidSubscriberMgr::start();

    springtail_daemon_run();

    springtail::springtail_shutdown();

    return 0;
}
