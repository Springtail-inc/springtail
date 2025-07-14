#include <boost/program_options.hpp>

#include <common/init.hh>

#include <pg_fdw/pg_xid_collector.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

void
block_term_signals()
{
    sigset_t mask;
    sigemptyset(&mask);

    std::vector<int> signals = {SIGINT, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2};
    for (int sig : signals) {
        sigaddset(&mask, sig);
    }

    // Block signals in the current thread (usually main) — must be done *before* any threads are created
    int rc = pthread_sigmask(SIG_BLOCK, &mask, nullptr);
    PCHECK(rc == 0) << "Failed to block signals";
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
        pidfile = "pg_xid_collector.pid";
    }

    block_term_signals();
    springtail_init_daemon("pg_xid_collector", pidfile, LOG_ALL);

    PgXidCollector::get_instance()->run();
    PgXidCollector::shutdown();

    springtail::springtail_shutdown();

    return 0;
}