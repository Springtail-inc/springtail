#include <boost/program_options.hpp>
#include <common/common.hh>
#include <common/coordinator.hh>
#include <csignal>
#include <garbage_collector/committer.hh>

namespace {
std::shared_ptr<springtail::gc::Committer> committer;

void
shutdown_handler(int signal)
{
    committer->shutdown();
}
}  // namespace

/**
 * Main program loop for the garbage collector daemon.
 */
int
main(int argc, char *argv[])
{
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
        pidfile = "gc.pid";
    }
    springtail::springtail_init("gc", pidfile);

    // the GC components
    // note: each performs any crash cleanup for their components prior to startup
    committer = std::make_shared<springtail::gc::Committer>(1);

    // signal handler for shutdown
    std::signal(SIGINT, shutdown_handler);

    // start the GC committer
    committer->run();

    return 0;
}
