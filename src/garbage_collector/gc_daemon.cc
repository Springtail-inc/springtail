#include <csignal>
#include <boost/program_options.hpp>

#include <common/common.hh>
#include <garbage_collector/log_parser.hh>
#include <garbage_collector/committer.hh>

namespace {
    std::shared_ptr<springtail::gc::LogParser> log_parser;
    std::shared_ptr<springtail::gc::Committer> committer;

    void shutdown_handler(int signal) {
        log_parser->shutdown();
        committer->shutdown();
    }
}

/**
 * Main program loop for the garbage collector daemon.
 */
int
main(int argc,
     char *argv[])
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("daemonize", "Start the server as a daemon");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

    // initialize the springtail subsystems
    springtail::springtail_init();

    // the GC components
    log_parser = std::make_shared<springtail::gc::LogParser>(1, 1);
    committer = std::make_shared<springtail::gc::Committer>(1);

    // daemonize the process
    if (vm.count("daemonize")) {
        springtail::common::daemonize("/tmp/gc.pid");
    }

    // signal handler for shutdown
    std::signal(SIGINT, shutdown_handler);

    // start the GC log parser
    log_parser->run();

    // start the GC committer
    committer->run();

    return 0;
}
