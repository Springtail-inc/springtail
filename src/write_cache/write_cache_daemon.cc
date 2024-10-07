#include <iostream>

#include <boost/program_options.hpp>

#include <common/common.hh>
#include <write_cache/write_cache_server.hh>

using namespace springtail;

namespace {
    void
    handle_sigint(int signal)
    {
        WriteCacheServer::shutdown();
    }
}


int main(int argc, char *argv[]) {
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

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "write_cache.pid";
    }
    springtail_init("write_cache", pidfile);

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    // start the server
    WriteCacheServer::startup();

    return 0;
}
