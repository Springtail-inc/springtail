#include <common/common.hh>
#include <common/logging.hh>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/program_options.hpp>
#include <iostream>

using namespace springtail;

namespace {
    void
    handle_sigint(int signal)
    {
    }
}


int main(int argc, char *argv[])
{
    std::string shmname;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");
    desc.add_options()("shmname,s", po::value<std::string>(&shmname)->required(), "The name of the shared memory region");

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
        pidfile = "pg_shm_cache.pid";
    }
    springtail::springtail_init("pg_shm_cache", pidfile, LOG_ALL);

    // register the SIGINT handler; do this before starting the main thread
    std::signal(SIGINT, handle_sigint);

}
