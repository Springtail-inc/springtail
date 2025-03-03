#include <unistd.h>

#include <iostream>

#include <boost/program_options.hpp>
#include <common/init.hh>
#include <sys_tbl_mgr/server.hh>

using namespace springtail;

namespace {
volatile std::sig_atomic_t shutdown_requested = 0;

void
handle_sigint(int signal)
{
    shutdown_requested = 1;
}
}  // namespace

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

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "sys_tbl_mgr.pid";
    }

    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<TermSignalRunner>(handle_sigint));
    runners->emplace_back(std::make_unique<sys_tbl_mgr::SysTblMgrRunner>());

    springtail_init(runners, false,"sys_tbl_mgr", pidfile);

    // Block until SIGINT is received. If any other signal wakes the process,
    // pause() will return and the loop will continue until shutdown_requested is set.
    while (!shutdown_requested) {
        // Technically there is a race here, where if a SIGINT is received before pause() is called,
        // we will ignore the SIGINT.
        pause();
    }

    springtail_shutdown();
    return 0;
}
