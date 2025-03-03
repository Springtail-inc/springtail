#include <iostream>

#include <boost/program_options.hpp>
#include <unistd.h>

#include <common/init.hh>
#include <xid_mgr/xid_mgr_server.hh>

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
main(int argc, char* argv[])
{
    uint64_t starting_xid;
    uint64_t db_id = 1;

    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("xid,x", po::value<uint64_t>(&starting_xid)->default_value(2),
                       "The starting XID.");
    desc.add_options()("dbid,d", po::value<uint64_t>(&db_id)->default_value(1), "DB ID.");
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
        pidfile = "xid_mgr.pid";
    }

    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<TermSignalRunner>(handle_sigint));
    runners->emplace_back(std::make_unique<xid_mgr::XidMgrRunner>(vm.count("xid") && vm.count("dbid"), db_id, starting_xid));

    springtail_init(runners, false, "xid_mgr", pidfile);

    // Block until SIGINT is received. If any other signal wakes the process,
    // pause() will return and the loop will continue until shutdown_requested is set.
    while (!shutdown_requested) {
        // Technically there is a race here if SIGINT is received before pause() is called.
        pause();
    }

    springtail_shutdown();
    return 0;
}
