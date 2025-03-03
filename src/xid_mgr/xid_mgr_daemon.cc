#include <iostream>

#include <boost/program_options.hpp>
#include <common/common.hh>
#include <unistd.h>
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
    desc.add_options()("xid,x", po::value<uint64_t>(&starting_xid), "The starting XID.");
    desc.add_options()("dbid,d", po::value<uint64_t>(&db_id), "DB ID.");
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
    springtail_init("xid_mgr", pidfile);

    auto* server = xid_mgr::XidMgrServer::get_instance();

    if (vm.count("xid") && vm.count("dbid")) {
        // note: since the defaults are set this always commits the starting_xid of 2 for db_id 1
        server->commit_xid(db_id, starting_xid, false);
    }

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    // start the server
    server->startup();

    // Block until SIGINT is received. If any other signal wakes the process,
    // pause() will return and the loop will continue until shutdown_requested is set.
    while (!shutdown_requested) {
        // Technically there is a race here if SIGINT is received before pause() is called.
        pause();
    }

    server->shutdown();
    return 0;
}
