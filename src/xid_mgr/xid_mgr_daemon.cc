#include <iostream>

#include <boost/program_options.hpp>
#include <common/common.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
void
handle_sigint(int signal)
{
    xid_mgr::XidMgrServer::get_instance()->stop();
}
}  // namespace

int
main(int argc, char *argv[])
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
    springtail_init("xid_mgr", pidfile);

    if (vm.count("xid") && vm.count("dbid")) {
        // note: since the defaults are set this always commits the starting_xid of 2 for db_id 1
        xid_mgr::XidMgrServer::get_instance()->commit_xid(db_id, starting_xid, false);
    }

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    // start the server
    xid_mgr::XidMgrServer::get_instance()->startup();

    // shutdown the server
    xid_mgr::XidMgrServer::shutdown();
    return 0;
}
