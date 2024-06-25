#include <boost/program_options.hpp>

#include <common/common.hh>
#include <xid_mgr/xid_mgr_server.hh>

using namespace springtail;

namespace {
    void
    handle_sigint(int signal)
    {
        XidMgrServer::shutdown();
    }
}

int main(int argc, char *argv[]) {
    springtail_init();

    uint64_t starting_xid;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("xid,x", boost::program_options::value<uint64_t>(&starting_xid)->default_value(2), "The starting XID.")
        ("daemonize", "Start the server as a daemon");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

    if (vm.count("xid")) {
        XidMgrServer::get_instance()->commit_xid(starting_xid);
    }

    // daemonize the process
    if (vm.count("daemonize")) {
        common::daemonize("/var/springtail/xid_mgr.pid");
    }

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    // start the server
    XidMgrServer::startup();

    return 0;
}
