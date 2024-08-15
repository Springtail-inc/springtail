#include <iostream>
#include <boost/program_options.hpp>

#include <common/common.hh>
#include <common/properties.hh>

#include <xid_mgr/xid_mgr_client.hh>

using namespace springtail;

int
main(int argc, char **argv)
{
    bool get = false;
    bool set = false;
    uint64_t xid = 0;

    springtail_init();

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("get,g", "Get latest committed xid")
        ("set,s", "Set latest committed xid")
        ("xid,x", boost::program_options::value<uint64_t>(), "xid to set");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("get")) {
        get = true;
    }

    if (vm.count("set")) {
        set = true;
    }

    if (vm.count("xid")) {
        xid = vm["xid"].as<uint64_t>();
    }

    if (set && xid == 0) {
        std::cerr << "XID must be specified with set" << std::endl;
        return -1;
    }

    if (!get && !set) {
        std::cerr << "Get or Set must be specified" << std::endl;
        return -1;
    }

    auto xid_mgr = XidMgrClient::get_instance();

    if (get) {
        // get the xid
        uint64_t committed_xid;
        committed_xid = xid_mgr->get_committed_xid(0);
        std::cout << fmt::format("{}\n", committed_xid);
    }

    if (set) {
        // set the xid
        xid_mgr->commit_xid(xid, false);
    }

    return 0;
}