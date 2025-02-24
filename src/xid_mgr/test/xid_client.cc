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
    bool schema_change = false;
    uint64_t xid = 0;
    uint64_t db_id = 1;

    springtail_init();

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("get,g", "Get latest committed xid");
    desc.add_options()("set,s", "Set latest committed xid");
    desc.add_options()("dbid,d", po::value<uint64_t>(&db_id)->default_value(1), "DB ID.");
    desc.add_options()("xid,x", po::value<uint64_t>(&xid)->default_value(0), "Xid to set");
    desc.add_options()("change,c", po::value<bool>(&schema_change)->default_value(false),
        "Has schema change (for testing set command so that the new xid is recorded in history)");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

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
        committed_xid = xid_mgr->get_committed_xid(db_id, 0);
        std::cout << fmt::format("{}\n", committed_xid);
    }

    if (set) {
        // set the xid
        xid_mgr->commit_xid(db_id, xid, schema_change);
    }

    XidMgrClient::shutdown();
    return 0;
}