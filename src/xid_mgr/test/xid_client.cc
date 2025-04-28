#include <iostream>
#include <boost/program_options.hpp>

#include <common/init.hh>

#include <xid_mgr/xid_mgr_client.hh>

using namespace springtail;

int
main(int argc, char **argv)
{
    bool get = false;
    uint64_t db_id = 1;
    uint64_t schema_xid = 0;

    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<GrpcClientRunner<XidMgrClient>>());
    springtail_init(runners);

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("get,g", "Get latest committed xid");
    desc.add_options()("dbid,d", po::value<uint64_t>(&db_id)->default_value(1), "DB ID.");
    desc.add_options()("schema_xid,s", po::value<uint64_t>(&schema_xid)->default_value(0), "Schema xid to set");

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

    auto xid_mgr = XidMgrClient::get_instance();

    if (get) {
        // get the xid
        uint64_t committed_xid;
        committed_xid = xid_mgr->get_committed_xid(db_id, schema_xid);
        std::cout << fmt::format("{}\n", committed_xid);
    }

    springtail_shutdown();
    return 0;
}