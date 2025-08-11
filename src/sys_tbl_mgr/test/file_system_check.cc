#include <boost/program_options.hpp>

#include <common/init.hh>

#include <test/file_system_check.hh>

using namespace springtail;
namespace po = boost::program_options;

int
main(int argc, char *argv[])
{
    uint64_t max_xid = 0;
    bool all_xids = false;

    po::options_description desc("Options");
    desc.add_options()("max_xid,mx", po::value<uint64_t>(&max_xid)->default_value(constant::LATEST_XID), "Maximum xid, default is the latest");
    desc.add_options()("all_xids,ax", po::value<bool>(&all_xids)->default_value(false), "Flag to check all xids, default is the false");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // no logging
    springtail_init(false, std::nullopt, LOG_NONE);

    auto fs_check = std::make_shared<test::FSCheck>(max_xid);
    fs_check->check_dbs(all_xids);
    fs_check.reset();

    springtail_shutdown();
}