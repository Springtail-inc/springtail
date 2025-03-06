#include <unistd.h>

#include <iostream>

#include <boost/program_options.hpp>
#include <common/init.hh>
#include <sys_tbl_mgr/schema_mgr.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <sys_tbl_mgr/server.hh>

using namespace springtail;

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
    runners->emplace_back(std::make_unique<SchemaMgrRunner>());
    runners->emplace_back(std::make_unique<TableMgrRunner>());
    runners->emplace_back(std::make_unique<sys_tbl_mgr::SysTblMgrRunner>());

    springtail_init_daemon(runners, "sys_tbl_mgr", pidfile);

    springtail_daemon_run();

    springtail_shutdown();
    return 0;
}
