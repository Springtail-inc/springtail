#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>

// springtail includes
#include <common/init.hh>

#include <proxy/database.hh>
#include <proxy/server.hh>
#include <proxy/user_mgr.hh>

using namespace springtail;
using namespace springtail::pg_proxy;

int main(int argc, char* argv[])
{
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");
    desc.add_options()("primary", "Force mode to primary, overriding redis");
    desc.add_options()("shadow", "Force mode to shadow, overriding redis");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    bool force_primary = false;
    bool force_shadow = false;

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    if (vm.count("primary")) {
        force_primary = true;
    }

    if (vm.count("shadow")) {
        force_shadow = true;
    }

    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "proxy.pid";
    }

    std::optional<std::vector<std::unique_ptr<ServiceRunner>>> runners;
    runners.emplace();
    runners->emplace_back(std::make_unique<DatabaseMgrRunner>());
    runners->emplace_back(std::make_unique<UserMgrRunner>(UserMgr::USER_MGR_SLEEP_INTERVAL_SECS));
    runners->emplace_back(std::make_unique<ProxyRunner>(force_shadow, force_primary));

    springtail_init_daemon(runners, "proxy", pidfile, LOG_PROXY);

    springtail_daemon_run();

    springtail_shutdown();
}
