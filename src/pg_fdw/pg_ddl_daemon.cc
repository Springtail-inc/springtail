#include <pg_fdw/pg_ddl_mgr.hh>

#include <boost/program_options.hpp>

// springtail includes
#include <common/common_init.hh>
#include <common/properties.hh>

#include <pg_log_mgr/pg_log_coordinator.hh>

using namespace springtail;

namespace {
    void
    handle_sigint(int signal)
    {
        pg_fdw::PgDDLMgr *ddl_mgr = pg_fdw::PgDDLMgr::get_instance();
        if (ddl_mgr != nullptr) {
            ddl_mgr->notify_shutdown();
        }
    }

    class PgDDLMgrRunner : public ServiceRunner {
    public:
        explicit PgDDLMgrRunner(const std::string &username,
                                const std::string &password,
                                const std::optional<std::string> &hostname) :
            ServiceRunner("PgDDLMgr"),
            _username(username),
            _password(password),
            _hostname(hostname) {}

        bool start() override
        {
            // start the ddl main thread
            std::string fdw_id = Properties::get_fdw_id();

            SPDLOG_DEBUG("Starting DDL Mgr with fdw_id: {}, username: {}, password: {}, socket_hostname: {}",
                        fdw_id, _username, _password, _hostname.value_or(""));
            pg_fdw::PgDDLMgr::get_instance()->init(_fdw_id, _username, _password, _hostname);
            return true;
        }

        void stop() override
        {
            pg_fdw::PgDDLMgr::shutdown();
        }

    private:
        std::string _fdw_id;                       ///< FDW ID
        std::string _username;                     ///< username
        std::string _password;                     ///< password
        std::optional<std::string> _hostname;      ///< hostname
    };

}

int main(int argc, char *argv[])
{
    std::optional<std::string> socket_hostname = {};
    std::string socket_host_str;

    std::string username;
    std::string password;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("daemonize", "Start the server as a daemon");
    desc.add_options()("username,u", po::value<std::string>(&username)->required(), "DDL Postgres username");
    desc.add_options()("password,p", po::value<std::string>(&password)->required(), "DDL Postgres password");
    desc.add_options()("socket,s", po::value<std::string>(&socket_host_str), "Unix domain socket path for Postgresql");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    po::notify(vm);

    if (!socket_host_str.empty()) {
        socket_hostname = socket_host_str;
    }

    // initialize the springtail subsystems
    std::optional<std::string> pidfile;
    if (vm.count("daemonize")) {
        pidfile = "pg_ddl_mgr.pid";
    }

    // check if the socket path is valid
    if (!socket_host_str.empty()) {
        // check that the socket path is valid and readable
        socket_hostname = socket_host_str;
        std::filesystem::path socket_path(*socket_hostname);
        if (!std::filesystem::exists(socket_path) || !std::filesystem::is_directory(socket_path)) {
            std::cerr << "Error: socket path does not exist: " << *socket_hostname << std::endl;
            socket_hostname = std::nullopt;
        } else {
            try {
                // Try to iterate over the directory
                for (const auto& entry : std::filesystem::directory_iterator(socket_path)) {
                    (void)entry;  // We don't actually need the entries
                }
            } catch (const std::filesystem::filesystem_error& e) {
                std::cerr << "Error: socket path is not readable: " << *socket_hostname << std::endl;
                socket_hostname = std::nullopt;
            }
        }
    }

    std::vector<ServiceRunner *> runners = {
        new TermSignalRunner(handle_sigint),
        new PgDDLMgrRunner(username, password, socket_hostname)
    };

    springtail::springtail_init(runners, "pg_ddl_mgr", pidfile, LOG_ALL);

    pg_fdw::PgDDLMgr::get_instance()->run();

    // wait for shutdown; wait for main thread to join
    pg_fdw::PgDDLMgr::shutdown();

    return 0;
}