#include <fstream>
#include <mutex>

#include <common/init.hh>

using namespace springtail;

namespace {

    static std::atomic_flag shutdown_flag = false;

    void
    handle_sigint(int signal)
    {
        shutdown_flag.test_and_set();
        shutdown_flag.notify_one();
    }
}  // namespace

namespace springtail {

bool DaemonRunner::start()
{
    std::string pid_path = Properties::get_instance()->get_pid_path();

    std::filesystem::path pid_filename(pid_path);
    pid_filename /= _daemon_pid;

    std::cout << "Daemonizing process, writing pid to: " << pid_filename << std::endl;

    int pid = fork();
    if (pid < 0) {
        throw Error(fmt::format("Failed to fork: {}", errno));
    }

    if (pid == 0) {
        // close the terminal inputs / outputs
        std::fclose(stdin);
        std::fclose(stdout);
        std::fclose(stderr);

        // make this process the session group leader
        int sid = setsid();
        if (sid < 0) {
            throw Error(fmt::format("Error calling setsid(): {}", errno));
        }

        // ignore hang-up
        std::signal(SIGHUP, SIG_IGN);
    } else {
        // ensure the pid directory exists
        std::filesystem::path pid_dir(pid_filename);
        pid_dir.remove_filename();
        std::filesystem::create_directories(pid_dir);

        // record the pid of the child into the pid file
        std::ofstream pid_file(pid_filename);
        pid_file << pid << std::endl;

        // exit cleanly
        std::exit(0);
    }
    return true;
}

class ServiceRegister : public Singleton<ServiceRegister>
{
    friend class Singleton<ServiceRegister>;

public:
    bool start(std::vector<std::unique_ptr<ServiceRunner>> &service_list)
    {
        std::move(service_list.begin(), service_list.end(), std::back_inserter(_service_list));
        auto reverse_iter = _service_list.rend();
        for (auto iter = _service_list.begin(); iter != _service_list.end(); iter++) {
            LOG_INFO("Starting service {}", (*iter)->get_name());
            if (!(*iter)->start()) {
                LOG_INFO("Service {} failed to start", (*iter)->get_name());
                // set reverse iterator
                reverse_iter = std::make_reverse_iterator(iter);
                break;
            }
        }
        // check reverse iterator
        if (reverse_iter != _service_list.rend()) {
            while (reverse_iter != _service_list.rend()) {
                LOG_INFO("Stopping service {}", (*reverse_iter)->get_name());
                (*reverse_iter)->stop();
                ++reverse_iter;
            }
            return false;
        }
        return true;
    }

private:
    ServiceRegister() : Singleton<ServiceRegister>(ServiceId::ServiceRegisterId) {}
    virtual ~ServiceRegister() override = default;

    std::vector<std::unique_ptr<ServiceRunner>> _service_list;

    void _internal_shutdown() override
    {
        for (auto reverse_iter = _service_list.rbegin(); reverse_iter != _service_list.rend();
                reverse_iter++) {
            LOG_INFO("Stoping service {}", (*reverse_iter)->get_name());
            (*reverse_iter)->stop();
        }
        _service_list.clear();
    }
};

void
springtail_init(const bool load_redis,
                const std::optional<std::string> &log_filename,
                const std::optional<uint32_t> &logging_mask)
{
    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(load_redis));
    service_runners.emplace_back(std::make_unique<LoggingRunner>(log_filename, std::nullopt, logging_mask));
    service_runners.emplace_back(std::make_unique<OpenTelemetryRunner>(log_filename));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());

    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void
springtail_init_daemon(const std::optional<std::string> &log_filename,
                       const std::optional<std::string> &daemon_pid,
                       const std::optional<uint32_t> &logging_mask)
{
    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(false));
    if (daemon_pid.has_value()) {
        service_runners.emplace_back(std::make_unique<DaemonRunner>(daemon_pid.value()));
    }
    service_runners.emplace_back(std::make_unique<LoggingRunner>(log_filename, daemon_pid, logging_mask));
    service_runners.emplace_back(std::make_unique<OpenTelemetryRunner>(log_filename));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());
    service_runners.emplace_back(std::make_unique<CoordinatorRunner>());

    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void
springtail_init_test(const std::optional<uint32_t> &logging_mask)
{
    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(true));
    service_runners.emplace_back(std::make_unique<LoggingRunner>(std::nullopt, std::nullopt, logging_mask));
    service_runners.emplace_back(std::make_unique<OpenTelemetryRunner>(std::nullopt));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());

    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void springtail_init_custom(std::vector<std::unique_ptr<ServiceRunner>> &runners)
{
    if (!ServiceRegister::get_instance()->start(runners)) {
        exit(1);
    }
}

void
springtail_daemon_run()
{
    std::vector<int> signals{SIGINT, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2};

    // set signal handlers
    for (int sig : signals) {
        std::signal(sig, handle_sigint);
    }

    // wait for shutdown signal
    while (!shutdown_flag.test()) {
        shutdown_flag.wait(false);
    }

    // restore signal handlers to default
    for (int sig : signals) {
        std::signal(sig, SIG_DFL);
    }
}

static const std::map<ServiceId, std::vector<ServiceId>> dependencies = {
    {ServiceId::ServiceRegisterId,     {}},
    {ServiceId::DatabaseMgrId,         {ServiceId::ServiceRegisterId}},
    {ServiceId::UserMgrId,             {ServiceId::DatabaseMgrId}},
    {ServiceId::ProxyServerId,         {ServiceId::ServiceRegisterId, ServiceId::UserMgrId}},
    {ServiceId::XidMgrServerId,        {ServiceId::ServiceRegisterId}},
    {ServiceId::XidMgrClientId,        {ServiceId::ServiceRegisterId, ServiceId::XidMgrServerId}},
    {ServiceId::SysTblMgrServerId,     {ServiceId::ServiceRegisterId, ServiceId::XidMgrClientId, ServiceId::TableMgrId}},
    {ServiceId::SysTblMgrClientId,     {ServiceId::ServiceRegisterId}},
    {ServiceId::WriteCacheServerId,    {ServiceId::ServiceRegisterId, ServiceId::SysTblMgrServerId}},
    {ServiceId::WriteCacheClientId,    {ServiceId::ServiceRegisterId, ServiceId::WriteCacheServerId}},
    {ServiceId::IOMgrId,               {ServiceId::ServiceRegisterId}},
    {ServiceId::VacuumerId,            {ServiceId::IOMgrId}},
    {ServiceId::SchemaMgrId,           {ServiceId::SysTblMgrClientId}},
    {ServiceId::TableMgrId,            {ServiceId::IOMgrId, ServiceId::SchemaMgrId, ServiceId::StorageCacheId}},
    {ServiceId::SyncTrackerId,         {ServiceId::ServiceRegisterId}},
    {ServiceId::PgFdwMgrId,            {ServiceId::ServiceRegisterId, ServiceId::XidMgrClientId, ServiceId::TableMgrId}},
    {ServiceId::PgXidSubscriberMgrId,  {ServiceId::ServiceRegisterId, ServiceId::XidMgrClientId, ServiceId::SysTblMgrClientId}},
    {ServiceId::PgDDLMgrId,            {ServiceId::ServiceRegisterId, ServiceId::XidMgrClientId, ServiceId::TableMgrId}},
    {ServiceId::PgLogCoordinatorId,    {ServiceId::ServiceRegisterId, ServiceId::XidMgrClientId, ServiceId::WriteCacheServerId, ServiceId::TableMgrId}},
    {ServiceId::StorageCacheId,        {ServiceId::IOMgrId}}
};

static const std::map<ServiceId, std::string> dependencies_names = {
    {ServiceId::ServiceRegisterId,     "ServiceRegister"},
    {ServiceId::DatabaseMgrId,         "DatabaseMgr"},
    {ServiceId::UserMgrId,             "UserMgr"},
    {ServiceId::ProxyServerId,         "ProxyServer"},
    {ServiceId::XidMgrServerId,        "XigMgrServer"},
    {ServiceId::XidMgrClientId,        "XidMgrClient"},
    {ServiceId::SysTblMgrServerId,     "SysTblMgrServer"},
    {ServiceId::SysTblMgrClientId,     "SyTblMgrClient"},
    {ServiceId::WriteCacheServerId,    "WriteCacheServer"},
    {ServiceId::WriteCacheClientId,    "WriteCacheClient"},
    {ServiceId::IOMgrId,               "IOMgr"},
    {ServiceId::SchemaMgrId,           "SchemaMgr"},
    {ServiceId::TableMgrId,            "TableMgr"},
    {ServiceId::SyncTrackerId,         "SyncTracker"},
    {ServiceId::PgFdwMgrId,            "PGFdwMgr"},
    {ServiceId::PgXidSubscriberMgrId,  "PgXidSubscriberMgr"},
    {ServiceId::PgDDLMgrId,            "PgDDLMgr"},
    {ServiceId::PgLogCoordinatorId,    "PgLogCoordinator"},
    {ServiceId::StorageCacheId,        "StorageCache"},
    {ServiceId::VacuumerId,            "Vacuumer"}
};

std::vector<ServiceId>
topo_sort()
{
    const auto to_index = [](ServiceId id) {
        return static_cast<uint32_t>(id);
    };
    std::vector<bool> visited(to_index(ServiceId::ServiceCountId), false);
    std::vector<bool> on_stack(to_index(ServiceId::ServiceCountId), false);
    std::vector<ServiceId> result(to_index(ServiceId::ServiceCountId), ServiceId::ServiceInvalidId);
    std::size_t pos = to_index(ServiceId::ServiceCountId);

    auto depth_first_sort = [&visited, &on_stack, &result, &pos, &to_index](auto&& self, ServiceId id) -> void {
        auto idx = to_index(id);
        if (visited[idx]) {
            return;
        }
        CHECK(!on_stack[idx]) << "Cycle detected";

        on_stack[idx] = true;
        CHECK(dependencies.contains(id)) << "Missing service type " << idx;

        for (auto dep : dependencies.at(id)) {
            self(self, dep);
        }
        on_stack[idx] = false;

        visited[idx] = true;
        result[--pos] = id;
    };

    for (std::size_t i = 0; i < to_index(ServiceId::ServiceCountId); ++i) {
        depth_first_sort(depth_first_sort, static_cast<ServiceId>(i));
    }

    // validate
    for (std::size_t i = 0; i < to_index(ServiceId::ServiceCountId); ++i) {
        CHECK(result[i] != ServiceId::ServiceInvalidId)
            << "Unvisited service " << dependencies_names.at(static_cast<ServiceId>(i));
    }

    return result;
}

static std::vector<ServiceId> topo_sorted_services = {};

static std::map<ServiceId, ShutdownFunc> running_services = {};

static std::mutex running_services_mutex;

void
springtail_register_service(ServiceId service_id, ShutdownFunc fn)
{
    std::unique_lock running_services_lock(running_services_mutex);
    if (topo_sorted_services.empty()) {
        topo_sorted_services = topo_sort();
    }
    CHECK(!running_services.contains(service_id));
    running_services[service_id] = fn;
}

void
springtail_shutdown()
{
    std::unique_lock running_services_lock(running_services_mutex);
    for (auto service_id : topo_sorted_services) {
        auto it = running_services.find(service_id);
        if (it == running_services.end()) {
            continue;
        }
        LOG_INFO("Stopping service {}", dependencies_names.at(service_id));
        it->second();
    }
}

static std::map<ServiceId, std::map<std::string, std::any>> service_arguments;

void springtail_store_argument_internal(ServiceId service_id, const std::string &arg_name, const std::any &value)
{
    auto [it, inserted] = service_arguments.try_emplace(service_id, std::map<std::string, std::any>());
    it->second.try_emplace(arg_name, value);
}

std::any springtail_retreive_argument_internal(ServiceId service_id, const std::string &arg_name)
{
    auto it = service_arguments.find(service_id);
    CHECK(it != service_arguments.end());
    auto value_it = it->second.find(arg_name);
    CHECK(value_it != it->second.end());
    return value_it->second;
}

};  // namespace springtail
