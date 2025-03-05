#include <iostream>

#include <common/init.hh>

using namespace springtail;

namespace {

    std::atomic<bool> shutdown_flag = false;

    void
    handle_sigint(int signal)
    {
        shutdown_flag = true;
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

void
springtail_init(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners,
                const bool load_redis,
                const std::optional<std::string> &log_filename,
                const std::optional<uint32_t> &logging_mask)
{
    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(load_redis));
    service_runners.emplace_back(std::make_unique<LoggingRunner>(log_filename, std::nullopt, logging_mask));
    service_runners.emplace_back(std::make_unique<TracingRunner>(log_filename));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());

    if (runners.has_value()) {
        std::vector<std::unique_ptr<ServiceRunner>> *non_const_runners = const_cast<std::vector<std::unique_ptr<ServiceRunner>> *>(&runners.value());
        std::move(non_const_runners->begin(), non_const_runners->end(), std::back_inserter(service_runners));
    }

    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void springtail_init_daemon(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners,
                            const std::optional<std::string> &log_filename,
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
    service_runners.emplace_back(std::make_unique<TracingRunner>(log_filename));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());

    if (runners.has_value()) {
        std::vector<std::unique_ptr<ServiceRunner>> *non_const_runners = const_cast<std::vector<std::unique_ptr<ServiceRunner>> *>(&runners.value());
        std::move(non_const_runners->begin(), non_const_runners->end(), std::back_inserter(service_runners));
    }

    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void
springtail_init_test(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners,
                     const std::optional<uint32_t> &logging_mask)
{
    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(true));
    service_runners.emplace_back(std::make_unique<LoggingRunner>(std::nullopt, std::nullopt, logging_mask));
    service_runners.emplace_back(std::make_unique<TracingRunner>(std::nullopt));
    service_runners.emplace_back(std::make_unique<RedisMgrRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesCacheRunner>());

    if (runners.has_value()) {
        std::vector<std::unique_ptr<ServiceRunner>> *non_const_runners = const_cast<std::vector<std::unique_ptr<ServiceRunner>> *>(&runners.value());
        std::move(non_const_runners->begin(), non_const_runners->end(), std::back_inserter(service_runners));
    }

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
springtail_shutdown()
{
    ServiceRegister::shutdown();
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
    while (!shutdown_flag) {
        shutdown_flag.wait(false);
    }

    // restore signal handlers to default
    for (int sig : signals) {
        std::signal(sig, SIG_DFL);
    }
}

};  // namespace springtail::init
