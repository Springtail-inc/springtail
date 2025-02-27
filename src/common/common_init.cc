#include <iostream>

#include <common/common_init.hh>

using namespace springtail;

namespace springtail {

void
daemonize(const std::string &pid_file)
{
    std::string pid_path = Properties::get_instance()->get_pid_path();

    std::filesystem::path pid_filename(pid_path);
    pid_filename /= pid_file;

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
}

void
springtail_init(const std::vector<ServiceRunner *> &runners,
                const bool load_redis,
                const std::optional<std::string> &log_filename,
                const std::optional<std::string> &daemon_pid,
                const std::optional<uint32_t> &logging_mask)
{
    std::vector<ServiceRunner *> service_runners = {
        new DefaultLoggingRunner(),
        new ExceptionRunner(),
        new PropertiesRunner(load_redis),
        new DaemonRunner(daemon_pid),
        new LoggingRunner(log_filename, daemon_pid, logging_mask),
        new TracingRunner(log_filename),
        new PropertiesCacheRunner()};

    service_runners.insert(service_runners.end(), runners.begin(), runners.end());
    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void
springtail_shutdown()
{
    ServiceRegister::shutdown();
}

};  // namespace springtail::init
