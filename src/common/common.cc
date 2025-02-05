#include <filesystem>
#include <string>
#include <atomic>
#include <iostream>
#include <optional>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/tracing.hh>

namespace springtail {

    namespace {

        // flag to prevent init from being called multiple times
        static std::atomic_flag init_lock = ATOMIC_FLAG_INIT;

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
    }

    void springtail_init(const std::optional<std::string> &log_filename,
                         const std::optional<std::string> &daemon_pid,
                         const std::optional<uint32_t> &logging_mask)
    {
        // prevent multiple calls to init
        if (init_lock.test_and_set()) {
            SPDLOG_WARN("Warning: springtail_init called multiple times");
            return;
        }

        // initialize the backtrace signal handling
        init_exception();

        // init system properties
        // only load redis from properties if no daemon pid is set
        Properties::get_instance()->init(!daemon_pid.has_value());

        // if requested, daemonize the process
        if (daemon_pid) {
            daemonize(*daemon_pid);
        }

        // initialize the logging infrastructure
        init_logging(logging_mask, log_filename, daemon_pid.has_value());

        // initialize the tracing infrastructure
        tracing::init_tracing_and_metrics(log_filename.value_or(""));
    }

    void
    springtail_init(const std::string &log_filename, uint32_t logging_mask)
    {
        springtail_init(log_filename, std::nullopt, logging_mask);
    }

}  // namespace springtail
