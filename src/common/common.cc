#include <filesystem>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>

namespace springtail {

    namespace {
        void
        daemonize(const std::filesystem::path &pid_filename)
        {
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
                         const std::optional<std::filesystem::path> &daemon_pid,
                         uint32_t logging_mask)
    {
        // if requested, daemonize the process
        if (daemon_pid) {
            daemonize(*daemon_pid);
        }

        // initialize the backtrace signal handling
        init_exception();

        // init system properties
        Properties::init();

        // initialize the logging infrastructure
        init_logging(logging_mask, log_filename, daemon_pid.has_value());
    }

    void springtail_init(const std::string &log_filename,
                         uint32_t logging_mask)
    {
        springtail_init(log_filename, std::nullopt, logging_mask);
    }

}
