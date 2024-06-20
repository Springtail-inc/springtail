#include <filesystem>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>

namespace springtail {

    void init_system_properties()
    {
        if (std::filesystem::exists(std::filesystem::path(Properties::SPRINGTAIL_PROPERTIES_FILE))) {
            Properties::init(std::string(Properties::SPRINGTAIL_PROPERTIES_FILE));
            return;
        }

        // check current dir
        if (std::filesystem::exists(std::filesystem::path("./system.json"))) {
            Properties::init("./system.json");
            return;
        }

        const char *env_p = std::getenv("SPRINGTAIL_PROPERTIES_FILE");
        if (env_p && std::filesystem::exists(std::filesystem::path(env_p))) {
            Properties::init(env_p);
            return;
        }

        throw Error("Springtail system.json properties file not found");
    }

    void springtail_init(uint32_t logging_mask) {
        // initialize the backtrace infrastructure
        backward::sh.loaded();

        // init system properties
        init_system_properties();

        // initialize the logging infrastructure
        init_logging(logging_mask);
    }

    void
    common::daemonize(const std::filesystem::path &pid_filename)
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
            // record the pid of the child into the pid file
            std::ofstream pid_file(pid_filename);
            pid_file << pid << std::endl;

            // exit cleanly
            std::exit(0);
        }
    }
}
