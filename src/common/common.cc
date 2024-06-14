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
}
