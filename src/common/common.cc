#include <filesystem>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>

namespace springtail {

    void init_system_properties(const std::string &path)
    {
        if (std::filesystem::exists(std::filesystem::path(path))) {
            Properties::init(path);
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

    void springtail_init(uint32_t logging_mask, const std::string &properties_path) {
        // initialize the backtrace infrastructure
        backward::sh.loaded();

        // init system properties
        init_system_properties(properties_path);

        // initialize the logging infrastructure
        init_logging(logging_mask);
    }
}
