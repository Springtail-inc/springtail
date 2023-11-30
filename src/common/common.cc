#include <filesystem>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>

namespace springtail {
    void springtail_init() {
        // initialize the backtrace infrastructure
        backward::sh.loaded();

        // initialize the logging infrastructure
        init_logging();

        // init system properties
        if (std::filesystem::exists(std::filesystem::path(Properties::SPRINGTAIL_PROPERTIES_FILE))) {
            Properties::init(std::string(Properties::SPRINGTAIL_PROPERTIES_FILE));
        } else {
            throw Error("Springtail system.json properties file not found");
        }
    }
}
