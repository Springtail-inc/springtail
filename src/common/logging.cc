#include <memory>
#include <set>
#include <vector>
#include <optional>
#include <map>
#include <iostream>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <common/logging.hh>

namespace springtail {

namespace logging {
    uint32_t _log_mask = LOG_ALL;

    std::shared_ptr<spdlog::logger> get_logger(uint32_t log_id)
    {
        if ((log_id & _log_mask) == 0) {
            return nullptr;
        }
        return spdlog::default_logger();
    }
} // namespace logging

    void init_logging(uint32_t module_mask)
    {
        // log bitmask
        logging::_log_mask = module_mask;

        // setup the console to print everything
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(spdlog::level::trace);
        console_sink->set_pattern("");

        /*
        // example of a file sink
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("log.txt", true);
        file_sink->set_level(spdlog::level::trace);
        */

        auto logger = std::make_shared<spdlog::logger>("springtail", console_sink);
        logger->set_level(spdlog::level::debug);
        logger->set_pattern("[%Y-%m-%d %T.%e %z] [%^%l%$] [%s:%#:%!] [thread %t] %v");

        spdlog::set_default_logger(logger);
    }
} // namespace springtail
