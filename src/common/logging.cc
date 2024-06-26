#include <memory>
#include <set>
#include <vector>
#include <optional>
#include <map>
#include <iostream>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include <nlohmann/json.hpp>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

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

    static void set_level(auto &logger, const std::string &level)
    {
        if (level == "debug") {
            logger->set_level(spdlog::level::debug);
        } else if (level == "info") {
            logger->set_level(spdlog::level::info);
        } else if (level == "warn") {
            logger->set_level(spdlog::level::warn);
        } else if (level == "error") {
            logger->set_level(spdlog::level::err);
        } else if (level == "critical") {
            logger->set_level(spdlog::level::critical);
        } else {
            logger->set_level(spdlog::level::trace);
        }
    }

    void init_logging(uint32_t module_mask)
    {
        nlohmann::json props = Properties::get(Properties::LOGGING_CONFIG);

        // log bitmask
        logging::_log_mask = module_mask;

        // configuration options
        std::string log_path;
        std::string log_level;
        std::string pattern;
        int max_size;
        int max_files;
        Json::get_to<std::string>(props, "log_path", log_path, "/tmp/springtail_log.txt");
        Json::get_to<int>(props, "log_file_size", max_size, 1024 * 1024 * 5);
        Json::get_to<int>(props, "log_file_count", max_files, 5);
        Json::get_to<std::string>(props, "log_level", log_level, "trace");
        Json::get_to<std::string>(props, "log_pattern", pattern, "[%Y-%m-%d %T.%e %z] [%^%l%$] [%s:%#:%!] [thread %t] %v");

        // console sink
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_pattern(pattern);
        set_level(console_sink, log_level);

        // file sink
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_path, max_size, max_files);
        set_level(file_sink, log_level);

        // create the logger for both console and file sink
        spdlog::sinks_init_list list = {console_sink, file_sink};
        auto logger = std::make_shared<spdlog::logger>("springtail", list);
        logger->set_pattern(pattern);
        set_level(logger, log_level);
        logger->flush_on(spdlog::level::err);

        spdlog::set_default_logger(logger);
        spdlog::flush_every(std::chrono::seconds(3));
    }
} // namespace springtail
