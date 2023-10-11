#include <spdlog/sinks/stdout_color_sinks.h>

#include <common/logging.hh>

namespace springtail {

    void init_logging() {
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
        logger->set_pattern("%@ [%H:%M:%S %z] [%n] [%^---%L---%$] [thread %t] %v");

        spdlog::set_default_logger(logger);

        logger->error("Test");
        SPDLOG_ERROR("Test 2");
    }

}
