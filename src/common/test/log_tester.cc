#include <common/logging.hh>
#include <common/init.hh>

using namespace springtail;
using namespace springtail::logging;

void
log_stuff()
{
    // basic functionality
    LOG_TRACE(LOG_ALL, "My trace log");
    LOG_DEBUG(LOG_ALL, "My debug log");
    LOG_INFO(LOG_ALL, "My info log");
    LOG_WARN(LOG_ALL, "My warning log");
    LOG_ERROR(LOG_ALL, "My error log");
    LOG_CRITICAL(LOG_ALL, "My critical log");

    // basic functionality with thread id
    LOG_TRACE(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_DEBUG(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_INFO(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_WARN(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_ERROR(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_CRITICAL(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());

    spdlog::set_level(spdlog::level::trace);

    // basic functionality with thread id
    LOG_TRACE(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_DEBUG(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_INFO(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_WARN(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_ERROR(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_CRITICAL(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());

    {
        auto token1 = Logger::set_context_variable("db_id", "1");
        LOG_INFO(LOG_ALL, "Log something with token1");
        {
            auto token2 = Logger::set_context_variable("xact_id", "2");
            LOG_INFO(LOG_ALL, "Log something with token1 and token2");
        }
        LOG_INFO(LOG_ALL, "Log something with token1 again");
    }

    LOG_INFO(LOG_ALL, "Log something without token");
}

int
main(int argc, char *argv[])
{
    log_stuff();

    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(true));
    service_runners.emplace_back(std::make_unique<LoggingRunner>(std::nullopt, std::nullopt, LOG_ALL));

    springtail_init_custom(service_runners);

    log_stuff();

    springtail_shutdown();
    return 0;
}