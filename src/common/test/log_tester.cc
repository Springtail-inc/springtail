#include <common/init.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>

using namespace springtail;
using namespace springtail::logging;
using namespace springtail::open_telemetry;

void
log_stuff()
{
    // basic functionality
    LOG_TRACE(LOG_ALL, "My trace log");
    LOG_DEBUG(LOG_ALL, "My debug log");
    LOG_INFO("My info log");
    LOG_WARN("My warning log");
    LOG_ERROR("My error log");
    LOG_CRITICAL("My critical log");

    // basic functionality with thread id
    LOG_TRACE(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_DEBUG(LOG_COMMON, "Current: {}\n", std::this_thread::get_id());
    LOG_INFO("Current: {}\n", std::this_thread::get_id());
    LOG_WARN("Current: {}\n", std::this_thread::get_id());
    LOG_ERROR("Current: {}\n", std::this_thread::get_id());
    LOG_CRITICAL("Current: {}\n", std::this_thread::get_id());

    spdlog::set_level(spdlog::level::trace);

    // basic functionality with thread id
    LOG_TRACE(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_DEBUG(LOG_COMMON, "One more time current: {}\n", std::this_thread::get_id());
    LOG_INFO("One more time current: {}\n", std::this_thread::get_id());
    LOG_WARN("One more time current: {}\n", std::this_thread::get_id());
    LOG_ERROR("One more time current: {}\n", std::this_thread::get_id());
    LOG_CRITICAL("One more time current: {}\n", std::this_thread::get_id());

    {
        auto token1 = OpenTelemetry::get_instance()->set_context_variable("db_id", "1");
        LOG_INFO("Log something with token1");
        {
            auto token2 = OpenTelemetry::get_instance()->set_context_variable("xact_id", "2");
            LOG_INFO("Log something with token1 and token2");
        }
        LOG_INFO("Log something with token1 again");
    }

    LOG_INFO("Log something without token");
}

int
main(int argc, char *argv[])
{
    log_stuff();

    std::vector<std::unique_ptr<ServiceRunner>> service_runners;
    service_runners.emplace_back(std::make_unique<DefaultLoggingRunner>());
    service_runners.emplace_back(std::make_unique<ExceptionRunner>());
    service_runners.emplace_back(std::make_unique<PropertiesRunner>(true));
    service_runners.emplace_back(std::make_unique<LoggingRunner>("test", std::nullopt, LOG_ALL));
    service_runners.emplace_back(std::make_unique<OpenTelemetryRunner>("test"));

    springtail_init_custom(service_runners);

    log_stuff();

    // increment without context
    OpenTelemetry::get_instance()->increment_counter(XID_MGR_COMMIT_XID_CALLS);

    // increment with context
    {
        auto token1 = OpenTelemetry::get_instance()->set_context_variable("db_id", "1");
        OpenTelemetry::get_instance()->increment_counter(XID_MGR_COMMIT_XID_CALLS);
        {
            auto token2 = OpenTelemetry::get_instance()->set_context_variable("xact_id", "2");
            OpenTelemetry::get_instance()->increment_counter(XID_MGR_COMMIT_XID_CALLS);
        }
        OpenTelemetry::get_instance()->increment_counter(XID_MGR_COMMIT_XID_CALLS);
    }

    // test tracers, spans, and scopes
    {
        auto token2 = OpenTelemetry::get_instance()->set_context_variable("xact_id", "2");
        auto tracer = open_telemetry::OpenTelemetry::get_instance()->tracer("test tracer");
        auto span = tracer->StartSpan("Test Span", {
            {"span attribute 1", "value 1"},
            {"span attribute 2", "value 2"}});

        LOG_INFO("Started span");

        {
            auto token1 = OpenTelemetry::get_instance()->set_context_variable("db_id", "1");
            auto scope1 = tracer->WithActiveSpan(span);
            span->AddEvent("Span Event 1", {
                {"Event 1 Attribute 1", "value 1"},
                {"Event 1 Attribute 1", "value 2" }});

            LOG_INFO("Added event 1 in scope 1");

            span->AddEvent("Span Event 2", {
                {"Event 2 Attribute 1", "value 1"},
                {"Event 2 Attribute 1", "value 2" }});

            LOG_INFO("Added event 2 in scope 1");
        }

        {
            auto token1 = OpenTelemetry::get_instance()->set_context_variable("db_id", "1");
            auto scope2 = tracer->WithActiveSpan(span);
            span->AddEvent("Span Event 2", {
                {"Event 1 Attribute 1", "value 1"},
                {"Event 1 Attribute 1", "value 2" }});

            LOG_INFO("Added event 1 in scope 2");

            span->AddEvent("Span Event 2", {
                {"Event 2 Attribute 1", "value 1"},
                {"Event 2 Attribute 1", "value 2" }});

            LOG_INFO("Added event 2 in scope 2");
        }
        span->End();
    }

    springtail_shutdown();
    return 0;
}
