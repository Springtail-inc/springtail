#pragma once

#include <common/logging.hh>

namespace springtail::pg_proxy {

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
    #define PROXY_DEBUG(level, fmt, ...) debug(level, fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__)
#else
    #define PROXY_DEBUG(level, ...) (void)0
#endif

    /** Log levels for the proxy */
    enum LogLevel : int {
        LOG_LEVEL_DEBUG1 = 1, // highest level, most priority
        LOG_LEVEL_DEBUG2 = 2,
        LOG_LEVEL_DEBUG3 = 3,
        LOG_LEVEL_DEBUG4 = 4, // lowest level, least priority
    };

    /** Global log level for the proxy -- defined in server.cc */
    extern LogLevel proxy_log_level;

    /** Internal call to handle debug logging filtered by level */
    template <typename... Args> void
    debug(LogLevel level, fmt::format_string<Args...> fmt, const char *func, const char *file, int line, Args&&... args)
    {
        if (level > proxy_log_level) {
            return;
        }
        springtail::logging::Logger::log(LOG_PROXY, func, file, line, spdlog::level::debug, fmt, std::forward<Args>(args)...);
    }
}
