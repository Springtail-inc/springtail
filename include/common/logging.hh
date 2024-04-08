#pragma once
#include <vector>
#include <optional>
#include <utility>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/std.h>

#include <spdlog/spdlog.h>

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
    #define SPDLOG_DEBUG_MODULE(module,fmt, ...) springtail::logging::debug(module, fmt, __func__, __FILE__, __LINE__, __VA_ARGS__)
#else
    #define SPDLOG_DEBUG_MODULE(module, ...) (void)0
#endif

namespace springtail {
    /**
     * @brief Enum for log module ids, add to end as hex bit value
     */
    enum : uint32_t {
        LOG_NONE = 0,
        LOG_PG_REPL = 0x01,
        LOG_PG_LOG_MGR = 0x02,
        LOG_WRITE_CACHE_SERVER = 0x04,
        LOG_BTREE = 0x08,
        LOG_STORAGE = 0x10,
        LOG_XID_MGR = 0x20,
        LOG_COMMON = 0x40,
        LOG_PROXY = 0x80,
        LOG_ALL = 0xFFFFFFFF
    };

    /**
     * @brief Initialize logging system
     * @param log_ids optionally pass in a set of modules (bitwise OR'ed) to filter debug logs
     * Use macro above: SPDLOG_DEBUG_MODULE(module, ...)
     */
    void init_logging(uint32_t module_mask=LOG_ALL);

    namespace logging {
        /** Internal call to get a logger based on log id */
        std::shared_ptr<spdlog::logger> get_logger(uint32_t log_id);

        /** Internal call to handle debug logging filtered by module */
        template <typename... Args> void
        debug(int log_id, fmt::format_string<Args...> fmt, const char *func, const char *file, int line, Args&&... args)
        {
            std::shared_ptr<spdlog::logger> logger = get_logger(log_id);
            if (logger == nullptr) {
                return;
            }
            logger->log(spdlog::source_loc{file, line, func}, spdlog::level::debug, fmt, std::forward<Args>(args)...);
        }
    }
}
