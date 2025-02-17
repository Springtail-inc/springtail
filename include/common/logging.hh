#pragma once
#include <cstdint>
#include <optional>
#include <utility>
#include <memory>
#include <map>
#include <string>
#include <optional>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/std.h>

#include <opentelemetry/baggage/baggage.h>
#include <opentelemetry/baggage/baggage_context.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/context/context_value.h>
#include <opentelemetry/context/runtime_context.h>
#include <spdlog/spdlog.h>

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
    #define SPDLOG_DEBUG_MODULE(module,fmt, ...) springtail::logging::debug(module, fmt, __func__, __FILE__, __LINE__, ##__VA_ARGS__)
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
        LOG_FDW = 0x100,
        LOG_CACHE = 0x200,
        LOG_SCHEMA = 0x400,
        LOG_GC = 0x800,
        LOG_SYS_TBL_MGR = 0x1000,
        LOG_ALL = 0xFFFFFFFF
    };

    static std::map<std::string, uint32_t> log_module_map = {
        {"pg_repl", LOG_PG_REPL},
        {"pg_log_mgr", LOG_PG_LOG_MGR},
        {"write_cache_server", LOG_WRITE_CACHE_SERVER},
        {"btree", LOG_BTREE},
        {"storage", LOG_STORAGE},
        {"xid_mgr", LOG_XID_MGR},
        {"common", LOG_COMMON},
        {"proxy", LOG_PROXY},
        {"fdw", LOG_FDW},
        {"cache", LOG_CACHE},
        {"schema", LOG_SCHEMA},
        {"gc", LOG_GC},
        {"none", LOG_NONE},
        {"all", LOG_ALL}
    };

    /**
     * @brief Initialize logging system
     * @param log_ids optionally pass in a set of modules (bitwise OR'ed) to filter debug logs
     * @param log_name optionally pass in a log name to use, relative path
     * Use macro above: SPDLOG_DEBUG_MODULE(module, ...)
     */
    void init_logging(const std::optional<uint32_t> &module_mask = std::nullopt,
                      const std::optional<std::string> &log_name = std::nullopt,
                      bool is_daemon = false);


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

        inline std::unique_ptr<opentelemetry::context::Token>
        set_otel_context(uint64_t db_id, uint64_t xid)
        {
            auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
            ctx = ctx.SetValue("db_id", static_cast<int64_t>(db_id));
            ctx = ctx.SetValue("xid", static_cast<int64_t>(xid));
            return opentelemetry::context::RuntimeContext::Attach(ctx);
        }
    }
}
