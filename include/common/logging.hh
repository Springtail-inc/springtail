#pragma once

#include <concepts>
#include <map>
#include <optional>

#include <string>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/std.h>
#include <fmt/ranges.h>

#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/baggage/baggage_context.h>

#include <spdlog/spdlog.h>

#include <common/singleton.hh>

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_TRACE
    #define LOG_TRACE(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::trace, fmt, ##__VA_ARGS__)
#else
    #define LOG_TRACE(module, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
    #define LOG_DEBUG(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::debug, fmt, ##__VA_ARGS__)
#else
    #define LOG_DEBUG(module, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_INFO
    #define LOG_INFO(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::info, fmt, ##__VA_ARGS__)
#else
    #define LOG_INFO(module, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_WARN
    #define LOG_WARN(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::warn, fmt, ##__VA_ARGS__)
#else
    #define LOG_WARN(module, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_ERROR
    #define LOG_ERROR(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::err, fmt, ##__VA_ARGS__)
#else
    #define LOG_ERROR(module, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_CRITICAL
    #define LOG_CRITICAL(module,fmt, ...) springtail::logging::Logger::log(module, __func__, __FILE__, __LINE__, spdlog::level::critical, fmt, ##__VA_ARGS__)
#else
    #define LOG_CRITICAL(module, ...) (void)0
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
        LOG_COMMITTER = 0x800,
        LOG_SYS_TBL_MGR = 0x1000,
        LOG_ALL = 0xFFFFFFFF
    };

    namespace logging {
        template <typename T>
        concept DerivedFromSink = std::derived_from<T, spdlog::sinks::sink>;

        class Logger final : public Singleton<Logger> {
            friend class Singleton<Logger>;
        public:
            static std::unique_ptr<opentelemetry::context::Token>
            set_context_variables(const std::unordered_map<std::string, std::string>& attributes);

            static std::unique_ptr<opentelemetry::context::Token>
            set_context_variable(const std::string &attr_key, const std::string &attr_value);

            static std::unordered_map<std::string, std::string>
            get_context_variables();

            template <typename... Args> static void
            log(int log_id, const char *func, const char *file, int line, spdlog::level::level_enum level, fmt::format_string<Args...> fmt, Args&&... args)
            {
                if (_inited_flag) {
                    if ((log_id & get_instance()->_log_mask) == 0) {
                        return;
                    }
                }
                _log(spdlog::source_loc{file, line, func}, level, fmt, std::forward<Args>(args)...);
            }

            void init(const std::optional<uint32_t> &module_mask = std::nullopt,
                      const std::optional<std::string> &log_name = std::nullopt,
                      bool is_daemon = false);

        protected:
            class SpdlogSink : public absl::LogSink {
            public:
                void Send(const absl::LogEntry& entry) override {
                    spdlog::level::level_enum spdlog_level;
                    switch (entry.log_severity()) {
                        case absl::LogSeverity::kInfo:
                            spdlog_level = spdlog::level::info;
                            break;
                        case absl::LogSeverity::kWarning:
                            spdlog_level = spdlog::level::warn;
                            break;
                        case absl::LogSeverity::kError:
                            spdlog_level = spdlog::level::err;
                            break;
                        case absl::LogSeverity::kFatal:
                            spdlog_level = spdlog::level::critical;
                            break;
                        default:
                            spdlog_level = spdlog::level::debug;
                            break;
                    }

                    spdlog::log(
                        spdlog_level,
                        "[{}:{}] {}",
                        entry.source_filename(),
                        entry.source_line(),
                        entry.text_message());
                }
            };

            static std::map<std::string, uint32_t> _log_module_map;
            static inline std::atomic<bool> _inited_flag{false};
            SpdlogSink _spdlog_sink;
            uint32_t _log_mask{LOG_ALL};

            void _internal_shutdown() override;

            template <typename DerivedFromSink> static void
            _set_level(std::shared_ptr<DerivedFromSink> &logger_sink, const std::string &level);

            template <typename... Args> static void
            _log(spdlog::source_loc loc, spdlog::level::level_enum lvl, fmt::format_string<Args...> fmt, Args &&...args) {
                std::unordered_map<std::string, std::string> context = springtail::logging::Logger::get_context_variables();
                auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
                auto baggage = opentelemetry::baggage::GetBaggage(ctx);

                std::string formatted_msg;

                baggage->GetAllEntries([&formatted_msg](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
                    std::string_view key_sv{key.data(), key.size()};
                    std::string_view value_sv{value.data(), value.size()};

                    fmt::format_to(std::back_inserter(formatted_msg), "[{}: {}] ", key_sv, value_sv);
                    return true;
                });

                formatted_msg += fmt::vformat(fmt, fmt::make_format_args(args...));
                spdlog::default_logger()->log(loc, lvl, formatted_msg);
            }

        };
    } // namespace logging

} // namespace springtail
