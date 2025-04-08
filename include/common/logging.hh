#pragma once

#include <map>
#include <optional>
#include <string>

#include <fmt/format.h>
#include <fmt/std.h>

#include <spdlog/spdlog.h>

#include <common/open_telemetry.hh>
#include <common/singleton.hh>

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_TRACE
    #define LOG_TRACE(module,fmt, ...) springtail::logging::Logger::debug(module, __func__, __FILE__, __LINE__, spdlog::level::trace, fmt, ##__VA_ARGS__)
#else
    #define LOG_TRACE(module, fmt, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
    #define LOG_DEBUG(module, fmt, ...) springtail::logging::Logger::debug(module, __func__, __FILE__, __LINE__, spdlog::level::debug, fmt, ##__VA_ARGS__)
#else
    #define LOG_DEBUG(module, fmt, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_INFO
    #define LOG_INFO(fmt, ...) springtail::logging::Logger::log(__func__, __FILE__, __LINE__, spdlog::level::info, fmt, ##__VA_ARGS__)
#else
    #define LOG_INFO(fmt, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_WARN
    #define LOG_WARN(fmt, ...) springtail::logging::Logger::log(__func__, __FILE__, __LINE__, spdlog::level::warn, fmt, ##__VA_ARGS__)
#else
    #define LOG_WARN(fmt, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_ERROR
    #define LOG_ERROR(fmt, ...) springtail::logging::Logger::log(__func__, __FILE__, __LINE__, spdlog::level::err, fmt, ##__VA_ARGS__)
#else
    #define LOG_ERROR(fmt, ...) (void)0
#endif

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_CRITICAL
    #define LOG_CRITICAL(fmt, ...) springtail::logging::Logger::log( __func__, __FILE__, __LINE__, spdlog::level::critical, fmt, ##__VA_ARGS__)
#else
    #define LOG_CRITICAL(fmt, ...) (void)0
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

    /**
     * @brief Forward declaration for OpenTelemetrySink.
     *
     */
    // class OpenTelemetrySink;

    namespace logging {
        /**
         * @brief Logger singleton class
         *
         */
        class Logger final : public Singleton<Logger> {
            friend class Singleton<Logger>;
        public:
            /**
             * @brief Debug logging function
             *
             * @tparam Args - argument list
             * @param log_id - identifier filtered by the log mask
             * @param func - calling function name
             * @param file - file name
             * @param line - line number
             * @param level - log level
             * @param fmt - format string
             * @param args - argument list
             */
            template <typename... Args> static void
            debug(int log_id, const char *func, const char *file, int line, spdlog::level::level_enum level, fmt::format_string<Args...> fmt, Args&&... args)
            {
                if (_inited_flag) {
                    if ((log_id & get_instance()->_log_mask) == 0) {
                        return;
                    }
                }
                _log(spdlog::source_loc{file, line, func}, level, fmt, std::forward<Args>(args)...);
            }

            /**
             * @brief Debug logging function
             *
             * @tparam Args - argument list
             * @param func - calling function name
             * @param file - file name
             * @param line - line number
             * @param level - log level
             * @param fmt - format string
             * @param args - argument list
             */
             template <typename... Args> static void
            log(const char *func, const char *file, int line, spdlog::level::level_enum level, fmt::format_string<Args...> fmt, Args&&... args)
            {
                _log(spdlog::source_loc{file, line, func}, level, fmt, std::forward<Args>(args)...);
            }

            /**
             * @brief Log object init function
             *
             * @param module_mask - mask for log id
             * @param log_name - name of log file for log storage
             * @param is_daemon - running as daemon flag, when it is on, does not turn on console sink
             */
            void init(const std::optional<uint32_t> &module_mask = std::nullopt,
                      const std::optional<std::string> &log_name = std::nullopt,
                      bool is_daemon = false);

            static spdlog::level::level_enum get_log_level_from_string(const std::string &level);

        protected:
            /** Helper class for forwarding failed CHECKs and DCHECKs to the log */
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

            static std::map<std::string, uint32_t> _log_module_map;    ///< mapping from log id name to value
            /**
             * @brief flag that indicates if logging was fully initialized, as it determins behavior of
             *      the log function
             *
             */
            static inline std::atomic<bool> _inited_flag{false};
            SpdlogSink _spdlog_sink;        ///< sink object for CHECKs and DCHECKs
            // std::shared_ptr<OpenTelemetrySink> _otel_sink{nullptr};     ///< OTEL sink

            uint32_t _log_mask{LOG_ALL};    ///< current log mask

            /**
             * @brief function that performs logging shutdown
             *
             */
            void _internal_shutdown() override;

            /**
             * @brief Internal log function that logs to spdlog and depending on configuration to OTEL.
             *
             * @tparam Args - variagle number of arguments template parameter
             * @param loc   - source location argument
             * @param lvl   - log level
             * @param fmt   - log format string
             * @param args  - variable number of argumenst
             */
            template <typename... Args> static void
            _log(spdlog::source_loc loc, spdlog::level::level_enum lvl, fmt::format_string<Args...> fmt, Args &&...args)
            {
                // create formatted message
                std::string formatted_msg = fmt::vformat(fmt, fmt::make_format_args(args...));

                // log to otel sink
                open_telemetry::OpenTelemetry::log(loc, spdlog::default_logger()->name(), lvl, formatted_msg);

                // put together the full message
                std::string full_msg;

                // extract baggage
                open_telemetry::OpenTelemetry::get_context_variables([&full_msg](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
                    std::string_view key_sv{key.data(), key.size()};
                    std::string_view value_sv{value.data(), value.size()};

                    fmt::format_to(std::back_inserter(full_msg), "[{}: {}] ", key_sv, value_sv);
                    return true;
                });

                full_msg += formatted_msg;
                spdlog::default_logger()->log(loc, lvl, full_msg);
            }
        };
    } // namespace logging

} // namespace springtail
