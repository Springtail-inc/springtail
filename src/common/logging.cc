#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/basic_file_sink.h>

#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>
#include <common/opentelemetry_sink.hh>

namespace springtail::logging {

    std::map<std::string, uint32_t> Logger::_log_module_map = {
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
        {"committer", LOG_COMMITTER},
        {"sys_tbl_mgr", LOG_SYS_TBL_MGR},
        {"none", LOG_NONE},
        {"all", LOG_ALL}
    };

    std::unique_ptr<opentelemetry::context::Token>
    Logger::set_context_variables(const std::unordered_map<std::string, std::string>& attributes)
    {
        auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();

        auto baggage = opentelemetry::baggage::GetBaggage(ctx);

        // Iterate over attributes and set baggage values
        for (const auto& attribute : attributes) {
            baggage = baggage->Set(attribute.first, attribute.second);
        }

        auto updated_context = opentelemetry::baggage::SetBaggage(ctx, baggage);
        return opentelemetry::context::RuntimeContext::Attach(updated_context);
    }

    std::unique_ptr<opentelemetry::context::Token>
    Logger::set_context_variable(const std::string &attr_key, const std::string &attr_value)
    {
        auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();

        auto baggage = opentelemetry::baggage::GetBaggage(ctx);
        baggage = baggage->Set(attr_key, attr_value);

        auto updated_context = opentelemetry::baggage::SetBaggage(ctx, baggage);
        return opentelemetry::context::RuntimeContext::Attach(updated_context);
    }

    std::unordered_map<std::string, std::string>
    Logger::get_context_variables()
    {
        auto ctx = opentelemetry::context::RuntimeContext::GetCurrent();
        auto baggage = opentelemetry::baggage::GetBaggage(ctx);
        std::unordered_map<std::string, std::string> attributes;

        // Iterate over all the baggage entries and populate the attributes object
        baggage->GetAllEntries([&attributes](opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) {
            attributes[std::string(key)] = std::string(value);
            return true;
        });

        return attributes;
    }

    template <typename DerivedFromSink> void
    Logger::_set_level(std::shared_ptr<DerivedFromSink> &logger_sink, const std::string &level)
    {
        if (level == "debug") {
            logger_sink->set_level(spdlog::level::debug);
        } else if (level == "info") {
            logger_sink->set_level(spdlog::level::info);
        } else if (level == "warn") {
            logger_sink->set_level(spdlog::level::warn);
        } else if (level == "error") {
            logger_sink->set_level(spdlog::level::err);
        } else if (level == "critical") {
            logger_sink->set_level(spdlog::level::critical);
        } else {
            logger_sink->set_level(spdlog::level::trace);
        }
    }

    void
    Logger::_log_otel(const spdlog::details::log_msg &msg)
    {
        get_instance()->_otel_sink->log(msg);
    }

    void
    Logger::init(const std::optional<uint32_t> &module_mask_opt,
                 const std::optional<std::string> &log_name,
                 bool is_daemon)
    {
        nlohmann::json props = Properties::get(Properties::LOGGING_CONFIG);

        uint32_t module_mask = module_mask_opt.has_value() ? module_mask_opt.value() : LOG_ALL;

        // configuration options
        std::string log_path_str = Json::get_or<std::string>(props, "log_path", "/tmp/");
        int max_size = Json::get_or<int>(props, "log_file_size", 1024 * 1024 * 5);
        int max_files = Json::get_or<int>(props, "log_file_count", 5);
        std::string log_level = Json::get_or<std::string>(props, "log_level", "trace");
        std::string pattern = Json::get_or<std::string>(props, "log_pattern", "[%Y-%m-%d %T.%e %z] [%^%l%$] [%s:%#:%!] [thread %t] %v");
        bool log_rotation_enabled = Json::get_or<bool>(props, "log_rotation_enabled", true);

        // if the mask wasn't passed in then check if log_module is set in properties
        if (!module_mask_opt && props.contains("log_modules")) {
            std::set<std::string> log_modules;
            // extract log_module array from properties
            Json::get_to<std::set<std::string>>(props, "log_modules", log_modules);
            // generate bit pattern for log modules looking up in map
            module_mask = 0;
            for (const auto &module : log_modules) {
                if (_log_module_map.find(module) != _log_module_map.end()) {
                    module_mask |= _log_module_map[module];
                } else {
                    std::cerr << fmt::format("Unknown log module: {}\n", module);
                }
            }
        }

        // if log_name is set, then override log_path
        if (log_name.has_value()) {
            std::filesystem::path log_path{log_path_str};
            std::filesystem::path log_name_path{log_name.value()};

            // add extension if not already exists
            if (!log_name_path.has_extension()) {
                log_name_path += ".log";
            }

            // if log_name is absolute path, then use it as is
            if (log_name_path.is_absolute()) {
                log_path = log_name_path;
            } else {
                log_path = log_path / log_name_path;
            }
            log_path_str = log_path.string();

        } else {
            std::filesystem::path log_path{log_path_str};
            if (!log_path.has_extension()) {
                log_path = log_path / "springtail.log";
                log_path_str = log_path.string();
            }
        }

        // log bitmask
        _log_mask = module_mask;

        std::vector<spdlog::sink_ptr> sinks;

        // console sink
        if (!is_daemon) {
            auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            console_sink->set_pattern(pattern);
            _set_level(console_sink, log_level);
            sinks.push_back(console_sink);
        }

        // create all directories in log path
        auto path = std::filesystem::path(log_path_str).parent_path();
        if (!std::filesystem::exists(path)) {
            std::filesystem::create_directories(path);
            std::filesystem::permissions(path,
                std::filesystem::perms::owner_all |
                std::filesystem::perms::group_all |
                std::filesystem::perms::others_all);
        }

        // file sink
        if (log_rotation_enabled) {
            auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_path_str, max_size, max_files);
            _set_level(file_sink, log_level);
            sinks.push_back(file_sink);
        } else {
            auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path_str);
            _set_level(file_sink, log_level);
            sinks.push_back(file_sink);
        }

        // Check OpenTelemetry configuration
        auto otel_config = Properties::get(Properties::OTEL_CONFIG);
        bool otel_enabled = Json::get_or<bool>(otel_config, "enabled", false);
        bool otel_remote = Json::get_or<bool>(otel_config, "remote", false);

        if (otel_enabled && otel_remote) {
            // host ex: http://otel_collector, port ex: 4318
            auto host = Json::get<std::string>(otel_config, "host");
            auto port = Json::get<int>(otel_config, "port");
            auto remote_log_level = Json::get_or<std::string>(otel_config, "remote_log_level", "info");

            if (host && port) {
                std::string endpoint = fmt::format("{}:{}/v1/logs", *host, *port);
                _otel_sink = std::make_shared<OpenTelemetrySink>("springtail", endpoint);
                _set_level(_otel_sink, remote_log_level);
                SPDLOG_INFO("Enabling OTel logging sink with endpoint: {}", endpoint);
            }
        } else {
            SPDLOG_INFO("OpenTelemetry logging sink disabled via configuration");
        }

        // create the logger with all sinks
        auto logger = std::make_shared<spdlog::logger>("springtail",
                                                       std::begin(sinks), std::end(sinks));
        logger->set_pattern(pattern);
        _set_level(logger, log_level);
        logger->flush_on(spdlog::level::err);

        spdlog::set_default_logger(logger);
        spdlog::flush_every(std::chrono::seconds(3));

        absl::AddLogSink(&_spdlog_sink);
        _inited_flag = true;
    }

    void Logger::_internal_shutdown()
    {
        absl::FlushLogSinks();
        absl::RemoveLogSink(&_spdlog_sink);

        spdlog::shutdown();
    }

} // namespace springtail::logging
