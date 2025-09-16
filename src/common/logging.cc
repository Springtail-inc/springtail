#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/basic_file_sink.h>

#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>

#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

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

    spdlog::level::level_enum
    Logger::get_log_level_from_string(const std::string &level_str)
    {
        return spdlog::level::from_str(level_str);
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
        set_debug_level(Json::get_or<uint32_t>(props, "log_level_debug", 1));
        std::string pattern = Json::get_or<std::string>(props, "log_pattern", "[%Y-%m-%d %T.%e %z] [%^%l%$] [%s:%#:%!] [thread %t] %v");
        bool log_rotation_enabled = Json::get_or<bool>(props, "log_rotation_enabled", true);
        spdlog::level::level_enum log_level_value = get_log_level_from_string(log_level);

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
            console_sink->set_level(log_level_value);
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
            file_sink->set_level(log_level_value);
            sinks.push_back(file_sink);
        } else {
            auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path_str);
            file_sink->set_level(log_level_value);
            sinks.push_back(file_sink);
        }

        // create the logger with all sinks
        auto logger = std::make_shared<spdlog::logger>("springtail",
                                                       std::begin(sinks), std::end(sinks));
        logger->set_pattern(pattern);
        logger->set_level(log_level_value);
        logger->flush_on(spdlog::level::err);

        spdlog::set_default_logger(logger);
        spdlog::flush_every(std::chrono::seconds(3));

        absl::AddLogSink(&_spdlog_sink);
        _inited_flag = true;
    }

    void
    Logger::_internal_shutdown()
    {
        absl::FlushLogSinks();
        absl::RemoveLogSink(&_spdlog_sink);

        spdlog::shutdown();
    }

    nlohmann::json
    Logger::get_stats()
    {
        std::string log_name = spdlog::default_logger()->name();
        spdlog::level::level_enum log_level = spdlog::default_logger()->level();
        spdlog::level::level_enum flush_level =spdlog::default_logger()->flush_level();
        auto str_log_level = spdlog::level::to_string_view(log_level);
        auto str_flush_level = spdlog::level::to_string_view(flush_level);

        nlohmann::json stats = {
            {"name", log_name},
            {"log_level", std::string(str_log_level.data(), str_log_level.size())},
            {"flush_level", std::string(str_flush_level.data(), str_flush_level.size())},
            {"debug_level", _debug_log_level.load(std::memory_order_relaxed)},
            {"module_mask", {}}
        };
        for (auto &[name, mask]: _log_module_map) {
            if ((mask & _log_mask.load()) == mask) {
                stats["module_mask"][name] = true;
            } else {
                stats["module_mask"][name] = false;
            }
        }
        return stats;
    }

    bool
    Logger::set_log_mask(const std::string &mask_name, bool value)
    {
        auto it = _log_module_map.find(mask_name);
        if (it == _log_module_map.end()) {
            return true;
        }
        uint32_t mask = it->second;
        if (value) {
            _log_mask |= mask;
        } else {
            _log_mask &= (~mask);
        }
        return true;
    }
} // namespace springtail::logging
