#pragma once

#pragma push_macro("DELETE")
#include <admin_http/admin_server.hh>
#pragma pop_macro("DELETE")

#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/coordinator.hh>

namespace springtail {

class ServiceRunner {
public:
    explicit ServiceRunner(const std::string &name) : _name(name) {}
    virtual ~ServiceRunner() = default;
    virtual bool start()
    {
        LOG_INFO("Default start for service {}", _name);
        return true;
    }
    virtual void stop() {
        LOG_INFO("Default stop for service {}", _name);
    }
    const std::string &get_name() const { return _name; };

private:
    std::string _name;
};

/**
 * @brief Initialize the springtail system
 * @param runners list of additional services to start
 * @param load_redis flag for loading redis
 * @param log_filename log filename override
 * @param daemon_pid if set, daemonize the process and store the pid in the provided file
 * @param logging_mask logging mask override
 */

void springtail_init(const bool load_redis = false,
                     const std::optional<std::string> &log_filename = std::nullopt,
                     const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_daemon(const std::optional<std::string> &log_filename = std::nullopt,
                            const std::optional<std::string> &daemon_pid = std::nullopt,
                            const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_test(const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_custom(std::vector<std::unique_ptr<ServiceRunner>> &runners);

void springtail_daemon_run();

void springtail_store_argument_internal(ServiceId service_id, const std::string &arg_name, const std::any &value);
std::optional<std::any> springtail_retreive_argument_internal(ServiceId service_id, const std::string &arg_name, bool is_required=true);

template<typename T> void
springtail_store_argument(ServiceId service_id, const std::string &arg_name, const T &value)
{
    springtail_store_argument_internal(service_id, arg_name, std::any(value));
}

template<typename T> std::optional<T>
springtail_retreive_argument(ServiceId service_id, const std::string &arg_name, bool is_required=true)
{
    auto ret_opt = springtail_retreive_argument_internal(service_id, arg_name, is_required);
    if (!ret_opt) {
        return std::nullopt;
    }
    auto ret = ret_opt.value();
    CHECK(ret.type() == typeid(T));
    return std::any_cast<T>(ret);
}

inline void
springtail_store_arguments(ServiceId service_id, const std::map<std::string, std::any> &args_list)
{
    for (auto &it: args_list) {
        springtail_store_argument_internal(service_id, it.first, it.second);
    }
}

/**
 * @brief Services shutdown function
 *
 */
void springtail_shutdown();

class AdminServerRunner : public ServiceRunner {
public:
    AdminServerRunner() : ServiceRunner("AdminService") {}
    bool start() override
    {
        AdminServer::get_instance();
        return true;
    }
    void stop() override
    {
        AdminServer::shutdown();
    }
};

/**
 * Intialize the exception and backtrace handling.
 */
class ExceptionRunner : public ServiceRunner {
public:
    ExceptionRunner();

    bool start() override;

    void stop() override;
private:
    std::vector<int> _signals;
};

// Daemon init
class DaemonRunner : public ServiceRunner {
public:
    explicit DaemonRunner(const std::string &daemon_pid)
        : ServiceRunner("Daemon"), _daemon_pid(daemon_pid) {}

    bool start() override;

private:
    const std::string _daemon_pid;
};

// Properties Init
class PropertiesRunner : public ServiceRunner {
public:
    explicit PropertiesRunner(bool load_redis) : ServiceRunner("Properties"), _load_redis(load_redis) {}

    bool start() override
    {
        Properties::get_instance()->init(_load_redis);
        return true;
    }

    void stop() override { Properties::shutdown(); }

private:
    bool _load_redis;
};

// Properties cache inint
class PropertiesCacheRunner : public ServiceRunner {
public:
    PropertiesCacheRunner() : ServiceRunner("PropertiesCache") {}

    bool start() override
    {
        Properties::get_instance()->init_cache();
        return true;
    }
};

// Logging init
class LoggingRunner : public ServiceRunner {
public:
    LoggingRunner(const std::optional<std::string> &log_filename,
                  const std::optional<std::string> &daemon_pid,
                  const std::optional<uint32_t> &logging_mask)
        : ServiceRunner("Logging"),
          _log_filename(log_filename),
          _daemon_pid(daemon_pid),
          _logging_mask(logging_mask) {}

    bool start() override
    {
        logging::Logger::get_instance()->init(_logging_mask, _log_filename, _daemon_pid.has_value());
        return true;
    }

private:
    const std::optional<std::string> &_log_filename;
    const std::optional<std::string> &_daemon_pid;
    const std::optional<uint32_t> &_logging_mask;
};

class DefaultLoggingRunner : public ServiceRunner {
public:
    DefaultLoggingRunner() : ServiceRunner("Default Logging") {}

    void stop() override
    {
        open_telemetry::OpenTelemetry::shutdown_instance();
        logging::Logger::get_instance()->shutdown();
    }
};

// Open Telemetry init
class OpenTelemetryRunner : public ServiceRunner {
public:
    explicit OpenTelemetryRunner(const std::optional<std::string> &name) : ServiceRunner("Tracing"), _name(name) {}

    bool start() override
    {
        open_telemetry::OpenTelemetry::get_instance()->init(_name.value_or(""));
        return true;
    }

private:
    const std::optional<std::string> &_name;
};

// RedisMgr init
class RedisMgrRunner : public ServiceRunner {
public:
    RedisMgrRunner() : ServiceRunner("RedisMgr") {}

    bool start() override
    {
        RedisMgr::get_instance();
        return true;
    }

    void stop() override { RedisMgr::shutdown(); }
};

class CoordinatorRunner : public ServiceRunner {
public:
    CoordinatorRunner() : ServiceRunner("Coordinator") {}

    bool start() override
    {
        Coordinator::get_instance()->start_thread();
        return true;
    }

    void stop() override
    {
        // Coordinator::get_instance()->stop_thread();
        Coordinator::shutdown();
    }
};

};  // namespace springtail
