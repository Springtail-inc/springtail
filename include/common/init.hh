#pragma once

#include <common/exception.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>
#include <common/service_register.hh>
#include <common/coordinator.hh>

namespace springtail {

/**
 * @brief Initialize the springtail system
 * @param runners list of additional services to start
 * @param load_redis flag for loading redis
 * @param log_filename log filename override
 * @param daemon_pid if set, daemonize the process and store the pid in the provided file
 * @param logging_mask logging mask override
 */

void springtail_init(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners = std::nullopt,
                     const bool load_redis = false,
                     const std::optional<std::string> &log_filename = std::nullopt,
                     const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_daemon(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners = std::nullopt,
                            const std::optional<std::string> &log_filename = std::nullopt,
                            const std::optional<std::string> &daemon_pid = std::nullopt,
                            const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_test(const std::optional<std::vector<std::unique_ptr<ServiceRunner>>> &runners = std::nullopt,
                          const std::optional<uint32_t> &logging_mask = std::nullopt);

void springtail_init_custom(std::vector<std::unique_ptr<ServiceRunner>> &runners);

void springtail_daemon_run();

/**
 * @brief Services shutdown function
 *
 */
void springtail_shutdown();

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

    void stop() override { logging::Logger::get_instance()->shutdown(); }
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

    void stop() override { open_telemetry::OpenTelemetry::shutdown(); }

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

};  // namespace springtail::init
