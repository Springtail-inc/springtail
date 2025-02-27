#pragma once

#include <common/exception.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/service_register.hh>
#include <common/tracing.hh>

namespace springtail {

void
daemonize(const std::string &pid_file);

/**
 * @brief Initialize the springtail system
 * @param runners list of additional services to start
 * @param log_filename log filename override
 * @param daemon_pid if set, daemonize the process and store the pid in the provided file
 * @param logging_mask logging mask override
 */

void springtail_init(const std::vector<ServiceRunner *> &runners = std::vector<ServiceRunner *>(),
                     const bool load_redis = true,
                     const std::optional<std::string> &log_filename = std::nullopt,
                     const std::optional<std::string> &daemon_pid = std::nullopt,
                     const std::optional<uint32_t> &logging_mask = std::nullopt);


/**
 * @brief Services shutdown function
 *
 */
void springtail_shutdown();

// Daemon init
class DaemonRunner : public ServiceRunner {
public:
    explicit DaemonRunner(const std::optional<std::string> &daemon_pid)
        : ServiceRunner("Daemon"), _daemon_pid(daemon_pid)
    {
    }

    bool start() override
    {
        if (_daemon_pid.has_value()) {
            daemonize(*_daemon_pid);
        }
        return true;
    }

private:
    const std::optional<std::string> &_daemon_pid;
};

// Exception init
class ExceptionRunner : public ServiceRunner {
public:
    explicit ExceptionRunner() : ServiceRunner("Exception") {}

    bool start() override
    {
        init_exception();
        return true;
    }

    void stop() override { shutdown_exception(); }
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
    explicit PropertiesCacheRunner() : ServiceRunner("PropertiesCache") {}

    bool start() override
    {
        Properties::get_instance()->init_cache();
        return true;
    }
};

// Logging init
class LoggingRunner : public ServiceRunner {
public:
    explicit LoggingRunner(const std::optional<std::string> &log_filename,
                  const std::optional<std::string> &daemon_pid,
                  const std::optional<uint32_t> &logging_mask)
        : ServiceRunner("Logging"),
          _log_filename(log_filename),
          _daemon_pid(daemon_pid),
          _logging_mask(logging_mask) {}

    bool start() override
    {
        init_logging(_logging_mask, _log_filename, _daemon_pid.has_value());
        return true;
    }

private:
    const std::optional<std::string> &_log_filename;
    const std::optional<std::string> &_daemon_pid;
    const std::optional<uint32_t> &_logging_mask;
};

class DefaultLoggingRunner : public ServiceRunner {
public:
    explicit DefaultLoggingRunner() : ServiceRunner("Default Logging") {}

    void stop() override { shutdown_logging(); }

private:
};

// Tracing init
class TracingRunner : public ServiceRunner {
public:
    explicit TracingRunner(const std::optional<std::string> &name) : ServiceRunner("Tracing"), _name(name) {}

    bool start() override
    {
        tracing::TracingAndMetrics::get_instance()->init(_name.value_or(""));
        return true;
    }

    void stop() override { tracing::TracingAndMetrics::shutdown(); }

private:
    const std::optional<std::string> &_name;
};

// Termination signals handling init
class TermSignalRunner : public ServiceRunner {
public:
    explicit TermSignalRunner(void (*handler)(int))
        : ServiceRunner("TermSignal")
    {
        _handler = handler;
    }

    bool start() override
    {
        for (int sig : _signals) {
            std::signal(sig, _handler);
        }
        return true;
    }

    void stop() override
    {
        for (int sig : _signals) {
            std::signal(sig, SIG_DFL);
        }
    }

private:
    const std::vector<int> _signals{SIGINT, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2};
    void (*_handler)(int);
};

};  // namespace springtail::init
