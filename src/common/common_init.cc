#include <common/common_init.hh>

using namespace springtail;
namespace springtail::init {
void
springtail_init(const std::vector<ServiceRunner *> &runners,
                const std::optional<std::string> &log_filename,
                const std::optional<std::string> &daemon_pid,
                const std::optional<uint32_t> &logging_mask)
{
    std::vector<ServiceRunner *> service_runners = {
        new DefaultLoggingRunner(),
        new ExceptionRunner(),
        new PropertiesRunner(!daemon_pid.has_value()),
        new DaemonRunner(daemon_pid),
        new LoggingRunner(log_filename, daemon_pid, logging_mask),
        new TracingRunner(log_filename),
        new PropertiesCacheRunner()};

    service_runners.insert(service_runners.end(), runners.begin(), runners.end());
    if (!ServiceRegister::get_instance()->start(service_runners)) {
        exit(1);
    }
}

void
springtail_shutdown()
{
    ServiceRegister::shutdown();
}

};  // namespace springtail::init
