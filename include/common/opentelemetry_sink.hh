#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/common/macros.h>
#include <opentelemetry/common/timestamp.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
#include <opentelemetry/version.h>
#include <iostream>

namespace otel = opentelemetry;

template<typename Mutex>
class OpenTelemetrySink : public spdlog::sinks::base_sink<Mutex>
{
public:
    OpenTelemetrySink()
    {
        auto provider = otel::logs::Provider::GetLoggerProvider();
        logger_ = provider->GetLogger("spdlog-otel");
        // int ctx_xid = -1;
        // int ctx_db_id = -1;

        // Log with metadata
        logger_->Log(otel::logs::Severity::kInfo,
                            "Processing request");

        std::cout << "Log sent from opentelemetry" << std::endl;
    }

protected:
    void sink_it_(const spdlog::details::log_msg &msg) override
    {
        
    }

    void flush_() override {}

private:
    otel::nostd::shared_ptr<otel::logs::Logger> logger_;
};

using OpenTelemetrySinkMt = OpenTelemetrySink<std::mutex>;