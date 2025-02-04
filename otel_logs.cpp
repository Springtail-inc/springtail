// #include <opentelemetry/context/runtime_context.h>
// #include <opentelemetry/context/threadlocal_context.h>
// #include <opentelemetry/logs/provider.h>
// #include <opentelemetry/logs/logger.h>
// #include <iostream>

// namespace context = opentelemetry::context;
// namespace logs = opentelemetry::logs;

// // Key types for metadata
// struct XidKey {};
// struct DatabaseIdKey {};

// // Get a global logger instance
// std::shared_ptr<logs::Logger> GetLogger()
// {
//     static auto provider = logs::Provider::GetLoggerProvider();
//     return provider->GetLogger("request_logger");
// }

// // Function to process a request in a new thread
// void ProcessRequest(int xid, int database_id)
// {
//     auto logger = GetLogger();

//     // Create a new context for this request
//     context::Context request_context =
//         context::RuntimeContext::GetCurrent()
//             .SetValue<XidKey>(xid)
//             .SetValue<DatabaseIdKey>(database_id);

//     // Attach the request context to this thread
//     context::Token token = context::RuntimeContext::Attach(request_context);

//     // Fetch metadata from context
//     int ctx_xid = context::RuntimeContext::GetValue<XidKey>().value_or(-1);
//     int ctx_db_id = context::RuntimeContext::GetValue<DatabaseIdKey>().value_or(-1);

//     // Log with metadata
//     logger->EmitLogRecord(logs::Severity::kInfo,
//                           "Processing request",
//                           {{"xid", ctx_xid}, {"database_id", ctx_db_id}});

//     std::cout << "Logged request with xid=" << ctx_xid << ", database_id=" << ctx_db_id << std::endl;
// }

// int main()
// {
//     std::thread t1(ProcessRequest, 1001, 42);
//     std::thread t2(ProcessRequest, 1002, 99);
    
//     t1.join();
//     t2.join();

//     return 0;
// }


// #include <fmt/core.h>
// #include <opentelemetry/context/runtime_context.h>
// #include <opentelemetry/logs/provider.h>
// #include <opentelemetry/logs/logger.h>
// #include <iostream>

// namespace context = opentelemetry::context;
// namespace logs = opentelemetry::logs;
// namespace otel = opentelemetry;

// // Key types for metadata
// struct XidKey {};
// struct DatabaseIdKey {};

// // Get a global logger instance
// opentelemetry::nostd::shared_ptr<logs::Logger> GetLogger()
// {
//     static auto provider = opentelemetry::logs::Provider::GetLoggerProvider();
//     return provider->GetLogger("spdlog-otel");
// }

// // Function to process a request in a new thread
// void ProcessRequest(int xid, int database_id)
// {
//     auto logger = GetLogger();

//     // Create a new context for this request
//     context::Context request_context =
//         context::RuntimeContext::GetCurrent()
//             .SetValue("xid", xid)
//             .SetValue("database_id", database_id);

//     // Attach the request context to this thread
//     context::RuntimeContext::Attach(request_context);

//     // Fetch metadata from context
//     auto ctx_xid = context::RuntimeContext::GetValue("xid", &request_context);
//     auto ctx_db_id = context::RuntimeContext::GetValue("database_id", &request_context);

//     opentelemetry::nostd::string_view msg = fmt::format("Processing request");

//     std::unique_ptr<opentelemetry::logs::LogRecord> record = logger->CreateLogRecord();
//     record->SetBody("Processing message");
//     record->SetSeverity(logs::Severity::kInfo);
//     logger->Log(logs::Severity::kInfo, msg);
//     logger->EmitLogRecord(record);

//     std::cout << "Logged request with xid=" << std::endl;
// }

// int main()
// {
//     std::thread t1(ProcessRequest, 1001, 42);
//     std::thread t2(ProcessRequest, 1002, 99);

//     t1.join();
//     t2.join();

//     return 0;
// }

// #include <fmt/core.h>
// #include <opentelemetry/context/runtime_context.h>
// #include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
// #include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h>
// #include <opentelemetry/logs/provider.h>
// #include <opentelemetry/logs/logger.h>
// #include <opentelemetry/sdk/logs/exporter.h>
// #include <opentelemetry/sdk/logs/processor.h>
// #include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>
// #include <iostream>

// #include "opentelemetry/sdk/logs/simple_log_record_processor.h"
// #include "opentelemetry/sdk/logs/logger_provider.h"
// #include "opentelemetry/logs/provider.h"

// #include "opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter.h"
// #include "opentelemetry/sdk/version/version.h"

// namespace logs_sdk = opentelemetry::sdk::logs;
// namespace otlp      = opentelemetry::exporter::otlp;

// otlp::OtlpHttpLogRecordExporterOptions logger_opts;
// int main()
// {
//   auto exporter = std::unique_ptr<logs_sdk::LogRecordExporter>();
//   auto processor = std::unique_ptr<logs_sdk::LogRecordProcessor>(
//       new logs_sdk::SimpleLogRecordProcessor(std::move(exporter)));
//   auto provider =
//       std::shared_ptr<logs_sdk::LoggerProvider>(new
//       logs_sdk::LoggerProvider(std::move(processor)));

//   // Get Logger
//   auto logger = provider->GetLogger("firstlog", "", OPENTELEMETRY_SDK_VERSION);
//   logger->Debug("I am the first log message.");
// }

#include <fmt/core.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/exporters/ostream/log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_log_record_exporter_options.h>
#include <opentelemetry/logs/logger.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/sdk/logs/exporter.h>
#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/processor.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>

namespace logs_api      = opentelemetry::logs;
namespace logs_sdk      = opentelemetry::sdk::logs;
namespace logs_exporter = opentelemetry::exporter::logs;

void InitLogger()
{
  // Create ostream log exporter instance
  auto exporter =
      std::unique_ptr<logs_sdk::LogRecordExporter>(new logs_exporter::OStreamLogRecordExporter);
  auto processor = logs_sdk::SimpleLogRecordProcessorFactory::Create(std::move(exporter));

  std::shared_ptr<opentelemetry::sdk::logs::LoggerProvider> sdk_provider(
      logs_sdk::LoggerProviderFactory::Create(std::move(processor)));

  // Set the global logger provider
  const std::shared_ptr<logs_api::LoggerProvider> &api_provider = sdk_provider;
  logs_api::Provider::SetLoggerProvider(api_provider);
}


int
main(int /* argc */, char ** /* argv */)
{
    InitLogger();
    
    auto provider = opentelemetry::logs::Provider::GetLoggerProvider();
    auto logger = provider->GetLogger("firstlog", "", OPENTELEMETRY_SDK_VERSION);
    return 0;
}