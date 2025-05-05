# Common library
find_package(redis++ CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(c-ares CONFIG REQUIRED)
find_package(opentelemetry-cpp CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS core secretsmanager)

set(COMMON_LIB_SOURCES
  coordinator.cc
  init.cc
  dns_resolver.cc
  exception.cc
  logging.cc
  open_telemetry.cc
  properties.cc
  redis.cc
  redis_cache.cc
  multi_queue_thread_manager.cc
  timestamp.cc
  time_trace.cc
  tracking_allocator.cc
  aws.cc
)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common
  PUBLIC absl::check
        cpptrace::cpptrace
        spdlog::spdlog
        fmt::fmt
        redis++::redis++
        nlohmann_json::nlohmann_json
        c-ares::cares
        opentelemetry-cpp::api
        opentelemetry-cpp::sdk
        opentelemetry-cpp::trace
        opentelemetry-cpp::metrics
        opentelemetry-cpp::logs
        opentelemetry-cpp::ostream_span_exporter
        opentelemetry-cpp::otlp_http_client
        opentelemetry-cpp::otlp_http_exporter
        opentelemetry-cpp::otlp_http_metric_exporter
        opentelemetry-cpp::otlp_http_log_record_exporter
        ${AWSSDK_LIBRARIES}
)

## helpers

add_executable(log_tester test/log_tester.cc)
target_include_directories(log_tester
     PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(log_tester PRIVATE springtail_common)
add_dsymutil_command(log_tester)

## unit tests
springtail_unit_test(test_thread
  SOURCES test/test_thread.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_queue
  SOURCES test/test_queue.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_common
  SOURCES test/test_common.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_filesystem
  SOURCES test/test_filesystem.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_counter
  SOURCES test/test_counter.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)

springtail_unit_test(test_dns
  SOURCES test/test_dns.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)

springtail_unit_test(test_synchronizer
  SOURCES test/test_synchronizer.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)

springtail_unit_test(test_exception
  SOURCES test/test_exception.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_coordinator
  SOURCES test/test_coordinator.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_redis_cache
  SOURCES test/test_redis_cache.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)

springtail_unit_test(test_thread_manager
  SOURCES test/test_thread_manager.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_timestamp
  SOURCES test/test_timestamp.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_time_trace
  SOURCES test/test_time_trace.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)
