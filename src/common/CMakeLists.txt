# Common library
find_package(redis++ CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(c-ares CONFIG REQUIRED)

set(COMMON_LIB_SOURCES
  exception.cc
  logging.cc
  common.cc
  properties.cc
  redis.cc
  redis_ddl.cc
  tracking_allocator.cc
  dns_resolver.cc
)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common
  PUBLIC cpptrace::cpptrace
         spdlog::spdlog
         fmt::fmt
         redis++::redis++_static
	       nlohmann_json::nlohmann_json
         c-ares::cares
)

add_executable(test_thread test/test_thread.cc)
target_include_directories(test_thread PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_thread PRIVATE springtail_common GTest::gtest_main)
add_dsymutil_command(test_thread)

add_executable(test_queue test/test_queue.cc)
target_include_directories(test_queue PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_queue PRIVATE springtail_common GTest::gtest_main)
add_dsymutil_command(test_queue)

add_executable(test_common test/test_common.cc)
target_include_directories(test_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_common PRIVATE springtail_common GTest::gtest_main)
add_dsymutil_command(test_common)

add_executable(test_filesystem test/test_filesystem.cc)
target_include_directories(test_filesystem PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_filesystem PRIVATE springtail_common GTest::gtest_main)
add_dsymutil_command(test_filesystem)

add_executable(test_counter test/test_counter.cc)
target_include_directories(test_counter PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_counter PRIVATE springtail_common Boost::thread GTest::gtest_main)
add_dsymutil_command(test_counter)

add_executable(test_redis test/test_redis.cc)
target_include_directories(test_redis PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_redis PRIVATE springtail_common Boost::thread GTest::gtest_main)
add_dsymutil_command(test_redis)

add_executable(test_dns test/test_dns.cc)
target_include_directories(test_dns PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_dns PRIVATE springtail_common Boost::thread GTest::gtest_main)
add_dsymutil_command(test_dns)

add_executable(main test/main.cc)
target_include_directories(main PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(main PRIVATE springtail_common Boost::thread)
add_dsymutil_command(main)

gtest_discover_tests(test_thread)
gtest_discover_tests(test_queue)
gtest_discover_tests(test_common)
gtest_discover_tests(test_filesystem)
gtest_discover_tests(test_counter)
gtest_discover_tests(test_redis)
gtest_discover_tests(test_dns)
