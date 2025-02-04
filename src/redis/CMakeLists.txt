find_package(redis++ CONFIG REQUIRED)

set(REDIS_LIB_SOURCES
  redis_ddl.cc
)

add_library(springtail_redis ${REDIS_LIB_SOURCES})
target_include_directories(springtail_redis PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_redis
    PUBLIC springtail_common
           spdlog::spdlog
           fmt::fmt
           redis++::redis++_static
           nlohmann_json::nlohmann_json
)

add_executable(test_redis test/test_redis.cc)
target_include_directories(test_redis PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_redis PRIVATE springtail_common Boost::thread GTest::gtest_main)
add_dsymutil_command(test_redis)

add_executable(test_pubsub_thread test/test_pubsub_thread.cc)
target_include_directories(test_pubsub_thread PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_pubsub_thread PRIVATE springtail_common Boost::thread GTest::gtest_main)
add_dsymutil_command(test_pubsub_thread)

gtest_discover_tests(test_redis)
gtest_discover_tests(test_pubsub_thread)
