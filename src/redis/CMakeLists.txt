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
           redis++::redis++
           nlohmann_json::nlohmann_json
)

springtail_unit_test(test_redis
  SOURCES test/test_redis.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)

springtail_unit_test(test_pubsub_thread
  SOURCES test/test_pubsub_thread.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_common Boost::thread GTest::gtest_main
)
