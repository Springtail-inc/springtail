# Common library
find_package(redis++ CONFIG REQUIRED)

set(COMMON_LIB_SOURCES exception.cc logging.cc common.cc properties.cc redis.cc thread_pool.cc)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common PUBLIC Backward::Backward spdlog::spdlog fmt::fmt redis++::redis++_static)

add_executable(redis_test redis.cc)
target_include_directories(redis_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(redis_test PRIVATE springtail_common)

add_executable(thread_test thread_pool.cc)
target_include_directories(thread_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(thread_test PRIVATE springtail_common)