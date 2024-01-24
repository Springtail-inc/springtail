# Common library
find_package(redis++ CONFIG REQUIRED)

set(COMMON_LIB_SOURCES exception.cc logging.cc common.cc properties.cc redis.cc tracking_allocator.cc)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common PUBLIC Backward::Backward spdlog::spdlog fmt::fmt redis++::redis++_static)

add_executable(test_threadpool test/thread_test.cc)
target_include_directories(test_threadpool PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_threadpool PRIVATE springtail_common)
