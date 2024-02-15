# Common library
find_package(redis++ CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

set(COMMON_LIB_SOURCES exception.cc logging.cc common.cc properties.cc redis.cc tracking_allocator.cc)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common PUBLIC Backward::Backward spdlog::spdlog fmt::fmt redis++::redis++_static)

add_executable(test_thread test/test_thread.cc)
target_include_directories(test_thread PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_thread PRIVATE springtail_common GTest::gtest_main)

add_executable(test_queue test/test_queue.cc)
target_include_directories(test_queue PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_queue PRIVATE springtail_common GTest::gtest_main)

add_executable(test_common test/test_common.cc)
target_include_directories(test_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_common PRIVATE springtail_common GTest::gtest_main)

gtest_discover_tests(test_thread)
gtest_discover_tests(test_queue)
gtest_discover_tests(test_common)
