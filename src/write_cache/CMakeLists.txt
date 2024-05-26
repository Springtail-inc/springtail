find_package(nlohmann_json CONFIG REQUIRED)

# source files
set(WRITE_CACHE_SOURCES
    write_cache_client.cc
    write_cache_server.cc write_cache_service.cc write_cache_index.cc write_cache_table_set.cc
    write_cache_index_node.cc
    extent_mapper.cc
)

# add the library target
add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
target_include_directories(springtail_write_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_write_cache PRIVATE thrift_write_cache nlohmann_json::nlohmann_json Boost::thread)

# daemon executable
add_executable(write_cache_daemon write_cache_daemon.cc)
target_include_directories(write_cache_daemon PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(write_cache_daemon
    PRIVATE springtail_write_cache
    PRIVATE springtail_common
)

## tests
add_executable(test_threaded test/write_cache_index_test.cc)
target_include_directories(test_threaded PRIVATE ${CMAKE_SOURCE_DIR}/include ${THRIFT_GEN_PATH})
target_link_libraries(test_threaded
    PRIVATE springtail_common springtail_write_cache
    GTest::gtest_main
)

add_executable(test_extent_mapper test/test_extent_mapper.cc)
target_include_directories(test_extent_mapper PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_extent_mapper
  PUBLIC springtail_write_cache springtail_common 
  PRIVATE GTest::gtest_main
)

gtest_discover_tests(test_threaded)
gtest_discover_tests(test_extent_mapper)
