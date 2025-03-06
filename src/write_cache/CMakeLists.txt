find_package(Boost COMPONENTS program_options REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

# source files
set(WRITE_CACHE_SOURCES
    write_cache_client.cc
    write_cache_server.cc
    write_cache_service.cc
    write_cache_index.cc
    write_cache_table_set.cc
    write_cache_index_node.cc
    extent_mapper.cc
)

# add the library target
add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
target_include_directories(springtail_write_cache
    PUBLIC
    ${CMAKE_BINARY_DIR}/src  # For generated protobuf files
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_write_cache
    PUBLIC
    springtail_common
    springtail_grpc
    proto_write_cache
    PRIVATE
    nlohmann_json::nlohmann_json
    gRPC::grpc++
    protobuf::libprotobuf
    Boost::thread
)

## tests
add_executable(test_wc_index test/test_wc_index.cc)
target_include_directories(test_wc_index PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_wc_index
  PUBLIC springtail_write_cache
  PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_wc_index)

add_executable(test_extent_mapper test/test_extent_mapper.cc)
target_include_directories(test_extent_mapper PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_extent_mapper
  PUBLIC springtail_write_cache
  PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_extent_mapper)

# hook tests into gtest framework
gtest_discover_tests(test_extent_mapper)
gtest_discover_tests(test_wc_index)
