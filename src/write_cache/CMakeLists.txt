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

## unit tests
springtail_unit_test(test_wc_index
  SOURCES test/test_wc_index.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_write_cache GTest::gtest_main
)
