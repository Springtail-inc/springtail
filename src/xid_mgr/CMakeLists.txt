find_package(Boost COMPONENTS program_options REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

# source files
set(XID_MGR_SOURCES
    xid_mgr_server.cc
    xid_mgr_service.cc
    pg_xact_log_writer.cc
    pg_xact_log_reader.cc
)

set(XID_MGR_CLIENT_SOURCES
    xid_mgr_subscriber.cc
    xid_mgr_client.cc
)

# add the library target
add_library(springtail_xid_mgr_client ${XID_MGR_CLIENT_SOURCES})
target_include_directories(springtail_xid_mgr_client
    PUBLIC
    ${CMAKE_BINARY_DIR}/src  # For generated protobuf files
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_xid_mgr_client
    PUBLIC
    springtail_common
    springtail_grpc
    springtail_redis
    PRIVATE
    proto_xid_manager
    nlohmann_json::nlohmann_json
    gRPC::grpc++
    protobuf::libprotobuf
)

# add the library target
add_library(springtail_xid_mgr ${XID_MGR_SOURCES})
target_include_directories(springtail_xid_mgr
    PUBLIC
    ${CMAKE_BINARY_DIR}/src  # For generated protobuf files
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_xid_mgr
    PUBLIC
    springtail_common
    springtail_grpc
    springtail_redis
    PRIVATE
    proto_xid_manager
    nlohmann_json::nlohmann_json
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(threaded_test test/threaded_test.cc)
target_include_directories(threaded_test PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(threaded_test
    PUBLIC springtail_common
           springtail_test
           springtail_xid_mgr
           springtail_xid_mgr_client
    PRIVATE Boost::program_options
            GTest::gtest_main
)
add_dsymutil_command(threaded_test)

add_executable(xid_client test/xid_client.cc)
target_include_directories(xid_client PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(xid_client
    PUBLIC springtail_common
           springtail_xid_mgr_client
    PRIVATE Boost::program_options
)
add_dsymutil_command(xid_client)

add_executable(dump_xact_log test/dump_xact_log.cc)
target_link_libraries(dump_xact_log
    PUBLIC springtail_xid_mgr
           springtail_common)
target_include_directories(dump_xact_log PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(dump_xact_log)

add_executable(roll_back_xact_log test/roll_back_xact_log.cc)
target_link_libraries(roll_back_xact_log
    PUBLIC springtail_xid_mgr
           springtail_common
    PRIVATE Boost::program_options
    )
target_include_directories(roll_back_xact_log PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(roll_back_xact_log)

add_executable(test_xid_mgr_xact_log test/test_xact_log.cc)
target_link_libraries(test_xid_mgr_xact_log
    PUBLIC springtail_xid_mgr
           springtail_common
    PRIVATE GTest::gtest_main)
target_include_directories(test_xid_mgr_xact_log PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(test_xid_mgr_xact_log)

add_executable(test_ssl test/test_ssl.cc)
target_include_directories(test_ssl
    PUBLIC
    ${CMAKE_BINARY_DIR}/src # for generated protobuf files
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH}
)
target_link_libraries(test_ssl
    PUBLIC
    springtail_common
    springtail_test
    springtail_xid_mgr
    springtail_xid_mgr_client
    PRIVATE
    Boost::program_options
    GTest::gtest_main
)
add_dsymutil_command(test_ssl)

# gtests
gtest_discover_tests(test_xid_mgr_xact_log)
gtest_discover_tests(threaded_test)
gtest_discover_tests(test_ssl)

# install utilities to install directory
install(TARGETS
    xid_client
    dump_xact_log
    roll_back_xact_log
    RUNTIME DESTINATION bin/utils)
