find_package(Boost COMPONENTS program_options REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# source files
set(XID_MGR_SOURCES
    xid_mgr_client.cc
    xid_mgr_server.cc
    xid_mgr_service.cc
)

# add the library target
add_library(springtail_xid_mgr ${XID_MGR_SOURCES})
target_include_directories(springtail_xid_mgr PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_xid_mgr
    PUBLIC springtail_common
    PRIVATE thrift_xid_mgr nlohmann_json::nlohmann_json
)

add_executable(xid_mgr_daemon xid_mgr_daemon.cc)
target_include_directories(xid_mgr_daemon PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(xid_mgr_daemon
    PUBLIC springtail_common
           springtail_xid_mgr
    PRIVATE Boost::program_options
)
add_dsymutil_command(xid_mgr_daemon)

add_executable(threaded_test test/threaded_test.cc)
target_include_directories(threaded_test PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(threaded_test
    PUBLIC springtail_common
           springtail_xid_mgr
    PRIVATE Boost::program_options
            GTest::gtest_main
)
add_dsymutil_command(threaded_test)

# hook tests into gtest framework
gtest_discover_tests(threaded_test)
