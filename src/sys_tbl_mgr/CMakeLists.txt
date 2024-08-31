find_package(Boost COMPONENTS program_options REQUIRED)

# source files
set(SYS_TBL_MGR_SOURCES
    client.cc
    server.cc
    service.cc
)

# add the library target
add_library(springtail_sys_tbl_mgr ${SYS_TBL_MGR_SOURCES})
target_include_directories(springtail_sys_tbl_mgr PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_sys_tbl_mgr
  PUBLIC springtail_common springtail_storage springtail_xid_mgr
  PRIVATE thrift_sys_tbl_mgr
  PRIVATE Boost::thread)

# daemon executable
add_executable(sys_tbl_mgr_daemon daemon.cc)
target_include_directories(sys_tbl_mgr_daemon PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(sys_tbl_mgr_daemon
  PUBLIC springtail_sys_tbl_mgr
  PRIVATE Boost::program_options
)
add_dsymutil_command(sys_tbl_mgr_daemon)


# tests
add_executable(test_sys_tbl_mgr test/test_sys_tbl_mgr.cc)
target_include_directories(test_sys_tbl_mgr PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_sys_tbl_mgr
  PUBLIC springtail_sys_tbl_mgr springtail_test
  PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_sys_tbl_mgr)


# hook tests into gtest framework
gtest_discover_tests(test_sys_tbl_mgr)
