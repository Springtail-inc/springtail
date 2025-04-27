find_package(Boost COMPONENTS program_options REQUIRED)
find_package(Boost COMPONENTS interprocess REQUIRED)
find_package(Boost COMPONENTS multi_index REQUIRED)

# source files
set(SYS_TBL_MGR_SOURCES
    client.cc
    server.cc
    service.cc
    schema_cache.cc
    schema_mgr.cc
    system_tables.cc
    table.cc
    table_mgr.cc
    shm_cache.cc
)

# add the library target
add_library(springtail_sys_tbl_mgr ${SYS_TBL_MGR_SOURCES})
target_include_directories(springtail_sys_tbl_mgr PUBLIC ${CMAKE_BINARY_DIR}/src PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_sys_tbl_mgr
  PUBLIC springtail_common springtail_grpc springtail_storage springtail_xid_mgr_client
  PRIVATE proto_sys_tbl_mgr
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

add_executable(test_schema_cache test/test_schema_cache.cc)
target_include_directories(test_schema_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_schema_cache
  PUBLIC springtail_test
  PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_schema_cache)

add_executable(test_shm_cache test/test_shm_cache.cc)
target_include_directories(test_shm_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_shm_cache
  PUBLIC springtail_sys_tbl_mgr springtail_test
  PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_shm_cache)

# hook tests into gtest framework
gtest_discover_tests(test_sys_tbl_mgr)
gtest_discover_tests(test_schema_cache)
gtest_discover_tests(test_shm_cache)

# install daemon to install directory
install(TARGETS sys_tbl_mgr_daemon RUNTIME DESTINATION bin/system)
