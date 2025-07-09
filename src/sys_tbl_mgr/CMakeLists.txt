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

# file system validate utility
add_executable(validate_file_system test/validate_file_system.cc)
target_include_directories(validate_file_system PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(validate_file_system
  PUBLIC springtail_sys_tbl_mgr
)
add_dsymutil_command(validate_file_system)

## unit tests
springtail_unit_test(test_sys_tbl_mgr
  SOURCES test/test_sys_tbl_mgr.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_sys_tbl_mgr springtail_test GTest::gtest_main
)

springtail_unit_test(test_schema_cache
  SOURCES test/test_schema_cache.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_test GTest::gtest_main
)

springtail_unit_test(test_shm_cache
  SOURCES test/test_shm_cache.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_sys_tbl_mgr springtail_test GTest::gtest_main
)

# install daemon to install directory
install(TARGETS sys_tbl_mgr_daemon RUNTIME DESTINATION bin/system)
# install file system validation utility to install directory
install(TARGETS validate_file_system RUNTIME DESTINATION bin/system)
