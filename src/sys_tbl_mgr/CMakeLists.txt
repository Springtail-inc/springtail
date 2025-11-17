find_package(Boost COMPONENTS program_options REQUIRED)
find_package(Boost COMPONENTS interprocess REQUIRED)
find_package(Boost COMPONENTS multi_index REQUIRED)

# source files
set(SYS_TBL_MGR_SOURCES
    server.cc
    service.cc
    schema_cache.cc
    schema_helpers.cc
    system_tables.cc
    system_table_mgr.cc
    system_table_mgr_server.cc
    system_mutable_table.cc
    table_mgr_base.cc
    table_mgr.cc
    table.cc
    mutable_table.cc
)

set(SYS_TBL_MGR_CLIENT_SOURCES
    client.cc
    schema_cache.cc
    schema_helpers.cc
    shm_cache.cc
    system_tables.cc
    system_table_mgr.cc
    system_table_mgr_client.cc
    table.cc
    table_mgr_base.cc
    table_mgr_client.cc
)

set(SCH_MEM_CACHE_SOURCES
    shm_cache.cc
)

# add the library target
add_library(springtail_sys_tbl_mgr ${SYS_TBL_MGR_SOURCES})
target_include_directories(springtail_sys_tbl_mgr PUBLIC ${CMAKE_BINARY_DIR}/src PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_sys_tbl_mgr
  PUBLIC springtail_common springtail_grpc springtail_storage springtail_xid_mgr
  PRIVATE proto_sys_tbl_mgr
  PRIVATE Boost::thread)

# add the library target
add_library(springtail_sys_tbl_mgr_client ${SYS_TBL_MGR_CLIENT_SOURCES})
target_include_directories(springtail_sys_tbl_mgr_client PUBLIC ${CMAKE_BINARY_DIR}/src PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_sys_tbl_mgr_client
  PUBLIC springtail_common springtail_grpc springtail_storage
  PRIVATE proto_sys_tbl_mgr
  PRIVATE Boost::thread)

# add the library target
add_library(springtail_sch_mem_cache ${SCH_MEM_CACHE_SOURCES})
target_include_directories(springtail_sch_mem_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_sch_mem_cache
  PUBLIC springtail_common
  PRIVATE Boost::thread)

# file system check utility
add_executable(file_system_check test/file_system_check.cc)
target_include_directories(file_system_check PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(file_system_check
  PUBLIC springtail_test_check
  PRIVATE Boost::program_options
)
add_dsymutil_command(file_system_check)

## unit tests
springtail_unit_test(test_sys_tbl_mgr
  SOURCES test/test_sys_tbl_mgr.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_test GTest::gtest_main
)

springtail_unit_test(test_schema_cache
  SOURCES test/test_schema_cache.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_test GTest::gtest_main
)

springtail_unit_test(test_shm_cache
  SOURCES test/test_shm_cache.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_test springtail_sch_mem_cache GTest::gtest_main
)

# install file system validation utility to install directory
install(TARGETS file_system_check RUNTIME DESTINATION bin/system)
