## library of functions using in testing
set(TEST_SOURCES services.cc ddl_helpers.cc)

find_package(absl REQUIRED)

add_library(springtail_test ${TEST_SOURCES})
target_include_directories(springtail_test
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_test
  PUBLIC springtail_xid_mgr
         springtail_xid_mgr_client
         springtail_sys_tbl_mgr
         springtail_sys_tbl_mgr_client
         springtail_write_cache)

set(TEST_CHECK_SOURCES file_system_check.cc)

add_library(springtail_test_check ${TEST_CHECK_SOURCES})
target_include_directories(springtail_test_check
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_test_check
  PUBLIC springtail_storage springtail_sys_tbl_mgr
)

