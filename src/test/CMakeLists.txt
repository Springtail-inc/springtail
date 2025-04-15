## library of functions using in testing
set(TEST_SOURCES services.cc)

find_package(absl REQUIRED)

add_library(springtail_test ${TEST_SOURCES})
target_include_directories(springtail_test
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_test
  PUBLIC springtail_xid_mgr springtail_xid_mgr_client springtail_sys_tbl_mgr springtail_write_cache)
