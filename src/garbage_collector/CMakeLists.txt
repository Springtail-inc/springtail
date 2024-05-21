# source files
set(GC_SOURCES gc_log_parser.cc gc_extent_mapper.cc)

add_library(springtail_gc ${GC_SOURCES})
target_include_directories(springtail_gc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_gc PRIVATE thrift_write_cache Boost::thread)

# ExtentMapper test
add_executable(test_extent_mapper test/test_extent_mapper.cc)
target_include_directories(test_extent_mapper PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_extent_mapper
  PUBLIC springtail_common springtail_gc springtail_write_cache springtail_pglog springtail_storage
  PRIVATE GTest::gtest_main
)

gtest_discover_tests(test_extent_mapper)
