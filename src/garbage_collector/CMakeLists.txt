# source files
set(GC_SOURCES log_parser.cc committer.cc)

add_library(springtail_gc ${GC_SOURCES})
target_include_directories(springtail_gc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_gc PRIVATE thrift_write_cache thrift_xid_mgr Boost::thread)
