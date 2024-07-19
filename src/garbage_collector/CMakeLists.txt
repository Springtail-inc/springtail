find_package(Boost COMPONENTS program_options REQUIRED)

# source files
set(GC_SOURCES log_parser.cc committer.cc)

add_library(springtail_gc ${GC_SOURCES})
target_include_directories(springtail_gc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_gc
  PUBLIC springtail_common springtail_storage springtail_xid_mgr springtail_write_cache springtail_pg
  PRIVATE thrift_write_cache thrift_xid_mgr Boost::thread)

add_executable(gc_daemon gc_daemon.cc)
target_include_directories(gc_daemon
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(gc_daemon
    PRIVATE springtail_gc Boost::program_options
)
add_dsymutil_command(gc_daemon)
