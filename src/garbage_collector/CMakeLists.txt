# source files
set(GC_SOURCES log_parser.cc)

add_library(springtail_gc ${GC_SOURCES})
target_include_directories(springtail_gc PRIVATE ${CMAKE_SOURCE_DIR}/include
                                                 ${CMAKE_SOURCE_DIR}/src/write_cache/gen-cpp) # for thrift headers
target_link_libraries(springtail_gc PRIVATE Boost::thread)
