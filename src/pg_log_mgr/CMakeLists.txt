# source files
set(PG_LOG_SOURCES
    pg_log_mgr.cc
    pg_log_writer.cc
    pg_log_reader.cc
    pg_xact_handler.cc
    pg_xact_log_writer.cc
)

add_executable(pg_log_mgr ${PG_LOG_SOURCES})

target_include_directories(pg_log_mgr
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/xid_mgr/gen-cpp # for thrift headers
)

target_link_libraries(pg_log_mgr
    PRIVATE psqlcdc
    PRIVATE springtail_xid_mgr
    PRIVATE springtail_common
)
