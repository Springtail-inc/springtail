# source files
set(PG_LOG_SOURCES
    pg_log_mgr.cc
    pg_log_writer.cc
    pg_log_reader.cc
)

add_executable(pg_log_mgr ${PG_LOG_SOURCES})

target_include_directories(pg_log_mgr
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(pg_log_mgr
    PRIVATE psqlcdc
    PRIVATE springtail_common
)
