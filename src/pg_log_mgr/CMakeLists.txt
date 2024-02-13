find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json REQUIRED)

# source files
set(PG_LOG_SOURCES
    pg_log_mgr.cc
    pg_log_writer.cc
    pg_log_reader.cc
    pg_xact_handler.cc
    pg_xact_log_writer.cc
    pg_log_mgr_daemon.cc
)

add_executable(pg_log_mgr_daemon ${PG_LOG_SOURCES})

target_include_directories(pg_log_mgr_daemon
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/xid_mgr/gen-cpp # for thrift headers
)

target_link_libraries(pg_log_mgr_daemon
    PRIVATE springtail_pg
    PRIVATE springtail_xid_mgr
    PRIVATE springtail_common
    PRIVATE Boost::program_options
)

