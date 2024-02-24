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
    pg_log_gen.cc
)

add_library(springtail_pglog ${PG_LOG_SOURCES})
target_include_directories(springtail_pglog
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/xid_mgr/gen-cpp # for thrift headers
)
target_link_libraries(springtail_pglog
    PUBLIC springtail_pg springtail_common springtail_xid_mgr
    PRIVATE nlohmann_json::nlohmann_json
    PRIVATE fmt::fmt)

add_executable(pg_log_mgr_daemon pg_log_mgr_daemon.cc)

target_include_directories(pg_log_mgr_daemon
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(pg_log_mgr_daemon
    PUBLIC springtail_pglog
    PRIVATE Boost::program_options
)

add_executable(gen_log test/gen_log.cc)
target_link_libraries(gen_log
    PUBLIC springtail_pglog
    PRIVATE Boost::program_options
)
target_include_directories(gen_log
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

add_executable(test_log_reader test/test_reader.cc)
target_link_libraries(test_log_reader
    PUBLIC springtail_pglog
    PRIVATE GTest::gtest_main
)
target_include_directories(test_log_reader
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

gtest_discover_tests(test_log_reader
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
)