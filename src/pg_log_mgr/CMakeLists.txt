find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json REQUIRED)

# source files
set(PG_LOG_SOURCES
    committer.cc
    indexer.cc
    pg_log_coordinator.cc
    pg_log_mgr.cc
    pg_log_writer.cc
    pg_log_reader.cc
    pg_xact_log_writer.cc
    pg_xact_log_reader.cc
    sync_tracker.cc
)

add_library(springtail_pglog ${PG_LOG_SOURCES})
target_include_directories(springtail_pglog
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/xid_mgr/gen-cpp # for thrift headers
)
target_link_libraries(springtail_pglog
    PUBLIC springtail_pg
           springtail_common
	   springtail_redis
           springtail_xid_mgr
           springtail_storage
           springtail_sys_tbl_mgr
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
add_dsymutil_command(pg_log_mgr_daemon)

add_executable(test_log_reader test/test_reader.cc)
target_link_libraries(test_log_reader
    PUBLIC springtail_pglog
           springtail_common
           springtail_storage
           springtail_sys_tbl_mgr
	   springtail_test
    PRIVATE GTest::gtest_main)
target_include_directories(test_log_reader PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(test_log_reader)

add_executable(test_xact_log test/test_xact_log.cc)
target_link_libraries(test_xact_log
    PUBLIC springtail_pglog
           springtail_common
           springtail_storage
    PRIVATE GTest::gtest_main)
target_include_directories(test_xact_log PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(test_xact_log)

add_executable(test_pg_log_redis test/test_redis.cc)
target_link_libraries(test_pg_log_redis
    PUBLIC springtail_pglog
    PRIVATE GTest::gtest_main)
target_include_directories(test_pg_log_redis PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(test_pg_log_redis)

add_executable(dump_xact_log test/dump_xact_log.cc)
target_link_libraries(dump_xact_log
    PUBLIC springtail_pglog
           springtail_common)
target_include_directories(dump_xact_log PRIVATE ${CMAKE_SOURCE_DIR}/include)
add_dsymutil_command(dump_xact_log)

# dump_repl_log
add_executable(dump_repl_log test/dump_repl_log.cc)
target_include_directories(dump_repl_log
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(dump_repl_log
    PUBLIC springtail_pg
    PRIVATE Boost::program_options
)
add_dsymutil_command(dump_repl_log)

# install pg_log_mgr_daemon to install directory
install(TARGETS pg_log_mgr_daemon RUNTIME DESTINATION bin/system)

# install utils
install(TARGETS
    dump_xact_log
    dump_repl_log
    RUNTIME DESTINATION bin/utils
)

gtest_discover_tests(test_log_reader
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
)
gtest_discover_tests(test_xact_log)
gtest_discover_tests(test_pg_log_redis)
