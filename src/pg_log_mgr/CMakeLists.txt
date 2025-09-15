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
    pg_log_recovery.cc
    sync_tracker.cc
)

add_library(springtail_pglog ${PG_LOG_SOURCES})
target_include_directories(springtail_pglog
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_pglog
    PUBLIC springtail_pg
           springtail_common
           springtail_redis
           springtail_xid_mgr
           springtail_storage
           springtail_sys_tbl_mgr_client
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

add_executable(test_reconnect test/test_reconnect.cc)
target_include_directories(test_reconnect
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_reconnect
    PUBLIC springtail_pglog
    PRIVATE Boost::program_options
)

## unit tests
springtail_unit_test(test_xact_log
  SOURCES test/test_xact_log.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_pglog GTest::gtest_main
)

springtail_unit_test(test_pg_log_redis
  SOURCES test/test_redis.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_pglog GTest::gtest_main
)

springtail_unit_test(test_pg_log_indexer
  SOURCES test/test_indexer.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_pglog springtail_test GTest::gtest_main
)

springtail_unit_test(test_log_repair
  SOURCES test/test_log_repair.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_pglog GTest::gtest_main
)

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
    dump_repl_log
    RUNTIME DESTINATION bin/utils
)
