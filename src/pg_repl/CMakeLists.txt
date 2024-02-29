# Postgres CDC module
# source files
set(PG_LIB_SOURCES
    pg_repl_msg.cc
    pg_repl_connection.cc
    libpq_connection.cc
    pg_copy_table.cc
    pg_msg_stream.cc
    pg_msg_log_gen.cc
)

# dependencies
find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json REQUIRED)

# destination library
add_library(springtail_pg ${PG_LIB_SOURCES})

# executables
add_executable(pg_wal_dump test/pg_wal_dump.cc)
add_executable(pg_wal_decode test/pg_wal_decode.cc)

# include dirs
target_include_directories(springtail_pg
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

target_include_directories(pg_wal_dump
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

target_include_directories(pg_wal_decode
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

# libs to link against
target_link_libraries(springtail_pg
    PRIVATE PostgreSQL::PostgreSQL
    PUBLIC springtail_common
)

target_link_libraries(pg_wal_dump
    PRIVATE springtail_pg
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE Boost::program_options
    PRIVATE nlohmann_json::nlohmann_json
    PUBLIC springtail_common
)

target_link_libraries(pg_wal_decode
    PRIVATE springtail_pg
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE Boost::program_options
    PRIVATE nlohmann_json::nlohmann_json
    PUBLIC springtail_common
)

add_executable(gen_log test/gen_log.cc)
target_link_libraries(gen_log
    PUBLIC springtail_pg
    PRIVATE Boost::program_options
)
target_include_directories(gen_log
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

add_executable(test_msg_stream test/test_msg_stream.cc)
target_link_libraries(test_msg_stream
    PUBLIC springtail_pg
    PRIVATE GTest::gtest_main
)
target_include_directories(test_msg_stream
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

gtest_discover_tests(test_msg_stream
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
)

#if (${MACOSX})
#    add_custom_command(TARGET pg_wal_dump POST_BUILD
#        COMMAND ${PROJECT_SOURCE_DIR}/build_scripts/mac_enable_core.sh pg_wal_dump
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#        COMMENT "Enabling core dumps...pg_wal_dump")
#
#    add_custom_command(TARGET pg_wal_decode POST_BUILD
#        COMMAND ${PROJECT_SOURCE_DIR}/build_scripts/mac_enable_core.sh pg_wal_dump
#        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
#        COMMENT "Enabling core dumps...pg_wal_decode")
#endif()
