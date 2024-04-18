# Postgres CDC module
# source files
set(INGEST_SOURCES
    ingest.cc
)

# dependencies
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(nlohmann_json REQUIRED)

# destination library
add_library(springtail_ingest ${INGEST_SOURCES})

target_include_directories(springtail_ingest
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(springtail_ingest
    PUBLIC springtail_common
    PUBLIC springtail_pg
    PUBLIC springtail_storage
)

# executables
add_executable(test_ingest test/ingest.cc)

target_include_directories(test_ingest
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(test_ingest
    PUBLIC springtail_ingest
)

gtest_discover_tests(test_ingest
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
