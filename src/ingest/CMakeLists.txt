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
    PRIVATE Boost::program_options
    PRIVATE nlohmann_json::nlohmann_json
)

# executables
add_executable(test_ingest test/test_ingest.cc)

target_include_directories(test_ingest
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(test_ingest
    PUBLIC springtail_ingest
    PRIVATE GTest::gtest_main
)

gtest_discover_tests(test_ingest
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
)
