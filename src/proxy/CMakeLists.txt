# boost-asio is header only, so we can't use find_package...
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(xxHash CONFIG REQUIRED)

# source files
set(PROXY_SOURCES
    server.cc
    connection.cc
    database.cc
    session.cc
    client_session.cc
    server_session.cc
    user_mgr.cc
    parser.cc
    buffer_pool.cc
    history_cache.cc
    authorization.cc
)

add_library(springtail_proxy ${PROXY_SOURCES})
target_include_directories(springtail_proxy
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)

target_link_libraries(springtail_proxy
    PUBLIC springtail_common
    PUBLIC springtail_pg
    PRIVATE ${libpg_query_BINARY_DIR}/libpg_query.a
    PRIVATE nlohmann_json::nlohmann_json
    PRIVATE fmt::fmt
    PRIVATE auth
    PRIVATE OpenSSL::SSL
    PRIVATE OpenSSL::Crypto
    PRIVATE xxHash::xxhash
)

## unit tests
# test parser executable
springtail_unit_test(test_parser
  SOURCES test/test_parser.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
           ${CMAKE_SOURCE_DIR}/external/libpg_query/
           ${CMAKE_SOURCE_DIR}/external/libpg_query/src
           ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
           ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
  LIBS springtail_proxy GTest::gtest_main
)

# test database manager executable
springtail_unit_test(test_database_mgr
  SOURCES test/test_database_mgr.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_proxy GTest::gtest_main
)

springtail_unit_test(test_database
  SOURCES test/test_database.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_proxy GTest::gtest_main
)

springtail_unit_test(test_util
  SOURCES test/test_util.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_msg_queue
  SOURCES test/test_msg_queue.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_common GTest::gtest_main
)

springtail_unit_test(test_scram
  SOURCES test/test_scram.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_proxy GTest::gtest_main
)

springtail_unit_test(test_replica_shutdown
  SOURCES test/test_replica_shutdown.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_proxy GTest::gtest_main
)

# test parsing executable
add_executable(parsing test/parsing.cc)
target_include_directories(parsing
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)
target_link_libraries(parsing
    PUBLIC springtail_proxy
)

# proxy server executable
add_executable(proxy daemon.cc)
target_include_directories(proxy
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(proxy
    PUBLIC springtail_proxy
    PRIVATE Boost::program_options
)
add_dsymutil_command(proxy)

add_subdirectory(auth)

# test logger executable
add_executable(test_logger test/test_logger.cc)
target_include_directories(test_logger
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_logger
    PUBLIC springtail_common
)
add_dsymutil_command(test_logger)

# install proxy to install directory
install(TARGETS
    proxy
    RUNTIME DESTINATION bin/system
)
