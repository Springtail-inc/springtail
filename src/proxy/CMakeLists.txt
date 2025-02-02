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

# test parser executable
add_executable(test_parser test/test_parser.cc)
target_include_directories(test_parser
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)
target_link_libraries(test_parser
    PRIVATE springtail_proxy
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_parser)
gtest_discover_tests(test_parser)

# test user manager executable
add_executable(test_user_mgr test/test_user_mgr.cc)
target_include_directories(test_user_mgr
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)
target_link_libraries(test_user_mgr
    PRIVATE springtail_proxy
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_user_mgr)
gtest_discover_tests(test_user_mgr)

# test database manager executable
add_executable(test_database_mgr test/test_database_mgr.cc)
target_include_directories(test_database_mgr
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_database_mgr
    PRIVATE springtail_proxy
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_database_mgr)
gtest_discover_tests(test_database_mgr)

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

# test database
add_executable(test_database test/test_database.cc)
target_include_directories(test_database
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_database
    PUBLIC springtail_proxy
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_database)
gtest_discover_tests(test_database)

add_executable(test_util test/test_util.cc)
target_include_directories(test_util
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_util
    PUBLIC springtail_common
    PRIVATE GTest::gtest_main
)
gtest_discover_tests(test_util)

add_executable(test_msg_queue test/test_msg_queue.cc)
target_include_directories(test_msg_queue
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_msg_queue
    PUBLIC springtail_common
    PRIVATE GTest::gtest_main
)
gtest_discover_tests(test_msg_queue)

# install proxy to install directory
install(TARGETS
    proxy
    RUNTIME DESTINATION bin/system
)
