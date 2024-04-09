# boost-asio is header only, so we can't use find_package...
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(nlohmann_json REQUIRED)

# source files
set(PROXY_SOURCES
    server.cc
    client_session.cc
    request_handler.cc
    parser.cc
)

add_library(springtail_proxy ${PROXY_SOURCES})
target_include_directories(springtail_proxy
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${Boost_INCLUDES}
)
target_link_libraries(springtail_proxy
    PUBLIC springtail_pg springtail_common
    PRIVATE ${Boost_LIBRARIES}
    PRIVATE ${libpg_query_BINARY_DIR}/libpg_query.a
    PRIVATE nlohmann_json::nlohmann_json
    PRIVATE fmt::fmt
    PRIVATE auth
)

# test parser executable
add_executable(parser test/parser.cc)
target_include_directories(parser
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
    PRIVATE ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)
target_link_libraries(parser PRIVATE pg_query)


# test server daemon executable
add_executable(daemon test/daemon.cc)
target_include_directories(daemon
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${Boost_INCLUDES}
)
target_link_libraries(daemon
    PRIVATE springtail_proxy
)

# test client executable
add_executable(client test/client.cc)
target_include_directories(client
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${Boost_INCLUDES}
)
target_link_libraries(client
    PRIVATE springtail_proxy
)

add_subdirectory(auth)

