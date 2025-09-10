find_package(nlohmann_json REQUIRED)

set(ADMIN_HTTP_LIB_SOURCES
    admin_server.cc
)

add_library(springtail_admin_http ${ADMIN_HTTP_LIB_SOURCES})
target_include_directories(springtail_admin_http PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_admin_http
  PUBLIC springtail_common
  PRIVATE nlohmann_json
)


