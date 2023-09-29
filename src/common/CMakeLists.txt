# Common library

set(COMMON_LIB_SOURCES exception.cc logging.cc)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_include_directories(springtail_common PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_common PUBLIC Backward::Backward spdlog::spdlog fmt::fmt)
