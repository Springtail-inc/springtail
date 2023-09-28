# Common library

set(COMMON_LIB_SOURCES exception.cc)

add_library(springtail_common ${COMMON_LIB_SOURCES})
target_link_libraries(springtail_common PUBLIC Backward::Backward)
