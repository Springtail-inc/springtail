set(WRITE_CACHE_SOURCES write_cache.cc)

add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
target_include_directories(springtail_write_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_write_cache PUBLIC springtail_common)