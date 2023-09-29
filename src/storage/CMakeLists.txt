# source files
set(STORAGE_SOURCES compressors.cc datafile.cc extent.cc schema.cc schema_column.cc)

add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_storage PUBLIC springtail_common)
