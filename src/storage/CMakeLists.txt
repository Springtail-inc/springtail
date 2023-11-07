# source files
set(STORAGE_SOURCES compressors.cc datafile.cc extent.cc schema.cc schema_column.cc io.cc io_pool.cc)

find_package(lz4 REQUIRED)

add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_storage PUBLIC springtail_common PRIVATE lz4::lz4)

add_executable(io_test io.cc io_pool.cc)
target_include_directories(io_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(io_test PRIVATE springtail_storage PUBLIC springtail_common)
