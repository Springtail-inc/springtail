# source files
set(STORAGE_SOURCES compressors.cc datafile.cc extent.cc schema.cc schema_column.cc io.cc io_mgr.cc io_file.cc io_request.cc)

find_package(lz4 REQUIRED)
find_package(xxhash CONFIG REQUIRED)

add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_storage PRIVATE lz4::lz4 xxHash::xxhash)

add_executable(io_test io_test.cc)
target_include_directories(io_test PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(io_test PUBLIC springtail_common springtail_storage)
