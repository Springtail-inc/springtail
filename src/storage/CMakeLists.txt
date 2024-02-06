# source files
set(STORAGE_SOURCES compressors.cc extent.cc schema.cc field.cc io.cc io_mgr.cc io_file.cc io_request.cc btree.cc mutable_btree.cc)

find_package(lz4 REQUIRED)
find_package(xxhash CONFIG REQUIRED)

add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_storage PRIVATE lz4::lz4 xxHash::xxhash)


## tests

add_executable(test_io test/io_test.cc)
target_include_directories(test_io PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_io PUBLIC springtail_common springtail_storage)

add_executable(test_btree test/test_btree.cc)
target_include_directories(test_btree PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_options(test_btree PRIVATE "--coverage")
target_link_libraries(test_btree
  PUBLIC springtail_common springtail_storage GTest::gtest_main
  PRIVATE unofficial::vincentlaucsb-csv-parser::csv)

add_executable(test_extent test/test_extent.cc)
target_include_directories(test_extent PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_extent PUBLIC springtail_common springtail_storage GTest::gtest_main)

add_test(NAME io COMMAND test_io)
add_test(NAME extent COMMAND test_extent)
add_test(NAME btree
  COMMAND test_btree
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test)
