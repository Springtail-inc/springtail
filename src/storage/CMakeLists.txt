# source files
set(STORAGE_SOURCES compressors.cc extent.cc schema.cc field.cc io.cc io_mgr.cc io_file.cc io_request.cc btree.cc mutable_btree.cc table_mgr.cc schema_mgr.cc data_cache.cc table.cc system_tables.cc cache.cc)

find_package(lz4 REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

## main library
add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_storage PRIVATE lz4::lz4 xxHash::xxhash)


## helpers
add_executable(read_leaf test/read_leaf.cc)
target_include_directories(read_leaf PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(read_leaf PUBLIC springtail_common springtail_storage)

add_executable(read_branch test/read_branch.cc)
target_include_directories(read_branch PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(read_branch PUBLIC springtail_common springtail_storage)

## tests

if(LINUX)
    set(GCOV_LIB "gcov")
else()
    set(GCOV_LIB "")
endif()

# io test
add_executable(test_io test/io_test.cc)
target_include_directories(test_io PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_io
  PUBLIC springtail_common springtail_storage
  PRIVATE GTest::gtest_main
)

# btree test
add_executable(test_btree test/test_btree.cc)
target_include_directories(test_btree PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_options(test_btree PRIVATE "--coverage")

target_link_libraries(test_btree
    PUBLIC springtail_common springtail_storage GTest::gtest_main
    PRIVATE unofficial::vincentlaucsb-csv-parser::csv Boost::thread ${GCOV_LIB})

# table test
add_executable(test_table test/test_table.cc)
target_include_directories(test_table PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_options(test_table PRIVATE "--coverage")

target_link_libraries(test_table
    PUBLIC springtail_common springtail_storage GTest::gtest_main
    PRIVATE unofficial::vincentlaucsb-csv-parser::csv Boost::thread ${GCOV_LIB})

# table_mgr test
add_executable(test_table_mgr test/test_table_mgr.cc)
target_include_directories(test_table_mgr PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_options(test_table_mgr PRIVATE "--coverage")

target_link_libraries(test_table_mgr
    PUBLIC springtail_common springtail_storage GTest::gtest_main
    PRIVATE unofficial::vincentlaucsb-csv-parser::csv Boost::thread ${GCOV_LIB})

# cache test
add_executable(test_cache test/test_cache.cc)
target_include_directories(test_cache PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_options(test_cache PRIVATE "--coverage")

target_link_libraries(test_cache
    PUBLIC springtail_common springtail_storage GTest::gtest_main
    PRIVATE unofficial::vincentlaucsb-csv-parser::csv Boost::thread ${GCOV_LIB})

# extent test
add_executable(test_extent test/test_extent.cc)
target_include_directories(test_extent PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test_extent PUBLIC springtail_common springtail_storage GTest::gtest_main)

# hook tests into gtest framework
gtest_discover_tests(test_extent)
gtest_discover_tests(test_btree
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test)
gtest_discover_tests(test_table
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test)
gtest_discover_tests(test_table_mgr
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test)
gtest_discover_tests(test_cache
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test)
gtest_discover_tests(test_io)
