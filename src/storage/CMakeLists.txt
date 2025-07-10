# source files
set(STORAGE_SOURCES compressors.cc extent.cc schema.cc field.cc io.cc io_mgr.cc io_file.cc io_request.cc btree.cc mutable_btree.cc cache.cc
                    numeric_utils.cc vacuumer.cc)

find_package(lz4 REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)
find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
find_package(Boost COMPONENTS thread REQUIRED)

## main library
add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_storage
  PRIVATE lz4::lz4 xxHash::xxhash
  PUBLIC Boost::thread
         springtail_redis
         springtail_write_cache absl::check)


## helpers
add_executable(read_leaf test/read_leaf.cc)
target_include_directories(read_leaf PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(read_leaf PUBLIC springtail_storage)
add_dsymutil_command(read_leaf)

add_executable(read_branch test/read_branch.cc)
target_include_directories(read_branch PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(read_branch PUBLIC springtail_storage)
add_dsymutil_command(read_branch)

add_executable(dump_system_tables test/dump_system_tables.cc)
target_include_directories(dump_system_tables PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(dump_system_tables PUBLIC springtail_sys_tbl_mgr)
add_dsymutil_command(dump_system_tables)

add_executable(dump_headers test/dump_headers.cc)
target_include_directories(dump_headers PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(dump_headers PUBLIC springtail_storage)
add_dsymutil_command(dump_headers)

add_executable(dump_file test/dump_file.cc)
target_include_directories(dump_file PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(dump_file PUBLIC springtail_storage)
add_dsymutil_command(dump_file)

if(LINUX)
    set(GCOV_LIB "gcov")
else()
    set(GCOV_LIB "")
endif()

# btree benchmark
add_executable(benchmark_btree test/benchmark_btree.cc)
target_include_directories(benchmark_btree PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(benchmark_btree
    PUBLIC springtail_storage
    PRIVATE benchmark::benchmark ${GCOV_LIB})
add_dsymutil_command(benchmark_btree)

# extent benchmark
add_executable(benchmark_extents test/benchmark_extents.cc)
target_include_directories(benchmark_extents PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(benchmark_extents PRIVATE springtail_storage benchmark::benchmark)
add_dsymutil_command(benchmark_extents)


## unit tests
# io test
springtail_unit_test(test_io
  SOURCES test/io_test.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
)

# btree test
springtail_unit_test(test_btree
  SOURCES test/test_btree.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# table test
springtail_unit_test(test_table
  SOURCES test/test_table.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage springtail_test GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# table_mgr test
springtail_unit_test(test_table_mgr
  SOURCES test/test_table_mgr.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage springtail_test GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# cache test
springtail_unit_test(test_cache
  SOURCES test/test_cache.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# extent test
springtail_unit_test(test_extent
  SOURCES test/test_extent.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

springtail_unit_test(test_numeric
  SOURCES test/test_numeric.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

install(TARGETS dump_system_tables dump_headers RUNTIME DESTINATION bin/utils)
