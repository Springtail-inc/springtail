# source files
set(STORAGE_SOURCES compressors.cc extent.cc schema.cc field.cc io.cc io_mgr.cc io_file.cc io_request.cc btree.cc mutable_btree.cc cache.cc cache_tracing.cc
                    numeric_utils.cc vacuumer.cc
                    index_engine/index_extent_cache.cc index_engine/btree_write_strategy.cc index_engine/index_writer.cc
                    index_engine/btree_read_strategy.cc index_engine/storage_cache_extent_source.cc index_engine/index_reader.cc
                    index_engine/shared_extent_pool.cc index_engine/shared_pool_extent_source.cc index_engine/segment_set.cc
                    # GiST (btree phase 10): only the opclass-INDEPENDENT halves. See
                    # springtail_opclass_engine below for why the opclass-calling halves
                    # (gist_helpers_opclass.cc, gist_reader_search.cc, gist_writer.cc,
                    # gin_helpers_opclass.cc, gin_write_strategy.cc) are a separate library
                    # instead of being folded in here too.
                    index_engine/gist_helpers.cc index_engine/gist_reader.cc
                    # GIN (btree phase 11): both halves are opclass-independent -- see
                    # gin_read_strategy.hh's own header comment for why, unlike GiST, GIN's
                    # *read* side (gin_read_strategy.cc) never needs the opclass-calling library
                    # at all (query-time term extraction is phase 12's job, out of this phase's
                    # scope); only gin_helpers.cc's codec/schema pieces and gin_read_strategy.cc
                    # itself land here, gin_helpers_opclass.cc/gin_write_strategy.cc go into
                    # springtail_opclass_engine below, mirroring gist_helpers_opclass.cc/
                    # gist_writer.cc's placement exactly.
                    index_engine/gin_helpers.cc index_engine/gin_read_strategy.cc)

find_package(lz4 REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)
find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
find_package(Boost COMPONENTS thread REQUIRED)
find_package(Boost COMPONENTS interprocess REQUIRED)
find_package(Boost COMPONENTS multi_index REQUIRED)

## main library
add_library(springtail_storage ${STORAGE_SOURCES})
target_include_directories(springtail_storage
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_storage
  PRIVATE lz4::lz4 xxHash::xxhash
  PUBLIC Boost::thread
         springtail_redis
         springtail_xid_mgr_client
         springtail_write_cache absl::check)

# Opclass-calling glue, GiST (btree redesign phase 10) and GIN (phase 11) both, is deliberately a
# SEPARATE library from springtail_storage above, not folded into STORAGE_SOURCES. It needs
# springtail_pgext (to dlopen and call into real Postgres opclass .so's), and springtail_pgext is a
# SHARED library -- linking it into springtail_storage, even PRIVATE, still adds it as a DT_NEEDED
# dependency of every final binary/.so that links springtail_storage transitively, including
# springtail_pg_fdw.so. That .so gets dlopen'd directly into a real, unmodified Postgres backend
# process, which already exports its own globals named palloc/elog/MemoryContextAlloc/etc.; pg_ext
# re-defines those same names with different ABI/semantics, and once pg_ext became a transitive
# link dependency of the FDW, those definitions started colliding (confirmed directly during phase
# 10: `ldd` on the built springtail_fdw.so showed libspringtail_pgext.so as a dependency for the
# first time in that phase, and the backend segfaulted inside CREATE EXTENSION springtail_fdw,
# before even reaching its own _PG_init() log line -- consistent with bad symbol resolution during
# dlopen's own relocation/ctor pass, not FDW logic).
#
# This library was named springtail_gist_engine through phase 10; phase 11 renamed it to
# springtail_opclass_engine (Open question (f), resolved in favor of folding GIN's opclass-calling
# glue in here rather than inventing a parallel springtail_gin_engine) once it stopped being
# GiST-specific: GinWriteStrategy::insert() needs GIN_EXTRACTVALUE (gin_helpers_opclass.cc) at
# build time, exactly the same shape of dependency (opclass-calling, pg_ext-needing, daemon-side-
# only) as GistWriter's compress/penalty/union/picksplit calls, and both are consumed only by the
# daemon-side springtail_sys_tbl_mgr -- never by springtail_sys_tbl_mgr_client/springtail_pg_fdw.
# Re-verified via `ldd` after the rename (see phases/phase-11.md's Design notes for the exact
# command/output) that springtail_pg_fdw.so and springtail_sys_tbl_mgr_client's own consumers still
# do not depend on libspringtail_pgext.so.
#
# GinReadStrategy (gin_read_strategy.cc, in STORAGE_SOURCES above, not here) does *not* link this
# library at all: unlike GistReader, it never calls into an opclass (query-time term extraction is
# explicitly phase 12's job, out of this phase's scope -- see gin_read_strategy.hh), so there is no
# GIN equivalent of gist_reader_search.cc's read-side opclass-calling split.
add_library(springtail_opclass_engine
  index_engine/gist_helpers_opclass.cc index_engine/gist_writer.cc index_engine/gist_reader_search.cc
  index_engine/gin_helpers_opclass.cc index_engine/gin_write_strategy.cc)
target_include_directories(springtail_opclass_engine
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_opclass_engine
  PUBLIC springtail_storage
  PRIVATE springtail_pgext
)


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
  LIBS springtail_storage springtail_test GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# extent test
springtail_unit_test(test_extent
  SOURCES test/test_extent.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# index_writer (btree) test
springtail_unit_test(test_index_writer
  SOURCES test/test_index_writer.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage springtail_xid_mgr GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# index_reader (btree) test
springtail_unit_test(test_index_reader
  SOURCES test/test_index_reader.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LINK_OPTIONS "--coverage"
  LIBS springtail_storage springtail_xid_mgr GTest::gtest_main unofficial::vincentlaucsb-csv-parser::csv ${GCOV_LIB}
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# shared extent pool (btree phase 5) test
springtail_unit_test(test_shared_extent_pool
  SOURCES test/test_shared_extent_pool.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# segment-set / sealed-segment-file test (btree phase 6)
springtail_unit_test(test_segment_file
  SOURCES test/test_segment_file.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage springtail_test springtail_xid_mgr GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

springtail_unit_test(test_numeric
  SOURCES test/test_numeric.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS springtail_storage GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# GiST strategy/engine test (btree phase 10)
springtail_unit_test(test_gist_strategy
  SOURCES test/test_gist_strategy.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  # -Wl,--start-group/--end-group (not plain ordering -- CMake's own transitive link-order
  # algorithm, not the LIBS order given here, decides the final link line, and it placed
  # springtail_xid_mgr.a *before* springtail_storage.a's (gist_engine-propagated) duplicate
  # occurrences, leaving vacuumer.cc.o's XidMgrServer references unresolved -- confirmed directly
  # by reading the generated link.txt, not guessed) makes the final link line order-independent
  # by letting ld re-scan the whole group until every symbol resolves.
  LIBS -Wl,--start-group springtail_storage springtail_opclass_engine springtail_pgext springtail_xid_mgr -Wl,--end-group GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

# GIN strategy/engine test (btree phase 11) -- links springtail_opclass_engine for the same
# link-order reason test_gist_strategy does (see its own comment above); GIN's test needs it too
# since gin_helpers::extract_terms() (opclass-calling, including the pure-C++ reference opclass
# dispatch) lives there even though gin_read_strategy.cc itself does not.
springtail_unit_test(test_gin_strategy
  SOURCES test/test_gin_strategy.cc
  INCLUDES ${CMAKE_SOURCE_DIR}/include
  LIBS -Wl,--start-group springtail_storage springtail_opclass_engine springtail_pgext springtail_xid_mgr -Wl,--end-group GTest::gtest_main
  WORKING_DIR ${CMAKE_CURRENT_SOURCE_DIR}/test
)

install(TARGETS dump_system_tables dump_headers RUNTIME DESTINATION bin/utils)
