enable_language(C)

find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

set(PG_FDW_SOURCES
    pg_fdw.c
    multicorn_util.c
    pg_fdw_mgr.cc
    pg_fdw_mgr_wrap.cc
)

add_library(springtail_pg_fdw SHARED ${PG_FDW_SOURCES})
target_include_directories(springtail_pg_fdw
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

if(${LINUX})
    target_link_options(springtail_pg_fdw
        PRIVATE -Wl,--unresolved-symbols=ignore-all
    )
else()
    target_link_options(springtail_pg_fdw
        PRIVATE -Wl,-undefined,dynamic_lookup
        PRIVATE -Wl,-bundle
    )
endif()

target_link_libraries(springtail_pg_fdw
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_xid_mgr
    PRIVATE springtail_sys_tbl_mgr
    PRIVATE PostgreSQL::PostgreSQL
)
#add_dsymutil_command(springtail_pg_fdw)

add_executable(copy_schema test/copy_schema.cc)
target_include_directories(copy_schema
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(copy_schema
    PRIVATE springtail_common
    PRIVATE springtail_pg
    PRIVATE springtail_storage
    PRIVATE springtail_sys_tbl_mgr
    PRIVATE Boost::program_options
)
add_dsymutil_command(copy_schema)

add_executable(dump_table test/dump_table.cc test/mock_pg.c pg_fdw_mgr.cc)
target_include_directories(dump_table
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(dump_table
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_sys_tbl_mgr
    PRIVATE springtail_xid_mgr
    PRIVATE Boost::program_options
    PRIVATE PostgreSQL::PostgreSQL
)
add_dsymutil_command(dump_table)

add_executable(where test/where.cc)
target_include_directories(where
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(where
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_sys_tbl_mgr
)
add_dsymutil_command(where)

add_executable(test_where test/where_test.cc test/mock_pg.c pg_fdw_mgr.cc)
target_include_directories(test_where
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(test_where
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_sys_tbl_mgr
    PRIVATE springtail_xid_mgr
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_where)
gtest_discover_tests(test_where)
