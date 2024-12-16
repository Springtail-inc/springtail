enable_language(C)

find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

set(PG_FDW_SOURCES
    pg_fdw.c
    multicorn_util.c
    pg_fdw_mgr.cc
    pg_fdw_mgr_wrap.cc
)

# For postgres regression tests
add_library(regress SHARED regress.c)
target_include_directories(regress
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
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
    target_link_options(regress
        PRIVATE -Wl,--unresolved-symbols=ignore-all
    )
else()
    target_link_options(springtail_pg_fdw
        PRIVATE -Wl,-undefined,dynamic_lookup
        PRIVATE -Wl,-bundle
    )
    target_link_options(regress
        PRIVATE -Wl,-undefined,dynamic_lookup
        PRIVATE -Wl,-bundle
    )
endif()

target_link_libraries(springtail_pg_fdw
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_xid_mgr
    PRIVATE springtail_sys_tbl_mgr
)

add_executable(pg_ddl_daemon pg_ddl_daemon.cc pg_ddl_mgr.cc)
target_include_directories(pg_ddl_daemon
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(pg_ddl_daemon
    PRIVATE springtail_common
    PRIVATE springtail_pg
    PRIVATE springtail_redis
    PRIVATE Boost::program_options
)
add_dsymutil_command(pg_ddl_daemon)

# Extract transitive dependencies of libraryC
get_target_property(POSTGRES_DEP_LIBS PostgreSQL::PostgreSQL INTERFACE_LINK_LIBRARIES)

add_executable(dump_table test/dump_table.cc test/mock_pg.c pg_fdw_mgr.cc)
target_include_directories(dump_table
     PUBLIC ${PROJECT_SOURCE_DIR}/include
     PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

if (${LINUX})
set(START_GROUP "-Wl,--start-group")
set(END_GROUP "-Wl,--end-group")
else()
set(START_GROUP "")
set(END_GROUP "")
endif()

target_link_libraries(dump_table
     PRIVATE springtail_common
     PRIVATE springtail_storage
     PRIVATE springtail_sys_tbl_mgr
     PRIVATE springtail_xid_mgr
     PRIVATE Boost::program_options
     PRIVATE ${START_GROUP}
         ${POSTGRES_DEP_LIBS}
     ${END_GROUP}
)
add_dsymutil_command(dump_table)

add_executable(where test/where.cc)
target_include_directories(where
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(where
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_sys_tbl_mgr
    PRIVATE ${START_GROUP}
        ${POSTGRES_DEP_LIBS}
    ${END_GROUP}
)
add_dsymutil_command(where)

add_executable(test_where test/where_test.cc test/mock_pg.c pg_fdw_mgr.cc)
target_include_directories(test_where
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(test_where
    PRIVATE springtail_common
    PRIVATE springtail_test
    PRIVATE ${START_GROUP}
        ${POSTGRES_DEP_LIBS}
    ${END_GROUP}
    PRIVATE GTest::gtest_main
)
add_dsymutil_command(test_where)
gtest_discover_tests(test_where)

# install ddl daemon to install directory
install(TARGETS
    pg_ddl_daemon
    RUNTIME DESTINATION bin/system
)

# install shared library to lib directory
install(TARGETS
    springtail_pg_fdw
    LIBRARY DESTINATION lib
)

# install fdw files into share
install(FILES
    springtail_fdw--1.0.sql
    springtail_fdw.control
    DESTINATION share
)