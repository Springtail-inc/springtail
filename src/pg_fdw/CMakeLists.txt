enable_language(C)

find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(Boost COMPONENTS interprocess REQUIRED)
find_package(Boost COMPONENTS thread REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

include(FdwTestSetup)

set(PG_FDW_SOURCES
    pg_fdw.c
    multicorn_util.c
    pg_fdw_mgr.cc
    pg_fdw_mgr_wrap.cc
    pg_fdw_ddl_common.cc
)

set(PG_DDL_SOURCES
    pg_ddl_mgr.cc
    pg_fdw_ddl_common.cc
)

set(PG_FDW_TEST_SOURCES
    test/mock_pg.c
    pg_fdw_mgr.cc
    pg_fdw_ddl_common.cc
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

add_library(springtail_pg_ddl OBJECT ${PG_DDL_SOURCES})
target_include_directories(springtail_pg_ddl
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)

add_library(springtail_test_fdw OBJECT ${PG_FDW_TEST_SOURCES})
target_include_directories(springtail_test_fdw
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

if(${LINUX})
    target_link_options(springtail_pg_fdw
        PRIVATE -Wl,--unresolved-symbols=ignore-all
        PRIVATE -Wl,-rpath,/home/dev/springtail/shared-lib
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
    PRIVATE springtail_pg
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_xid_mgr_client
    PRIVATE springtail_sys_tbl_mgr
)

target_link_libraries(springtail_pg_ddl
    PUBLIC springtail_common
    springtail_xid_mgr
    PRIVATE Boost::thread
    nlohmann_json::nlohmann_json
    gRPC::grpc++
    protobuf::libprotobuf
)

target_link_libraries(springtail_test_fdw
    PUBLIC
    springtail_pg
    springtail_common
    springtail_xid_mgr
    springtail_sys_tbl_mgr
    PRIVATE Boost::thread
    nlohmann_json::nlohmann_json
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(pg_ddl_daemon pg_ddl_daemon.cc)
target_include_directories(pg_ddl_daemon
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(pg_ddl_daemon
    PRIVATE springtail_pg_ddl
    PRIVATE springtail_common
    PRIVATE springtail_pg
    PRIVATE springtail_redis
    PRIVATE Boost::program_options
)
add_dsymutil_command(pg_ddl_daemon)

add_executable(pg_xid_subscriber_daemon pg_xid_subscriber_daemon.cc pg_xid_subscriber_mgr.cc)
target_include_directories(pg_xid_subscriber_daemon
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(pg_xid_subscriber_daemon
    PRIVATE springtail_common
    PRIVATE springtail_pg
    PRIVATE springtail_xid_mgr_client
    PRIVATE Boost::program_options
)
add_dsymutil_command(pg_xid_subscriber_daemon)

# Extract transitive dependencies of libraryC
get_target_property(POSTGRES_DEP_LIBS PostgreSQL::PostgreSQL INTERFACE_LINK_LIBRARIES)

add_executable(dump_table test/dump_table.cc)
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
     PRIVATE springtail_test_fdw
     PRIVATE springtail_pg
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

## unit tests
springtail_unit_test(test_where
  SOURCES test/where_test.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include ${PostgreSQL_INCLUDE_DIRS}
  LIBS springtail_test_fdw
       springtail_pg
       springtail_common
       springtail_test
       ${START_GROUP}
         ${POSTGRES_DEP_LIBS}
       ${END_GROUP}
       GTest::gtest_main
)

springtail_unit_test(test_xid_subscriber
  SOURCES test/test_xid_subscriber.cc test/mock_pg.c pg_xid_subscriber_mgr.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/include ${PostgreSQL_INCLUDE_DIRS}
  LIBS springtail_pg
       springtail_common
       springtail_test
       springtail_sys_tbl_mgr
       ${START_GROUP}
         ${POSTGRES_DEP_LIBS}
       ${END_GROUP}
       GTest::gtest_main
)

# install ddl daemon to install directory
install(TARGETS
    pg_ddl_daemon
    RUNTIME DESTINATION bin/system
)

install(TARGETS
    pg_xid_subscriber_daemon
    RUNTIME DESTINATION bin/system
)

# install shared library to lib directory
install(TARGETS
    springtail_pg_fdw
    regress
    LIBRARY DESTINATION lib
)

# install fdw files into share
install(FILES
    springtail_fdw--1.0.sql
    springtail_fdw.control
    DESTINATION share
)

springtail_unit_test(test_pg_ddl_mgr
    SOURCES test/test_pg_ddl_mgr.cc pg_ddl_mgr.cc
    INCLUDES ${PROJECT_SOURCE_DIR}/include ${PostgreSQL_INCLUDE_DIRS}
    LIBS springtail_pg
        springtail_common
        springtail_test
        springtail_redis
        GTest::gtest_main
)

springtail_unit_test(test_field_quals
    SOURCES test/test_field_quals.cc
    INCLUDES ${PROJECT_SOURCE_DIR}/include ${PostgreSQL_INCLUDE_DIRS}
    LIBS springtail_test_fdw
         springtail_pg
         springtail_common
         springtail_test
         ${START_GROUP}
           ${POSTGRES_DEP_LIBS}
         ${END_GROUP}
         GTest::gtest_main
)

# dependencies for copying fdw files to postgres shared directory
add_dependencies(copy_fdw_files springtail_pg_fdw)
add_dependencies(test_pg_ddl_mgr copy_fdw_files)
