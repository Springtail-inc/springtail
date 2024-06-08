enable_language(C)

find_package(PostgreSQL REQUIRED)

set(PG_FDW_SOURCES
    pg_fdw.c
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
        #PRIVATE -Wl,-all_load
    )
endif()

target_link_libraries(springtail_pg_fdw
    PRIVATE springtail_common
    PRIVATE springtail_storage
    PRIVATE springtail_xid_mgr
    #PRIVATE PostgreSQL::PostgreSQL
)

add_executable(gen_schema_sql test/gen_schema_sql.cc)
target_include_directories(gen_schema_sql
    PUBLIC ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(gen_schema_sql
    PUBLIC springtail_common
    PUBLIC springtail_storage
    PUBLIC springtail_pg
)