# Postgres CDC module
add_definitions( -D CXX_BYTE_ORDER=${CMAKE_CXX_BYTE_ORDER} )

# source files
set(LIBSOURCES pg_repl_connection.cc pg_repl_msg.cc)

# dependencies
find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)
find_package(fmt CONFIG REQUIRED)

# destination library
add_library(psqlcdc STATIC ${LIBSOURCES})

# executables
add_executable(pg_wal_dump pg_wal_dump.cc)
add_executable(pg_wal_decode pg_wal_decode.cc)

# include dirs
target_include_directories(psqlcdc
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

target_include_directories(pg_wal_dump
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

target_include_directories(pg_wal_decode
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

# libs to link against
target_link_libraries(psqlcdc
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE fmt::fmt
)

target_link_libraries(pg_wal_dump
    PRIVATE psqlcdc
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE Boost::program_options
    PRIVATE fmt::fmt
)

target_link_libraries(pg_wal_decode
    PRIVATE psqlcdc
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE Boost::program_options
    PRIVATE fmt::fmt
)

if (${MACOSX})
    add_custom_command(TARGET pg_wal_dump POST_BUILD
        COMMAND ${PROJECT_SOURCE_DIR}/build_scripts/mac_enable_core.sh pg_wal_dump
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Enabling core dumps...pg_wal_dump")

    add_custom_command(TARGET pg_wal_decode POST_BUILD
        COMMAND ${PROJECT_SOURCE_DIR}/build_scripts/mac_enable_core.sh pg_wal_dump
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Enabling core dumps...pg_wal_decode")

endif()