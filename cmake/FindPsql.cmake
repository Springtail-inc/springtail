# Find Postgres; system Find doesn't always look in the right places
# Use pg_config, like one should, assumes it is in the path

message(STATUS "Running pg_config commands")

execute_process(COMMAND pg_config --libdir --includedir-server --includedir
    OUTPUT_VARIABLE CMD_OUTPUT
    RESULT_VARIABLE CMD_ERROR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(CMD_ERROR)
    message(STATUS "Command failed: pg_config")
    set(PostgreSQL_FOUND FALSE)
else()
    set(PostgreSQL_FOUND TRUE)

    string(REPLACE "\n" ";" outputs "${CMD_OUTPUT}")
    list(POP_FRONT outputs LIB_DIR)
    set(PostgreSQL_LIBRARY_DIRS ${LIB_DIR})
    set(PostgreSQL_INCLUDE_DIRS ${outputs})

    message(STATUS "Include dirs: " ${PostgreSQL_INCLUDE_DIRS})
    message(STATUS "Library dirs: " ${PostgreSQL_LIBRARY_DIRS})

    find_library(LIBPQ pq PATHS ${PostgreSQL_LIBRARY_DIRS} PATH_SUFFIXES "postgresql" REQUIRED)
    find_library(LIBPQWAL pqwalreceiver PATHS ${PostgreSQL_LIBRARY_DIRS} PATH_SUFFIXES "postgresql" REQUIRED)

    set(PostgreSQL_LIBRARIES ${LIBPQ} ${LIBPQWAL})

    message(STATUS "Libs: " ${PostgreSQL_LIBRARIES})

endif()




