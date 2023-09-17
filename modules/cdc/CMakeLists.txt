# Postgres CDC module

message(STATUS "Postgres: includes: ${PostgreSQL_INCLUDE_DIRS}")
message(STATUS "Postgres: type includes: ${PostgreSQL_TYPE_INCLUDE_DIR}")
message(STATUS "Postgres: libraries: ${PostgreSQL_LIBRARIES}")

# source files
set(SOURCES src/PqConnection.cc)

# destination library
add_library(libcdc STATIC ${SOURCES})

# include dirs
target_include_directories(libcdc
    PUBLIC include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)

# libs to link against
target_link_libraries(libcdc
    ${PostgreSQL_LIBRARIES})
