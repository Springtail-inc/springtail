# Postgres CDC module

message(STATUS "Postgres: includes: ${PostgreSQL_INCLUDE_DIRS}")
message(STATUS "Postgres: type includes: ${PostgreSQL_TYPE_INCLUDE_DIR}")
message(STATUS "Postgres: libraries: ${PostgreSQL_LIBRARIES}")

add_definitions( -D CXX_BYTE_ORDER=${CMAKE_CXX_BYTE_ORDER} )

# source files
set(SOURCES src/ReplConnection.cc src/ReplMsg.cc)

find_package(PostgreSQL REQUIRED)

# destination library
add_library(cdc STATIC ${SOURCES})

# include dirs
target_include_directories(cdc
    PUBLIC include
    PRIVATE PostgreSQL::PostgreSQL
)

# libs to link against
target_link_libraries(cdc
                      PostgreSQL::PostgreSQL
)

