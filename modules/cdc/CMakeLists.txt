# Postgres CDC module

# check for libpqxx packages -- pulled in from conan
find_package(PostgreSQL REQUIRED)
find_package(libpqxx REQUIRED)

# source files
set(SOURCES src/cdc.cc)

# destination library
add_library(libcdc SHARED ${SOURCES})

# include dirs
target_include_directories(libcdc
    PUBLIC include
)

# libs to link against
target_link_libraries(libcdc
    PRIVATE libpq::pq
    PRIVATE libpqxx::pqxx)
