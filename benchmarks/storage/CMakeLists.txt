# Benchmarks for storage layer

# XXX check for MinIO packages
find_package(minio REQUIRED)

# source files
set(SOURCES src/minio_bench.cc)

# benchmarks
add_executable(minio_bench ${SOURCES})

# include dirs
target_include_directories(libcdc
    PUBLIC include
)

# libs to link against
target_link_libraries(libcdc
    PRIVATE libpq::pq
    PRIVATE libpqxx::pqxx)
