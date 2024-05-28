# Benchmarks for storage layer

# fmt
find_package(fmt CONFIG REQUIRED)

# boost program options
find_package(Boost COMPONENTS program_options REQUIRED)

# source files
set(FILE_SOURCES file_bench.cc)

# benchmarks
add_executable(file_bench ${FILE_SOURCES})
target_include_directories(file_bench PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(file_bench PRIVATE fmt::fmt Boost::program_options)

add_executable(crash crash.cc)
target_include_directories(crash PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(crash PRIVATE springtail_common)


# # include dirs
# target_include_directories(libcdc
#     PUBLIC include
# )

# # libs to link against
# target_link_libraries(libcdc
#     PRIVATE libpq::pq
#     PRIVATE libpqxx::pqxx)
