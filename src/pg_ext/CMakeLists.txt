find_package(absl CONFIG REQUIRED)

# Add source files
set(LIB_PGEXT_SOURCES
    memory.cc
)

# Create the library
add_library(pgext SHARED ${LIB_PGEXT_SOURCES})
target_include_directories(pgext
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(pgext
    PRIVATE absl::check
)
