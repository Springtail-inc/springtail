find_package(absl CONFIG REQUIRED)

# Add source files
set(LIB_PGEXT_SOURCES
    error.cc
    memory.cc
    guc.cc
)

# Create the library
add_library(pgext SHARED ${LIB_PGEXT_SOURCES})
target_include_directories(pgext
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(pgext
    PRIVATE absl::check
)
target_link_options(pgext PRIVATE -Wl,--exclude-libs,ALL) # prevent exports that aren't explicitly part of this library

# target_compile_options(pgext PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
set_target_properties(pgext PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)
