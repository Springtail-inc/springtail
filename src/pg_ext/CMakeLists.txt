find_package(absl CONFIG REQUIRED)

# Add source files
set(LIB_PGEXT_SOURCES
    error.cc
    memory.cc
    guc.cc
    string.cc
    list.cc
    hash.cc
    regex.cc
    array.cc
    bit.cc
    fmgr.cc
    sort.cc
    crc.cc
    reloptions.cc
)

# Create the library
add_library(pgext SHARED ${LIB_PGEXT_SOURCES})
target_include_directories(pgext
    PUBLIC ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(pgext
    PRIVATE absl::check
    PRIVATE absl::flat_hash_map
)
target_link_options(pgext PRIVATE -Wl,--exclude-libs,ALL) # prevent exports that aren't explicitly part of this library

# target_compile_options(pgext PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
set_target_properties(pgext PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)
