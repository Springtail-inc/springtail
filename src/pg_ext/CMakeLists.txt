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
    float.cc
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

# ──────────────────────────────────────────────────────────────
# Add test_runner executable
# ──────────────────────────────────────────────────────────────

# Adjust path if test_runner.cc is elsewhere
add_executable(test_runner test_runner.cc)

# Link the executable against your shim lib and libdl for dlsym
target_link_libraries(test_runner
    PRIVATE pgext
    PRIVATE dl
    PRIVATE absl::flat_hash_map
)

target_include_directories(test_runner
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

# Add extn_parser executable
add_executable(extn_parser extn_parser.cc)

# Link the executable against the required libraries
target_link_libraries(extn_parser
    PRIVATE pgext
    PRIVATE springtail_proxy  # Add the proxy library that contains the parser implementation
    PRIVATE dl
    PRIVATE absl::flat_hash_map
)

target_include_directories(extn_parser
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    INCLUDES ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/external/libpg_query/
        ${CMAKE_SOURCE_DIR}/external/libpg_query/src
        ${CMAKE_SOURCE_DIR}/external/libpg_query/src/postgres/include
        ${CMAKE_SOURCE_DIR}/external/libpg_query/vendor/include
)
