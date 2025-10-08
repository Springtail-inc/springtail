find_package(absl CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

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
    numeric.cc
    node.cc
    jsonb.cc
    toast.cc
    parser.cc
    pqformat.cc
    domains.cc
    type.cc
    extn_registry.cc
)

# Create the library
add_library(pgext SHARED ${LIB_PGEXT_SOURCES})
target_include_directories(pgext
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(pgext
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE absl::check
    PRIVATE absl::flat_hash_map
    PRIVATE fmt::fmt
    PRIVATE springtail_common
)
# target_link_options(pgext PRIVATE -Wl,--exclude-libs,ALL) # prevent exports that aren't explicitly part of this library
target_link_options(pgext PRIVATE -W)

# target_compile_options(pgext PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
set_target_properties(pgext PROPERTIES
  CXX_VISIBILITY_PRESET default
  VISIBILITY_INLINES_HIDDEN ON
)

# Add extn_parser executable
add_executable(extn_parser extn_parser.cc)

# Link the executable against the required libraries
target_link_libraries(extn_parser
    PRIVATE pgext
    PRIVATE springtail_common
    PRIVATE dl
    PRIVATE absl::flat_hash_map
)
