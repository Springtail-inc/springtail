find_package(absl CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

# Add source files
set(LIB_PGEXT_SOURCES
    error.cc
    memory.cc
    guc.cc
    date.cc
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
    heaptuple.cc
    float.cc
    numeric.cc
    node.cc
    jsonb.cc
    toast.cc
    parser.cc
    pqformat.cc
    domains.cc
    type.cc
    trgm_helpers.cc
    extn_registry.cc
)

# Create the library
add_library(springtail_pgext SHARED ${LIB_PGEXT_SOURCES})
target_include_directories(springtail_pgext
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${PostgreSQL_INCLUDE_DIRS}
)
target_link_libraries(springtail_pgext
    PRIVATE PostgreSQL::PostgreSQL
    PRIVATE absl::check
    PRIVATE absl::flat_hash_map
    PRIVATE fmt::fmt
    PRIVATE springtail_common
)
target_link_options(springtail_pgext PRIVATE -W)

set_target_properties(springtail_pgext PROPERTIES
  CXX_VISIBILITY_PRESET default
  VISIBILITY_INLINES_HIDDEN ON
)

# Add extn_parser executable
add_executable(springtail_extn_parser extn_parser.cc)

# Link the executable against the required libraries
target_link_libraries(springtail_extn_parser
    PRIVATE springtail_pgext
    PRIVATE springtail_common
    PRIVATE dl
    PRIVATE absl::flat_hash_map
)

install(TARGETS
    springtail_pgext
    LIBRARY DESTINATION shared-lib
)
