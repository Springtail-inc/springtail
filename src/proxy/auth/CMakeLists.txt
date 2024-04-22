## Note most of this code orginates from PGBouncer and LibUsual

enable_language(C)

find_package(fmt CONFIG REQUIRED)
# Add all the source files in the current directory to the library
set(SOURCES
    scram.cc
    base64.c
    saslprep.c
    scram-common.c
    sha256.c
    wchar.c
    md5.c
    unicode_norm.c
)

# Create the library target
add_library(auth STATIC ${SOURCES})

target_include_directories(auth
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(auth PRIVATE fmt::fmt-header-only)
