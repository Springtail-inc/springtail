# Setup FDW extension for testing

# the cp command
find_program(
    CP_COMMAND
    cp
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

# the sudo command
find_program(
    SUDO_COMMAND
    sudo
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

# Get the PostgreSQL shared directory
execute_process(
    COMMAND pg_config --sharedir
    OUTPUT_VARIABLE PG_SHARE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get the PostgreSQL package library directory
execute_process(
    COMMAND pg_config --pkglibdir
    OUTPUT_VARIABLE PG_PKG_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Define the source files
set(SRC_DIR "${CMAKE_SOURCE_DIR}/src/pg_fdw")
set(SRC_FILES
    "${SRC_DIR}/springtail_fdw--1.0.sql"
    "${SRC_DIR}/springtail_fdw.control"
    "${CMAKE_BINARY_DIR}/src/pg_fdw/libspringtail_pg_fdw.so"
)

# Create custom commands for each file
foreach(src_file ${SRC_FILES})
    get_filename_component(filename ${src_file} NAME)

    if (filename STREQUAL "libspringtail_pg_fdw.so")
        set(dst_file "${PG_PKG_LIB_DIR}/springtail_fdw.so")
    else()
        set(dst_file "${PG_SHARE_DIR}/extension/${filename}")
    endif()

    add_custom_command(
        OUTPUT "${dst_file}"
        COMMAND ${SUDO_COMMAND} ${CP_COMMAND} --update=none
            "${src_file}"
            "${dst_file}"
        DEPENDS "${src_file}"
        COMMENT "Copying ${filename} to ${dst_file}"
    )
    list(APPEND COPY_TARGETS "${dst_file}")
endforeach()

# Create a target that depends on all copied files
add_custom_target(copy_fdw_files ALL
    DEPENDS ${COPY_TARGETS}
    COMMENT "Copied FDW files to their respective destinations"
)
