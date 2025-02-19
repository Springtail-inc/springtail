# Setup FDW extension for testing

# the cp command
find_program(
    CP_COMMAND
    cp
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

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

# Construct the destination directory
set(DEST_DIR "${PG_SHARE_DIR}/extension")

# Define the source files
set(SRC_DIR "${CMAKE_SOURCE_DIR}/src/pg_fdw")
set(SRC_FILES
    "${SRC_DIR}/springtail_fdw--1.0.sql"
    "${SRC_DIR}/springtail_fdw.control"
)

# Create custom commands for each file
foreach(src_file ${SRC_FILES})
    get_filename_component(filename ${src_file} NAME)
    add_custom_command(
        OUTPUT "${DEST_DIR}/${filename}"
        COMMAND ${SUDO_COMMAND} ${CP_COMMAND} --update=none
            "${src_file}"
            "${DEST_DIR}/${filename}"
        DEPENDS "${src_file}"
        COMMENT "Copying ${filename} to ${DEST_DIR}"
    )
    list(APPEND COPY_TARGETS "${DEST_DIR}/${filename}")
endforeach()

# Create a target that depends on all copied files
add_custom_target(copy_fdw_files ALL
    DEPENDS ${COPY_TARGETS}
)
