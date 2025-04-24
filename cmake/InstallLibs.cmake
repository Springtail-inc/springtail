# Create a custom target to copy shared libraries
# the cp command
find_program(
    CP_COMMAND
    cp
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(SOURCE_LIB_DIR "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/debug/lib")
else()
    set(SOURCE_LIB_DIR "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/lib")
endif()

# Clean up the destination directory and copy libraries
set(DEST_LIB_DIR "${PROJECT_SOURCE_DIR}/shared-lib")
add_custom_target(copy_shared_libraries ALL
    COMMAND ${CMAKE_COMMAND} -E rm -rf "${DEST_LIB_DIR}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${DEST_LIB_DIR}"
    COMMAND ${CP_COMMAND} -au "${SOURCE_LIB_DIR}/*.so*" "${DEST_LIB_DIR}"
    COMMENT "Copying shared libraries from vcpkg to the project-level shared-lib directory"
)