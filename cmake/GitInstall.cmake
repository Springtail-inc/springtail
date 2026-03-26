## This script retrieves the current Git hash and writes it to a file
## in the binary directory.  On make install it will copy the file to
## the install directory as INFO.txt.

# Step 1: Get the current Git hash
execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}  # Use the source directory for the git command
    RESULT_VARIABLE git_result
    OUTPUT_VARIABLE GIT_HASH
    ERROR_VARIABLE git_error
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Step 2: Check if Git command was successful
if(NOT git_result EQUAL 0)
    message(FATAL_ERROR "Failed to retrieve Git hash: ${git_error}")
endif()

# Step 3: Get the current date and time
execute_process(
    COMMAND date -u
    RESULT_VARIABLE date_result
    OUTPUT_VARIABLE INSTALL_DATE
    ERROR_VARIABLE date_error
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Step 4: Check if date command was successful
if(NOT date_result EQUAL 0)
    message(FATAL_ERROR "Failed to retrieve installation date: ${date_error}")
endif()

# Step 5: Create a file that contains the Git hash in the binary directory
set(INFO_FILE "${CMAKE_BINARY_DIR}/INFO.txt")

# Write the Git hash to the file
file(WRITE ${INFO_FILE} "Git Hash: ${GIT_HASH}\n")
file(APPEND ${INFO_FILE} "Install Date: ${INSTALL_DATE}\n")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    file(APPEND ${INFO_FILE} "Build Type: Debug\n")
else()
    file(APPEND ${INFO_FILE} "Build Type: Release\n")
endif()

# Step 6: Install the file to the desired directory as part of the install target
install(
    FILES ${INFO_FILE}
    DESTINATION .
)