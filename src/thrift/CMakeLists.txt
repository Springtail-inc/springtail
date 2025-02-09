find_package(Thrift CONFIG REQUIRED)
include("${THRIFT_CMAKE_DIR}/thriftnbTargets.cmake")

# the thrift rpc gen
find_program(
    THRIFT_COMPILER
    thrift
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

# the grep command
find_program(
    GREP_COMMAND
    grep
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

# the cp command
find_program(
    CP_COMMAND
    cp
    HINTS /usr/bin /bin /usr/local/bin
    REQUIRED
)

message(STATUS "Found thrift compiler: ${THRIFT_COMPILER}")

# Set Python output directory
set(PYTHON_GEN_DIR ${PROJECT_SOURCE_DIR}/python/thrift)

# set variables for thrift schema .thrift file and generated .h
file(GLOB THRIFT_FILES "*.thrift")
# set(THRIFT_FILES write_cache.thrift xid_mgr.thrift sys_tbl_mgr.thrift)

# iterate over all thrift files; generate the cpp and h files and link them into libraries
foreach(THRIFT_FILE ${THRIFT_FILES})
    # extract thrift base file name and set the paths
    get_filename_component(THRIFT_FILE_NAME ${THRIFT_FILE} NAME_WE)
    set(THRIFT_FILE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${THRIFT_FILE_NAME}.thrift)
    set(THRIFT_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/${THRIFT_FILE_NAME})
    set(THRIFT_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include/thrift/${THRIFT_FILE_NAME})
    set(THRIFT_PYTHON_GEN_DIR ${PYTHON_GEN_DIR}/${THRIFT_FILE_NAME})

    MESSAGE(STATUS "Processing thrift file: ${THRIFT_FILE_PATH}")

    set(EXTRACTED_SERVICE_NAME "")  # Initialize variable to store the extracted service name

    # Execute process with capture to extract service name from thrift file
    execute_process(
        COMMAND ${GREP_COMMAND} service ${THRIFT_FILE_PATH}
        OUTPUT_VARIABLE EXTRACTED_SERVICE_NAME  # Capture output in variable
        OUTPUT_STRIP_TRAILING_WHITESPACE  # Remove trailing whitespaces from output
        ERROR_VARIABLE EXECUTE_PROCESS_ERROR  # Capture error messages (optional)
    )
    # Regular expression to match a word after "service " (with optional whitespace)
    set(THRIFT_SERVICE "")
    string(REGEX REPLACE  "^[ ]*service[ ]+(.*)[ ]+{" "\\1" THRIFT_SERVICE "${EXTRACTED_SERVICE_NAME}")
    MESSAGE(STATUS "Found service ${EXTRACTED_SERVICE_NAME} for ${THRIFT_FILE}")

    # set the generated files
    set(THRIFT_GEN_FILES
        ${THRIFT_GEN_DIR}/${THRIFT_FILE_NAME}_types.h
        ${THRIFT_GEN_DIR}/${THRIFT_FILE_NAME}_types.cpp
        ${THRIFT_GEN_DIR}/${THRIFT_SERVICE}.h
        ${THRIFT_GEN_DIR}/${THRIFT_SERVICE}.cpp
    )

    # Set Python generated files
    set(THRIFT_PYTHON_GEN_FILES
        ${THRIFT_PYTHON_GEN_DIR}/ttypes.py
        ${THRIFT_PYTHON_GEN_DIR}/${THRIFT_SERVICE}.py
        ${THRIFT_PYTHON_GEN_DIR}/constants.py
        ${THRIFT_PYTHON_GEN_DIR}/__init__.py
    )

    # add custom command to generate both C++ and Python thrift files
    add_custom_command(
        OUTPUT ${THRIFT_GEN_FILES} ${THRIFT_PYTHON_GEN_FILES}
        COMMAND mkdir -p ${THRIFT_GEN_DIR}
        COMMAND mkdir -p ${THRIFT_INCLUDE_DIR}
        COMMAND mkdir -p ${THRIFT_PYTHON_GEN_DIR}
        # Generate C++ files
        COMMAND ${THRIFT_COMPILER} -I ${CMAKE_CURRENT_SOURCE_DIR} --gen cpp:no_skeleton,pure_enums,moveable_types -out ${THRIFT_GEN_DIR} ${THRIFT_FILE_PATH}
        # Generate Python files
        COMMAND ${THRIFT_COMPILER} -I ${CMAKE_CURRENT_SOURCE_DIR} --gen py -out ${PYTHON_GEN_DIR} ${THRIFT_FILE_PATH}
        COMMAND ${CP_COMMAND} ${THRIFT_GEN_DIR}/*.h ${THRIFT_INCLUDE_DIR}
        COMMAND touch ${THRIFT_GEN_FILES} # seem to need this to ensure dependencies are honored after a re-build of the generated files
        COMMAND touch ${THRIFT_PYTHON_GEN_FILES} # seem to need this to ensure dependencies are honored after a re-build of the generated files
        DEPENDS ${THRIFT_FILE_PATH}
        COMMENT "Generating C++ and Python thrift files for ${THRIFT_FILE_PATH}"
    )

    # target to ensure that all files are built prior to start of compilation
    add_custom_target(thrift_generated_${THRIFT_FILE_NAME}
        DEPENDS ${THRIFT_GEN_FILES} ${THRIFT_PYTHON_GEN_FILES}
    )

    # set the name of the library based on the thrift file name
    set(THRIFT_LIB_NAME thrift_${THRIFT_FILE_NAME})

    # build the library
    add_library(${THRIFT_LIB_NAME} ${THRIFT_GEN_FILES})
    add_dependencies(${THRIFT_LIB_NAME} thrift_generated_${THRIFT_FILE_NAME})
    target_link_libraries(${THRIFT_LIB_NAME} PRIVATE thrift::thrift thriftnb::thriftnb)

    # add the gen-cpp dir to the list of files to remove during 'make clean'
    set_target_properties(${THRIFT_LIB_NAME}
        PROPERTIES ADDITIONAL_CLEAN_FILES "${THRIFT_GEN_DIR};${THRIFT_INCLUDE_DIR};${THRIFT_PYTHON_GEN_DIR}"
    )

endforeach(THRIFT_FILE)

