# Find required packages
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Find the protoc compiler
find_program(PROTOC protoc
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

# Find the gRPC cpp plugin
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

# Find the gRPC python plugin
find_program(GRPC_PYTHON_PLUGIN grpc_python_plugin
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

message(STATUS "Found protoc: ${PROTOC}")
message(STATUS "Found grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")

# Get all .proto files in the directory
file(GLOB PROTO_FILES "*.proto")

# Iterate over all proto files to generate cpp files and link them into libraries
foreach(PROTO_FILE ${PROTO_FILES})
    # Extract proto base file name and set paths
    get_filename_component(PROTO_FILE_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_FILE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE_NAME}.proto)
    set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR})

    message(STATUS "Processing proto file: ${PROTO_FILE_PATH}")

    # Set the generated files
    set(PROTO_GEN_FILES
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}.pb.h
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}.pb.cc
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}.grpc.pb.h
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}.grpc.pb.cc
    )

    # Set the generated Python files
    set(PROTO_PYTHON_FILES
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}_pb2.py
        ${PROTO_GEN_DIR}/${PROTO_FILE_NAME}_pb2_grpc.py
    )

    # Add custom command to generate the proto files and copy headers to include directory
    add_custom_command(
        OUTPUT ${PROTO_GEN_FILES} ${PROTO_PYTHON_FILES}
        COMMAND mkdir -p ${PROTO_GEN_DIR}
        COMMAND ${PROTOC}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${PROTO_GEN_DIR}
            --grpc_out=${PROTO_GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            ${PROTO_FILE_PATH}

        # Generate Python files
        COMMAND ${PROTOC}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --python_out=${PROTO_GEN_DIR}
            --grpc_python_out=${PROTO_GEN_DIR}  # Update Python output directory
            --plugin=protoc-gen-grpc_python=${GRPC_PYTHON_PLUGIN}
            ${PROTO_FILE_PATH}

        COMMAND touch ${PROTO_GEN_FILES} ${PROTO_PYTHON_FILES} # Ensure dependencies are honored after rebuild
        DEPENDS ${PROTO_FILE_PATH}
        COMMENT "Generating proto files for ${PROTO_FILE_PATH} into ${PROTO_GEN_DIR}"
    )

    # Target to ensure all files are built prior to compilation
    add_custom_target(proto_generated_${PROTO_FILE_NAME}
        DEPENDS ${PROTO_GEN_FILES} ${PROTO_PYTHON_FILES}
    )

    # Set the name of the library based on the proto file name
    set(PROTO_LIB_NAME proto_${PROTO_FILE_NAME})

    # Build the library
    add_library(${PROTO_LIB_NAME} ${PROTO_GEN_FILES})
    add_dependencies(${PROTO_LIB_NAME} proto_generated_${PROTO_FILE_NAME})
    target_link_libraries(${PROTO_LIB_NAME}
        PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
    )

    # Add the generated directory to include paths
    target_include_directories(${PROTO_LIB_NAME}
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
    )

    # Set the Python copy directory
    set(PYTHON_COPY_DIR "${CMAKE_SOURCE_DIR}/python/grpc/${PROTO_FILE_NAME}")
    set(PYTHON_COPY_FILES
        ${PYTHON_COPY_DIR}/${PROTO_FILE_NAME}_pb2.py
        ${PYTHON_COPY_DIR}/${PROTO_FILE_NAME}_pb2_grpc.py)

    # Create a custom target to copy Python files to a specific location (optional)
    add_custom_command(
        OUTPUT ${PYTHON_COPY_FILES}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${PYTHON_COPY_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy
            ${PROTO_PYTHON_FILES}
            ${PYTHON_COPY_DIR}
        DEPENDS ${PROTO_PYTHON_FILES}
        COMMENT "Copying generated Python files for ${PROTO_FILE_NAME} to ${PYTHON_COPY_DIR}"
    )

    # Create a custom target that depends on the copied files
    add_custom_target(copy_python_protos_${PROTO_FILE_NAME} ALL
        DEPENDS ${PYTHON_COPY_FILES}
    )

    # Add a dependency to ensure it runs after proto generation
    add_dependencies(copy_python_protos_${PROTO_FILE_NAME} proto_generated_${PROTO_FILE_NAME})
    add_dependencies(${PROTO_LIB_NAME} copy_python_protos_${PROTO_FILE_NAME})

    # Add gen directory to clean files
    set_target_properties(${PROTO_LIB_NAME}
        PROPERTIES ADDITIONAL_CLEAN_FILES "${PROTO_GEN_DIR} ${PYTHON_COPY_FILES}"
    )

endforeach(PROTO_FILE)