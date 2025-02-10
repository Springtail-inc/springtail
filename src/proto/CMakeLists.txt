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

    # Add custom command to generate the proto files and copy headers to include directory
    add_custom_command(
        OUTPUT ${PROTO_GEN_FILES}
        COMMAND mkdir -p ${PROTO_GEN_DIR}
        COMMAND ${PROTOC}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${PROTO_GEN_DIR}
            --grpc_out=${PROTO_GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            ${PROTO_FILE_PATH}
        COMMAND touch ${PROTO_GEN_FILES}  # Ensure dependencies are honored after rebuild
        DEPENDS ${PROTO_FILE_PATH}
        COMMENT "Generating proto files for ${PROTO_FILE_PATH} into ${PROTO_GEN_DIR}"
    )

    # Target to ensure all files are built prior to compilation
    add_custom_target(proto_generated_${PROTO_FILE_NAME}
        DEPENDS ${PROTO_GEN_FILES}
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

    # Add gen directory to clean files
    set_target_properties(${PROTO_LIB_NAME}
        PROPERTIES ADDITIONAL_CLEAN_FILES "${PROTO_GEN_DIR}"
    )

endforeach(PROTO_FILE) 