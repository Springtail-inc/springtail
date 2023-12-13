find_package(ZeroMQ CONFIG REQUIRED)
find_package(cppzmq CONFIG REQUIRED)
find_package(flatbuffers CONFIG REQUIRED)

# the flatbuffers compiler flatc
find_program(
    FLATBUFFS_COMPILER 
    flatc
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

# set variables for flatbuffer schema .fbs file and generated .h
set(FLATBUFFER_SCHEMA "write_cache_fbs.fbs")
string(REPLACE ".fbs" "" FLATBUFFER_SCHEMA_BASE ${FLATBUFFER_SCHEMA})
set(FLATBUFFER_SCHEMA_H "${FLATBUFFER_SCHEMA_BASE}_generated.h")
set(FLATBUFFER_GEN_PATH "${CMAKE_SOURCE_DIR}/include/write_cache")

set(GENERATED_FILES ${FLATBUFFER_GEN_PATH}/${FLATBUFFER_SCHEMA_H})

# command to generate .h/.cc files from the .fbs file
add_custom_command(
    PRE_BUILD
    OUTPUT ${GENERATED_FILES}
    COMMAND ${FLATBUFFS_COMPILER} -o ${FLATBUFFER_GEN_PATH} --cpp --cpp-std c++17 ${FLATBUFFER_SCHEMA}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${FLATBUFFER_SCHEMA}
    COMMENT "Generating: flatbuffers and grpc stubs:  ${FLATBUFFS_COMPILER} -o ${FLATBUFFER_GEN_PATH} --grpc --cpp --cpp-std c++17 --gen-all ${FLATBUFFER_GEN_PATH}/${FLATBUFFER_SCHEMA}"
    VERBATIM
)

# target to ensure that all files are built prior to start of compilation
add_custom_target(generated_fbs
    DEPENDS ${GENERATED_FILES}
)

set_source_files_properties(${GENERATED_FILES} PROPERTIES GENERATED TRUE)

set(WRITE_CACHE_SOURCES write_cache_client.cc write_cache_server.cc)

# add the library target
add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
add_dependencies(springtail_write_cache generated_fbs)

# add generated .cc/.h files to set of clean files
set_target_properties(springtail_write_cache 
    PROPERTIES ADDITIONAL_CLEAN_FILES "${FLATBUFFER_GEN_PATH}/${FLATBUFFER_SCHEMA_H}")

target_include_directories(springtail_write_cache
    PRIVATE ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(springtail_write_cache 
    PUBLIC springtail_common 
    PRIVATE libzmq
)