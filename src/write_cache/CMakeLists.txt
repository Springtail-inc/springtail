find_package(Thrift CONFIG REQUIRED)

# the flatbuffers compiler flatc
find_program(
    THRIFT_COMPILER 
    thrift
    HINTS ${PROJECT_SOURCE_DIR}/external/vcpkg/packages/
    REQUIRED
)

message(STATUS "Found thrift compiler: ${THRIFT_COMPILER}")

# set variables for thrift schema .thrift file and generated .h
set(THRIFT_SCHEMA "write_cache.thrift")
set(THRIFT_GEN_PATH "${CMAKE_CURRENT_SOURCE_DIR}/gen-cpp")
set(THRIFT_CC_FILES "${THRIFT_GEN_PATH}/ThriftWriteCache.cpp;${THRIFT_GEN_PATH}/ThriftWriteCache_server.skeleton.cpp;${THRIFT_GEN_PATH}/write_cache_types.cpp")
set(THRIFT_HH_FILES "${THRIFT_GEN_PATH}/ThriftWriteCache.h;${THRIFT_GEN_PATH}/write_cache_types.h")

set(GENERATED_FILES ${THRIFT_CC_FILES} ${THRIFT_HH_FILES})

# command to generate .h/.cc files from the .fbs file
add_custom_command(
    PRE_BUILD
    OUTPUT ${GENERATED_FILES}
    COMMAND ${THRIFT_COMPILER} -o ${CMAKE_CURRENT_SOURCE_DIR} --gen cpp:no_skeleton,pure_enums ${THRIFT_SCHEMA}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${THRIFT_SCHEMA}
    COMMENT "Generating: thrift rpc files: ${THRIFT_COMPILER} -o ${CMAKE_CURRENT_SOURCE_DIR} --gen cpp ${THRIFT_SCHEMA}"
    VERBATIM
)

# target to ensure that all files are built prior to start of compilation
add_custom_target(generated_thrift
    DEPENDS ${GENERATED_FILES}
)

set_source_files_properties(${GENERATED_FILES} PROPERTIES GENERATED TRUE)

set(WRITE_CACHE_SOURCES write_cache_client.cc write_cache_server.cc ${THRIFT_CC_FILES})

# add the library target
add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
add_dependencies(springtail_write_cache generated_thrift)

# add generated .cc/.h files to set of clean files
set_target_properties(springtail_write_cache 
    PROPERTIES ADDITIONAL_CLEAN_FILES ${GENERATED_FILES}
)

target_include_directories(springtail_write_cache
    PRIVATE ${CMAKE_SOURCE_DIR}/include ${THRIFT_GEN_PATH}
)

target_link_libraries(springtail_write_cache 
    PUBLIC springtail_common 
    PRIVATE thrift::thrift thriftz::thriftz
)