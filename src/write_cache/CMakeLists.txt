find_package(Thrift CONFIG REQUIRED)

# the thrift rpc gen
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
set(THRIFT_CC_FILES "${THRIFT_GEN_PATH}/ThriftWriteCache.cpp;${THRIFT_GEN_PATH}/write_cache_types.cpp")
set(THRIFT_HH_FILES "${THRIFT_GEN_PATH}/ThriftWriteCache.h;${THRIFT_GEN_PATH}/write_cache_types.h")

set(GENERATED_FILES ${THRIFT_CC_FILES} ${THRIFT_HH_FILES})

# command to generate .h/.cc files from the .fbs file
add_custom_command(
    PRE_BUILD
    OUTPUT ${GENERATED_FILES}
    COMMAND ${THRIFT_COMPILER} -o ${CMAKE_CURRENT_SOURCE_DIR} --gen cpp:no_skeleton,pure_enums,moveable_types ${THRIFT_SCHEMA}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${THRIFT_SCHEMA}
    COMMENT "Generating: thrift rpc files: ${THRIFT_COMPILER} -o ${CMAKE_CURRENT_SOURCE_DIR} --gen cpp ${THRIFT_SCHEMA}"
    VERBATIM
)

# target to ensure that all files are built prior to start of compilation
add_custom_target(generated_thrift
    DEPENDS ${GENERATED_FILES}
)

# set the generated property on the generate files, this ensures they are cleaned by 'make clean'
set_source_files_properties(${GENERATED_FILES} PROPERTIES GENERATED TRUE)

# source files
set(WRITE_CACHE_SOURCES
    write_cache_client.cc
    write_cache_server.cc write_cache_service.cc write_cache_index.cc write_cache_table_set.cc
    write_cache_index_node.cc
    ${THRIFT_CC_FILES}
)

# add the library target
add_library(springtail_write_cache ${WRITE_CACHE_SOURCES})
add_dependencies(springtail_write_cache generated_thrift)

# add the gen-cpp dir to the list of files to remove during 'make clean'
set_target_properties(springtail_write_cache
    PROPERTIES ADDITIONAL_CLEAN_FILES ${THRIFT_GEN_PATH}
)

# include the thrift gen-cpp dir as an include path; so we don't have to move the .h files
target_include_directories(springtail_write_cache
    PRIVATE ${CMAKE_SOURCE_DIR}/include
            ${THRIFT_GEN_PATH}
)

target_link_libraries(springtail_write_cache
    PRIVATE thrift::thrift # thriftz::thriftz (optional, used for compression)
)

add_executable(test_threaded test/write_cache_index_test.cc)
target_include_directories(test_threaded PRIVATE ${CMAKE_SOURCE_DIR}/include ${THRIFT_GEN_PATH})
target_link_libraries(test_threaded
    PRIVATE springtail_common springtail_write_cache
    GTest::gtest GTest::gtest_main
)

