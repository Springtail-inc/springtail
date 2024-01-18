cmake_minimum_required(VERSION 3.22)

## Need to set top level include for conan, must be before project
# disable for now
# set(CMAKE_PROJECT_TOP_LEVEL_INCLUDES "./cmake/conan_provider.cmake")

## project definition
project(Springtail
    VERSION 0.1
    DESCRIPTION "Springtail source code"
    LANGUAGES CXX)

## check for vcpkg install
if (NOT IS_DIRECTORY "${PROJECT_SOURCE_DIR}/external/vcpkg")
    message(FATAL_ERROR "Error: external/vcpkg does not exist, run vcpkg.sh")
endif()

## include the vcpkg.cmake configuration to support find_package from vcpkg
include("${PROJECT_SOURCE_DIR}/external/vcpkg/scripts/buildsystems/vcpkg.cmake")

## CXX options
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

## add checks for thread data races: https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -O2")
#set(LINK_FLAGS_DEBUG "${LINK_FLAGS_DEBUG} -fsanitize=thread")

## Default location of properties file
add_compile_definitions(SPRINGTAIL_PROPERTIES="${PROJECT_SOURCE_DIR}/system.json")

## Cmake modules in ./cmake (for finding libraries)
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(SetSystem)

find_library(LIB_ELF NAMES elf PATHS /usr/local/lib)
find_library(LIB_DWARF NAMES dwarf PATHS /usr/local/lib)

find_package(Psql)
find_package(Backward CONFIG REQUIRED)
find_package(spdlog)
# find_package(Boost REQUIRED boost-program-options)

## source modules
add_subdirectory(src/common)
add_subdirectory(src/storage)
add_subdirectory(src/benchmarks)
add_subdirectory(src/psql_cdc)
add_subdirectory(src/write_cache)

## Messages keep at end of file
message(STATUS "Build type: " ${CMAKE_BUILD_TYPE})
message(STATUS "Compiler: " ${CMAKE_CXX_COMPILER})
message(STATUS "Compiler flags: " ${CMAKE_CXX_COMPILE_FLAGS})
message(STATUS "Compiler cxx debug flags: " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "Compiler cxx release flags: " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "Compiler cxx flags:" ${CMAKE_CXX_FLAGS})
message(STATUS "Byte order for target: " ${CMAKE_CXX_BYTE_ORDER})

