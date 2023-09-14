cmake_minimum_required(VERSION 3.27)

## Need to set top level include for conan, must be before project
set(CMAKE_PROJECT_TOP_LEVEL_INCLUDES "./cmake/conan_provider.cmake")

## project definition
project(Springtail
    VERSION 0.1
    DESCRIPTION "Springtail source code"
    LANGUAGES CXX)


## CXX options
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

## Cmake modules in ./cmake (for finding libraries)
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(SetSystem)

## Source modules in ./modules
add_subdirectory(modules/cdc)

## Messages keep at end of file
message(STATUS "Build type: " ${CMAKE_BUILD_TYPE})
message(STATUS "Compiler: " ${CMAKE_CXX_COMPILER})
message(STATUS "Compiler flags: " ${CMAKE_CXX_COMPILE_FLAGS})
message(STATUS "Compiler cxx debug flags: " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "Compiler cxx release flags: " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "Compiler cxx flags:" ${CMAKE_CXX_FLAGS})


