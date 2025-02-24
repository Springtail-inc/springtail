cmake_minimum_required(VERSION 3.25)

## Need to set top level include for conan, must be before project
# disable for now
# set(CMAKE_PROJECT_TOP_LEVEL_INCLUDES "./cmake/conan_provider.cmake")

## project definition
project(Springtail
  VERSION 0.1
  DESCRIPTION "Springtail source code"
  LANGUAGES CXX)

## add ctest framework
include(CTest)

## check for vcpkg install
if (NOT IS_DIRECTORY "${PROJECT_SOURCE_DIR}/external/vcpkg")
    message(FATAL_ERROR "Error: external/vcpkg does not exist, run vcpkg.sh")
endif()

## Cmake modules in ./cmake (for finding libraries)
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(SetSystem)

## include the vcpkg.cmake configuration to support find_package from vcpkg
include("${PROJECT_SOURCE_DIR}/external/vcpkg/scripts/buildsystems/vcpkg.cmake")

## add command add_dsymutil_command() to run dsymutil against an executable when on macOS
include(DSymUtil)

## add make install target to copy git hash to install directory
include(GitInstall)

## CXX options
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wno-sign-compare -Wuninitialized -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "-g3 -gz")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_SKIP_TEST_ALL_DEPENDENCY FALSE)

## add checks for thread data races: https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -O2")
#set(LINK_FLAGS_DEBUG "${LINK_FLAGS_DEBUG} -fsanitize=thread")

## Default location of properties file
add_compile_definitions(SPRINGTAIL_PROPERTIES="${PROJECT_SOURCE_DIR}/system.json")

## globally required packages
find_package(cpptrace)
find_package(spdlog)
find_package(Boost REQUIRED thread)
find_package(GTest CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

# Boost settings
set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
set(Boost_USE_DEBUG_LIBS        OFF)  # ignore debug libs and
set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME     ON)  # link Boost Static libraries

## enable google testing framework
enable_testing()
include(GoogleTest)

add_definitions( -D CXX_BYTE_ORDER=${CMAKE_CXX_BYTE_ORDER} )

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  # set debug level to debug for debug builds
  add_definitions( -D SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG )
  set( CMAKE_EXPORT_COMPILE_COMMANDS ON )
else()
  add_definitions( -D SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO )
endif()

## include external projects
include(ExternalProjects)

## set install directory
set(CMAKE_INSTALL_PREFIX "${PROJECT_SOURCE_DIR}/install" CACHE PATH "Installation Directory" FORCE)
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set_property(CACHE CMAKE_INSTALL_PREFIX PROPERTY VALUE "${PROJECT_SOURCE_DIR}/install")
endif()

## source modules
set(MODULES
  benchmarks
  common
  redis
  storage
  pg_repl
  write_cache
  sys_tbl_mgr
  pg_log_mgr
  xid_mgr
  proto
  proxy
  pg_fdw
  test
)

## iterate over modules and add subdirectory
## also configure CTestCustom.cmake for each module
foreach(MODULE ${MODULES})
  add_subdirectory(src/${MODULE})
  set(MODULE_BIN_DIR ${CMAKE_BINARY_DIR}/src/${MODULE})
  configure_file(${PROJECT_SOURCE_DIR}/scripts/CTestCustom.cmake.in ${MODULE_BIN_DIR}/CTestCustom.cmake @ONLY)
endforeach(MODULE ${MODULES})

## configure CTestCustom.cmake for top level
configure_file(${PROJECT_SOURCE_DIR}/scripts/CTestCustom.cmake.in ${PROJECT_BINARY_DIR}/CTestCustom.cmake @ONLY)

## Messages keep at end of file
message(STATUS "Build type: " ${CMAKE_BUILD_TYPE})
message(STATUS "System name: " ${CMAKE_SYSTEM_NAME})
message(STATUS "Compiler: " ${CMAKE_CXX_COMPILER})
message(STATUS "Compiler flags: " ${CMAKE_CXX_COMPILE_FLAGS})
message(STATUS "Compiler cxx debug flags: " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "Compiler cxx release flags: " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "Compiler cxx flags:" ${CMAKE_CXX_FLAGS})
message(STATUS "Byte order for target: " ${CMAKE_CXX_BYTE_ORDER})
