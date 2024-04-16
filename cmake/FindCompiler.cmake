# Check for operating system
include(CheckFunctionExists)

if(NOT DEFINED CMAKE_SYSTEM_NAME)
  message(FATAL_ERROR "Unsupported operating system!")
endif()

# Linux
if(CMAKE_SYSTEM_NAME MATCHES Linux)
  find_program(GCC_C_COMPILER gcc)
  find_program(GCC_CXX_COMPILER g++)

  # Verify GCC is the actual compiler (not Clang)
  check_function_exists(main "" HAS_GCC_C_COMPILER)

  set(CMAKE_C_COMPILER ${GCC_C_COMPILER})
  set(CMAKE_CXX_COMPILER ${GCC_CXX_COMPILER})

# macOS (GCC version check)
elseif(CMAKE_SYSTEM_NAME MATCHES Darwin)
  find_program(CLANG_C_COMPILER clang)
  find_program(GCC_C_COMPILER gcc-13)
  find_program(GCC_CXX_COMPILER g++-13)

  # Ensure GCC is used, not Clang
  if(CLANG_C_COMPILER AND NOT GCC_C_COMPILER)
    message(FATAL_ERROR "Clang compiler found. Please install or use a compatible GCC version (>= 13).")
  endif()

  set(CMAKE_C_COMPILER ${GCC_C_COMPILER})
  set(CMAKE_CXX_COMPILER ${GCC_CXX_COMPILER})
endif()

if(NOT GCC_C_COMPILER)
  message(FATAL_ERROR "GCC compiler not found. Please install or use a compatible GCC version (>= 13). Use `brew install gcc@13` on MacOSX.")
endif()

# Verify GCC version is >= 13
execute_process(
    COMMAND ${GCC_CXX_COMPILER} --version
    OUTPUT_VARIABLE GCC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REGEX MATCH "clang" HAS_CLANG ${GCC_VERSION})
string(REGEX REPLACE "^.* ([0-9]+)\.[0-9]+.*$" "\\1" GCC_MAJOR_VERSION ${GCC_VERSION})

if(HAS_CLANG)
  message(FATAL_ERROR "Clang compiler found. Please install or use a compatible GCC version.")
endif()

if(GCC_MAJOR_VERSION LESS 13)
  message(FATAL_ERROR "GCC version detected is less than 13. Please install or use a compatible GCC version (>= 13).")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -std=c++20")
