# Check for operating system
include(CheckFunctionExists)

if(NOT DEFINED CMAKE_SYSTEM_NAME)
  message(FATAL_ERROR "Unsupported operating system!")
endif()

# Function to validate gcc compiler
function(check_gcc_function RESULT_VAR COMPILER)
    if(NOT COMPILER)
        message(FATAL_ERROR "GCC compiler not found!")
    endif()

    # Get the version of the compiler using --version
    execute_process(
        COMMAND ${COMPILER} --version
        OUTPUT_VARIABLE GCC_FULL_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    # make sure the output of --version does not contain "clang" and is >= 13.3
    string(REGEX MATCH "clang" HAS_CLANG ${GCC_FULL_VERSION})

    # use -dumpversion to get actual version number
    execute_process(
        COMMAND ${COMPILER} -dumpversion
        OUTPUT_VARIABLE GCC_MAJOR_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    if(NOT GCC_MAJOR_VERSION OR GCC_MAJOR_VERSION LESS 13)
        set(${RESULT_VAR} FALSE PARENT_SCOPE)
    endif()

    if(HAS_CLANG)
        set(${RESULT_VAR} FALSE PARENT_SCOPE)
    endif()

    message(STATUS "Compiler check: ${COMPILER}, ${GCC_VERSION}, ${RESULT_VAR}")
endfunction()

# Define the search paths
set(SEARCH_PATHS
  "/usr/bin"
  "/usr/local/bin"
  "/opt/bin"
  "/opt/homebrew/bin"
)

# Find the GCC compiler
find_program(GCC_C_COMPILER
    NAMES gcc-13
    HINTS ${SEARCH_PATHS} ENV PATH
    VALIDATOR check_gcc_function
)

# Find the G++ compiler
find_program(GCC_CXX_COMPILER
    NAMES g++-13
    HINTS ${SEARCH_PATHS} ENV PATH
    VALIDATOR check_gcc_function
)

# Check if the GCC compiler was found
if (NOT GCC_C_COMPILER OR NOT GCC_CXX_COMPILER)
    message(FATAL_ERROR "GCC compiler >= 13.0 not found!")
else()
    message(STATUS "GCC compiler found: ${GCC_C_COMPILER}")
    message(STATUS "GCC++ compiler found: ${GCC_CXX_COMPILER}")
endif()

set(CMAKE_C_COMPILER ${GCC_C_COMPILER})
set(CMAKE_CXX_COMPILER ${GCC_CXX_COMPILER})

message(STATUS "CMAKE_C_COMPILER: ${CMAKE_C_COMPILER}")
message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")

set(ENV{CC} ${GCC_C_COMPILER})
set(ENV{CXX} ${GCC_CXX_COMPILER})
