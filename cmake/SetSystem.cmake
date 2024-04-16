## Detect build system and set options

## Set LINUX to TRUE/FALSE
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    set(LINUX TRUE)
    message(STATUS "Detected Linux build env")
    # Set linux specific build flags
else()
    set(LINUX FALSE)
endif()

EXECUTE_PROCESS( COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE )

## Set MACOSX to TRUE/FALSE
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(MACOSX TRUE)
    if (${ARCHITECTURE} MATCHES "arm64")
       set(CMAKE_OSX_ARCHITECTURES "arm64")
    endif()
    message(STATUS "Detected MacOSX build env for ${ARCHITECTURE}")
    # Set mac specific build flags
    # Linker flags
    # set(CMAKE_EXE_LINKER_FLAGS "-Wl,-object_path_lto,lto.o ${CMAKE_EXE_LINKER_FLAGS}")
else()
    set(MACOSX FALSE)
endif()

include(FindCompiler)

if(FALSE)

# look for gcc/g++
SET(GCC_SEARCH_PATHS
    /usr/bin
    /bin
    /usr/local/bin
    /opt/bin
    /opt/homebrew/bin
)

if (MACOSX)
    set(GCC "gcc-13")
    set(GXX "g++-13")
else()
    set(GCC "gcc")
    set(GXX "g++")
endif()

FIND_PATH(GCC_PATH
    NAMES ${GCC}
    PATHS ${GCC_SEARCH_PATHS}
    REQUIRED
)

FIND_PATH(GCC_XX_PATH
    NAMES ${GXX}
    PATHS ${GCC_SEARCH_PATHS}
    REQUIRED
)

message(STATUS "Found gcc at: ${GCC_PATH}/{GCC}")
message(STATUS "Found g++ at: ${GCC_XX_PATH}/{GXX}")

# set compiler to clang
set(CMAKE_C_COMPILER ${GCC_PATH}/{GCC})
set(CMAKE_CXX_COMPILER ${GCC_XX_PATH}/{GXX})

execute_process(
    COMMAND ${GCC_PROGRAM} -dumpversion
    OUTPUT_VARIABLE GCC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (GCC_VERSION VERSION_LESS "13")
    message(FATAL_ERROR "GCC version 13 or higher is required.")
endif()

endif()