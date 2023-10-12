## Detect build system and set options

# look for clang/clang++
SET(CLANG_SEARCH_PATHS
    /usr/bin
    /bin
    /usr/local/bin
    /opt/bin
    /opt/homebrew/bin)

FIND_PATH(CLANG_PATH
    NAMES clang
    PATHS ${CLANG_SEARCH_PATHS}
    REQUIRED)

FIND_PATH(CLANG_XX_PATH
    NAMES clang++
    PATHS ${CLANG_SEARCH_PATHS}
    REQUIRED)

message(STATUS "Found clang at: ${CLANG_PATH}/clang")
message(STATUS "Found clang++ at: ${CLANG_XX_PATH}/clang++")

# set compiler to clang
set(CMAKE_C_COMPILER ${CLANG_PATH}/clang)
set(CMAKE_CXX_COMPILER ${CLANG_XX_PATH}/clang++)

## Set LINUX to TRUE/FALSE
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    set(LINUX TRUE)
    message(STATUS "Detected Linux build env")
    # Set linux specific build flags
else()
    set(LINUX FALSE)
endif()

## Set MACOSX to TRUE/FALSE
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(MACOSX TRUE)
    message(STATUS "Detected MacOSX build env")
    # Set mac specific build flags
    # Linker flags to avoid no compact unwind errors
    set(CMAKE_EXE_LINKER_FLAGS "-Wl,-no_compact_unwind ${CMAKE_EXE_LINKER_FLAGS}")
else()
    set(MACOSX FALSE)
endif()
