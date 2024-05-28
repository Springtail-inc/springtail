## Detect build system and set options

## Set LINUX to TRUE/FALSE
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    set(LINUX TRUE)
    message(STATUS "Detected Linux build env")
    # Set linux specific build flags
    add_compile_definitions(LINUX)
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
