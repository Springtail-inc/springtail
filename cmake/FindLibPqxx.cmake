# try and find libpqxx

SET(PQXX_SEARCH_PATHS
    ~/Library/Frameworks
    /Library/Frameworks
    /usr/local
    /usr
    /opt/local # DarwinPorts
    /opt
    /opt/homebrew
)

FIND_PATH(PQXX_INCLUDE_DIR
    NAMES pqxx
    PATH_SUFFIXES include/pqxx include
    PATHS ${PQXX_SEARCH_PATHS}
)

if(CMAKE_SIZEOF_VOID_P EQUAL 8) 
    set(PATH_SUFFIXES lib64 lib/x64 lib)
else() 
    set(PATH_SUFFIXES lib/x86 lib)
endif() 

FIND_LIBRARY(PQXX_LIBRARY
    NAMES pqxx
    PATH_SUFFIXES ${PATH_SUFFIXES}
    PATHS ${PQXX_SEARCH_PATHS}
)

FIND_LIBRARY(PQ_LIBRARY
    NAMES pq
    PATH_SUFFIXES ${PATH_SUFFIXES}
    PATHS ${PQXX_SEARCH_PATHS}
)

if(PQXX_LIBRARY)
    message(STATUS "Found libpqxx: ${PQXX_LIBRARY}")
else()
    message(FATAL_ERROR "Libpqxx not found, it is required")
endif()

if(PQ_LIBRARY)
    message(STATUS "Found libpq: ${PQ_LIBRARY}")
else()
    message(FATAL_ERROR "Libpq not found, it is required")
endif()

if(PQXX_INCLUDE_DIR)
    message(STATUS "Found pqxx include dir: ${PQXX_INCLUDE_DIR}/pqxx")
else()
    message(FATAL_ERROR "Pqxx include dir not found, it is required")
endif()

