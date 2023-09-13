

set(SOURCES src/cdc.cc)

add_library(libcdc SHARED ${SOURCES})

target_include_directories(libcdc
    PUBLIC include
    PRIVATE ${PQXX_INCLUDE_DIR})

target_link_libraries(libcdc
    PRIVATE ${PQXX_LIBRARY}
    PRIVATE ${PQ_LIBRARY})
