include(FindGit)
include(FetchContent)

# Fetch the libpg_query from the springtail fork which includes
# a CMakeLists.txt to more easily integrate with current source code
FetchContent_Declare(
     libpg_query
<<<<<<< HEAD
     GIT_REPOSITORY git@github.com:Springtail-inc/libpg_query.git
=======
     GIT_REPOSITORY https://github.com/Springtail-inc/libpg_query.git #git@github.com:Springtail-inc/libpg_query.git
>>>>>>> origin/main
     GIT_TAG 16-latest
     SOURCE_DIR ${CMAKE_SOURCE_DIR}/external/libpg_query
)

# NOTE: this could also have been done with:
# add_subdirectory(${CMAKE_SOURCE_DIR}/external/libpg_query)
# either way seems fine, the build location is a bit different

FetchContent_GetProperties(libpg_query)
if(NOT libpg_query_POPULATED)
    FetchContent_Populate(libpg_query)
    add_subdirectory(${libpg_query_SOURCE_DIR} ${libpg_query_BINARY_DIR})
endif()
