find_package(gRPC CONFIG REQUIRED)

set(COMMON_LIB_SOURCES
  grpc_server_manager.cc
)

add_library(springtail_grpc ${COMMON_LIB_SOURCES})
target_include_directories(springtail_grpc PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_grpc
  PUBLIC gRPC::grpc++
)

