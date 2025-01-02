find_package(Thrift CONFIG REQUIRED)

set(THRIFT_OVERRIDE_LIB_SOURCES
  concurrency/ThreadManager.cpp
  concurrency/ThreadFactory.cpp
  concurrency/Thread.cpp
  server/TThreadPoolServer.cpp
  server/TServerFramework.cpp
  transport/TServerSocket.cpp
  transport/TSocket.cpp
)

add_library(springtail_thrift_override ${THRIFT_OVERRIDE_LIB_SOURCES})
target_include_directories(springtail_thrift_override PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(springtail_thrift_override
    PUBLIC thrift::thrift
)

