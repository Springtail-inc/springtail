# source files
set(IPC_SOURCES event_loop.cc watcher.cc signal_watcher.cc pid_watcher.cc timer_watcher.cc io_watcher.cc)

find_package(absl CONFIG REQUIRED)

## main library
add_library(springtail_ipc ${IPC_SOURCES})
target_include_directories(springtail_ipc
  PRIVATE ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(springtail_ipc
  PUBLIC absl::check)
