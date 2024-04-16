find_package(nlohmann_json CONFIG REQUIRED)

# source files
set(XID_MGR_SOURCES
    xid_mgr_client.cc
    xid_mgr_server.cc
    xid_mgr_service.cc
)

# add the library target
add_library(springtail_xid_mgr ${XID_MGR_SOURCES})
target_include_directories(springtail_xid_mgr PRIVATE ${CMAKE_SOURCE_DIR}/include)
<<<<<<< Updated upstream
target_link_libraries(springtail_xid_mgr PRIVATE springtail_common thrift_xid_mgr nlohmann_json::nlohmann_json)
=======
target_link_libraries(springtail_xid_mgr
    PRIVATE thrift_xid_mgr nlohmann_json::nlohmann_json
)
>>>>>>> Stashed changes

add_executable(xid_mgr_daemon xid_mgr_daemon.cc)
target_include_directories(xid_mgr_daemon PRIVATE ${CMAKE_SOURCE_DIR}/include ${VCPKG_INCLUDE_PATH})
target_link_libraries(xid_mgr_daemon
<<<<<<< Updated upstream
    PRIVATE springtail_common
    PRIVATE springtail_xid_mgr
=======
    PUBLIC springtail_common
           springtail_xid_mgr
>>>>>>> Stashed changes
)
