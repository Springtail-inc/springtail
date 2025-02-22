#pragma once

#include <memory>

#include <common/grpc_server_manager.hh>
#include <common/singleton.hh>

namespace springtail::sys_tbl_mgr {

class Server final : public Singleton<Server> {
    friend class Singleton<Server>;

public:
    void startup();
    void shutdown();

    ~Server() override = default;

private:
    Server();

    GrpcServerManager _grpc_server_manager;
};

}  // namespace springtail::sys_tbl_mgr
