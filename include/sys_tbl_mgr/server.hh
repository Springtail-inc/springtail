#pragma once

#include <common/grpc_server_manager.hh>
#include <common/singleton.hh>

namespace springtail::sys_tbl_mgr {

class Server final : public Singleton<Server> {
    friend class Singleton<Server>;

public:
    void startup();

    ~Server() override = default;

private:
    Server();

    GrpcServerManager _grpc_server_manager;

    void _internal_shutdown() override;
};

}  // namespace springtail::sys_tbl_mgr
