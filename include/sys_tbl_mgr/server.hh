#pragma once

#include <grpc/grpc_server_manager.hh>
#include <common/init.hh>
#include <common/singleton.hh>

namespace springtail::sys_tbl_mgr {

class Server final : public Singleton<Server>,
                     public AutoRegisterShutdown<Server, ServiceId::SysTblMgrServerId>
{
    friend class Singleton<Server>;
public:
    static void start();

private:
    Server();
    ~Server() override = default;

    GrpcServerManager _grpc_server_manager;

    void _internal_shutdown() override;
};

}  // namespace springtail::sys_tbl_mgr
