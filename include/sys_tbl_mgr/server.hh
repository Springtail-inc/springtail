#pragma once

#include <common/grpc_server_manager.hh>
#include <common/service_register.hh>
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

class SysTblMgrRunner : public ServiceRunner {
public:
    explicit SysTblMgrRunner() :
        ServiceRunner("SysTblMgr") {}

    bool start() override {
        Server::get_instance()->startup();
        return true;
    }

    void stop() override {
        Server::shutdown();
    }
};

}  // namespace springtail::sys_tbl_mgr
