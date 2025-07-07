#include <common/common.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/service.hh>

namespace springtail::sys_tbl_mgr {

Server::Server()
{
    auto json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the sys_tbl_mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("SysTblMgr RPC settings are not found");
    }

    _grpc_server_manager.init(rpc_json);
    _grpc_server_manager.addService(Service::get_instance());
}

void
Server::start()
{
    get_instance()->_grpc_server_manager.startup();
}

void
Server::_internal_shutdown()
{
    _grpc_server_manager.shutdown();
    Service::shutdown();
}

}  // namespace springtail::sys_tbl_mgr
