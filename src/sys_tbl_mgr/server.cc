#include <common/common.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <iostream>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sys_tbl_mgr/server.hh>

namespace at = apache::thrift;

namespace springtail::sys_tbl_mgr {

Server::Server()
{
    nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the sys_tbl_mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("SysTblMgr RPC settings are not found");
    }
    init(rpc_json);
}

void
Server::_internal_shutdown()
{
    stop();
}
}  // namespace springtail::sys_tbl_mgr
