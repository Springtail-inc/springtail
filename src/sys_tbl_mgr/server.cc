#include <iostream>
#include <mutex>
#include <memory>

#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <sys_tbl_mgr/server.hh>

namespace at = apache::thrift;

namespace springtail::sys_tbl_mgr {

    Server::Server()
    {
        nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("SysTblMgr server settings not found");
        }

        int worker_thread_count = Json::get_or<int>(server_json, "worker_threads", 8);
        int port = Json::get_or<int>(server_json, "port", 55053);

        init(worker_thread_count, port);
    }

    void
    Server::_internal_shutdown()
    {
        stop();
    }
}
