#include <common/json.hh>
#include <common/properties.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>

namespace springtail {

WriteCacheServer::WriteCacheServer()
{
    auto json = Properties::get(Properties::WRITE_CACHE_CONFIG);
    nlohmann::json rpc_json;

    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("WriteCache RPC settings are not found");
    }

    _grpc_server_manager.init(rpc_json);
    _grpc_server_manager.addService(WriteCacheService::get_instance());
}

void
WriteCacheServer::startup()
{
    _grpc_server_manager.startup();
}

void
WriteCacheServer::shutdown()
{
    _grpc_server_manager.shutdown();
}

}  // namespace springtail
