#include <common/json.hh>
#include <common/properties.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>

namespace springtail {

WriteCacheServer::WriteCacheServer()
{
    springtail_register_service(ServiceId::WriteCacheServerId, WriteCacheServer::shutdown);
    auto json = Properties::get(Properties::WRITE_CACHE_CONFIG);
    nlohmann::json rpc_json;

    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("WriteCache RPC settings are not found");
    }

    _grpc_server_manager.init(rpc_json);
    _grpc_server_manager.addService(WriteCacheService::get_instance());
    _startup();
}

void
WriteCacheServer::_startup()
{
    _grpc_server_manager.startup();
}

void
WriteCacheServer::_internal_shutdown()
{
    _grpc_server_manager.shutdown();
    WriteCacheService::shutdown();
}

}  // namespace springtail
