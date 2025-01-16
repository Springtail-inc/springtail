#include <iostream>
#include <mutex>
#include <memory>

#include <nlohmann/json.hpp>

#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_service.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail {

    WriteCacheServer::WriteCacheServer()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json rpc_json;

        // fetch RPC properties for the write cache server
        if (!Json::get_to(json, "rpc_config", rpc_json)) {
            throw Error("Write cache RPC settings are not found");
        }

        init(rpc_json);
    }

    void
    WriteCacheServer::_internal_shutdown()
    {
        stop();
    }
}
