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
        nlohmann::json client_json;
        nlohmann::json server_json;

        if (!Json::get_to<nlohmann::json>(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        int worker_thread_count = Json::get_or<int>(server_json, "worker_threads", 8);
        int port = Json::get_or<int>(server_json, "port", 55051);

        init(worker_thread_count, port);
    }

    void
    WriteCacheServer::_internal_shutdown()
    {
        stop();
    }
}
