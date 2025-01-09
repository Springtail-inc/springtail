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

        int worker_thread_count;
        int port;
        Json::get_to<int>(server_json, "port", port, 55051);
        Json::get_to<int>(server_json, "worker_threads", worker_thread_count, 8);

        init(worker_thread_count, port);
    }

    void
    WriteCacheServer::_internal_shutdown()
    {
        stop();
    }
}
