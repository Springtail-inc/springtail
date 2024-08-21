#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <thrift/server/TServer.h>

#include <write_cache/write_cache_index.hh>

namespace springtail {

    class WriteCacheServer
    {
    public:
        /**
         * @brief Get the singleton write cache server instance object
         * @return WriteCacheServer *
         */
        static WriteCacheServer *get_instance() {
            std::call_once(_init_flag, &WriteCacheServer::_init);
            return _instance;
        }
        /**
         * @brief Shutdown cache
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &WriteCacheServer::_shutdown);
        }

        /**
         * @brief Startup server; does not return
         */
        static void startup() {
            // start the server
            auto server = get_instance();
            server->_startup();

            // after shutdown() we delete the instance
            delete _instance;
        }

        /**
         * @brief Get the write cache index object
         * @return std::shared_ptr<WriteCacheIndex>
         */
        std::shared_ptr<WriteCacheIndex> get_index(uint64_t db_id) {
            std::unique_lock lock(_mutex);
            auto it = _indexes.find(db_id);
            if (it == _indexes.end()) {
                it = _indexes.insert({db_id, std::make_shared<WriteCacheIndex>()}).first;
            }
            return it->second;
        }

        // delete copy constructor
        WriteCacheServer(const WriteCacheServer &) = delete;
        void operator=(const WriteCacheServer &)   = delete;

    private:
        /**
         * @brief Construct a new Write Cache Server object
         */
        WriteCacheServer();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~WriteCacheServer() {}

        /** init from get_instance, called once */
        static WriteCacheServer *_init();

        /** shutdown from shutdown(), called once */
        static void _shutdown();

        /** startup from startup(), called once */
        void _startup();

        /** Singleton write cache server instance */
        static WriteCacheServer *_instance;

        /** init flag */
        static std::once_flag _init_flag;
        /** shutdown flag */
        static std::once_flag _shutdown_flag;

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;

        /** indexes mutex */
        std::mutex _mutex;

        /** map of indexes by db_id */
        std::map<uint64_t, WriteCacheIndexPtr> _indexes;
    };

} // namespace springtail
