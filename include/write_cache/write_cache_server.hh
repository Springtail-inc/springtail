#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <thrift/server/TServer.h>
#include <thrift/concurrency/ThreadManager.h>

#include <common/singleton.hh>
#include <write_cache/write_cache_index.hh>

namespace springtail {

    class WriteCacheServer : public Singleton<WriteCacheServer>
    {
        friend class Singleton<WriteCacheServer>;
    public:
        /**
         * @brief Startup server; does not return
         */
        static void startup() {
            // start the server
            auto server = get_instance();
            server->_startup();
        }

        void stop() {
            _server->stop();
            _thread_manager->stop();
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
         ~WriteCacheServer() override = default;

        /** init from get_instance, called once */
        // static WriteCacheServer *_init();

        /** shutdown from shutdown(), called once */
        void _internal_shutdown();

        /** startup from startup(), called once */
        void _startup();

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** thread manager that is used by the server */
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> _thread_manager = {nullptr};

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
