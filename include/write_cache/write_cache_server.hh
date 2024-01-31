#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

namespace springtail {

    class WriteCacheIndex;

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
        void startup();

        /**
         * @brief Get the write cache index object
         * @return std::shared_ptr<WriteCacheIndex>
         */
        std::shared_ptr<WriteCacheIndex> get_index() {
            return _index;
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

        /** Singleton write cache server instance */
        static WriteCacheServer *_instance;

        /** init flag */
        static std::once_flag _init_flag;
        /** shutdown flag */
        static std::once_flag _shutdown_flag;

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;

        std::shared_ptr<WriteCacheIndex> _index;
    };

} // namespace springtail