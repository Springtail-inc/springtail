#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

//#include <write_cache/write_cache_index.hh>

namespace springtail {

    class WriteCacheIndex;

    class WriteCacheServer
    {
    public:
        /**
         * @brief Get the singleton write cache server instance object
         * @return WriteCacheServer *
         */
        static WriteCacheServer *get_instance();

        /**
         * @brief Shutdown cache
         */
        static void shutdown();

        /**
         * @brief Startup server; does not return
         */
        void startup();

        /**
         * @brief Get the write cache index object
         * @return std::shared_ptr<WriteCacheIndex>
         */
        inline std::shared_ptr<WriteCacheIndex> get_index() {
            return _index;
        }

    protected:
        /** Singleton write cache server instance */
        static WriteCacheServer *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        /**
         * @brief Construct a new Write Cache Server object
         */
        WriteCacheServer();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~WriteCacheServer() {}

    private:
        // delete copy constructor
        WriteCacheServer(const WriteCacheServer &) = delete;
        void operator=(const WriteCacheServer &)   = delete;

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;

        std::shared_ptr<WriteCacheIndex> _index;
    };

} // namespace springtail