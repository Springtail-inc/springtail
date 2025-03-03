#pragma once

#include <map>
#include <mutex>
#include <memory>

#include <grpc/grpc_server_manager.hh>
#include <common/service_register.hh>
#include <common/singleton.hh>
#include <write_cache/write_cache_index.hh>

namespace springtail {

    class WriteCacheServer final : public Singleton<WriteCacheServer>
    {
        friend class Singleton<WriteCacheServer>;
    public:
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

        void startup();

    private:
        WriteCacheServer();
        ~WriteCacheServer() override = default;

        /** indexes mutex */
        std::mutex _mutex;

        /** map of indexes by db_id */
        std::map<uint64_t, WriteCacheIndexPtr> _indexes;

        GrpcServerManager _grpc_server_manager;

        void _internal_shutdown() override;
    };

    class WriteCacheRunner : public ServiceRunner {
    public:
        explicit WriteCacheRunner() :
            ServiceRunner("WriteCacheServer") {}
        bool start() override {
            WriteCacheServer::get_instance()->startup();
            return true;
        }
        void stop() override {
            WriteCacheServer::shutdown();
        }
    };

} // namespace springtail
