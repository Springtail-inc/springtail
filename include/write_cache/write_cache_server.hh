#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <common/singleton.hh>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_service.hh>
#include <thrift/common/thrift_server.hh>

namespace springtail {

    class WriteCacheServer final :
        public springtail::thrift::Server<WriteCacheServer,
                                        thrift::write_cache::ThriftWriteCacheProcessorFactory,
                                        ThriftWriteCacheService,
                                        thrift::write_cache::ThriftWriteCacheIfFactory,
                                        thrift::write_cache::ThriftWriteCacheIf>,
        public Singleton<WriteCacheServer>
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

    private:
        /**
         * @brief Construct a new Write Cache Server object
         */
        WriteCacheServer();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~WriteCacheServer() override = default;

        /** shutdown from shutdown(), called once */
        void _internal_shutdown();

        /** indexes mutex */
        std::mutex _mutex;

        /** map of indexes by db_id */
        std::map<uint64_t, WriteCacheIndexPtr> _indexes;
    };

} // namespace springtail
