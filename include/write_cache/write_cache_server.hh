#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <write_cache/write_cache_fbs.grpc.fb.h>

namespace springtail {

    class WriteCacheService : public GrpcWriteCache::Service
    {
    public:
        /** These functions match those in the write_cache_fbs.fbs schema definition */

        virtual ::grpc::Status AddRows(::grpc::ServerContext* context, 
                                       const flatbuffers::grpc::Message<AddRowRequest>* request, 
                                       flatbuffers::grpc::Message<StatusResponse>* response) override;

        virtual ::grpc::Status GetRow(::grpc::ServerContext* context,
                                      const flatbuffers::grpc::Message<GetRowRequest>* request,
                                      flatbuffers::grpc::Message<RowResponse>* response) override;

        virtual ::grpc::Status GetRows(::grpc::ServerContext* context,
                                       const flatbuffers::grpc::Message<GetRowsRequest>* request,
                                       flatbuffers::grpc::Message<RowsResponse>* response) override;

        virtual ::grpc::Status RemoveExtent(::grpc::ServerContext* context, 
                                            const flatbuffers::grpc::Message<RemoveExtentRequest>* request, 
                                            flatbuffers::grpc::Message<StatusResponse>* response) override;

        virtual ::grpc::Status Evict(::grpc::ServerContext* context,
                                     const flatbuffers::grpc::Message<EvictRequest>* request,
                                     flatbuffers::grpc::Message<StatusResponse>* response) override;

        virtual ::grpc::Status ListExtents(::grpc::ServerContext* context,
                                           const flatbuffers::grpc::Message<ListExtentsRequest>* request, 
                                           flatbuffers::grpc::Message<ListExtentsResponse>* response) override;

    private:
        flatbuffers::grpc::MessageBuilder _mb;
    };

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
         WriteCacheServer() {}

    private:
        // delete copy constructor
        WriteCacheServer(const WriteCacheServer &) = delete;
        void operator=(const WriteCacheServer &)   = delete;

        std::string _server_host;
        int _thread_count;
        std::unique_ptr<grpc::Server> _server;
        std::vector<std::thread> _threads;
    };

} // namespace springtail