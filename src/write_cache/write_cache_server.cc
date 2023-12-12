#include <grpc++/grpc++.h>
#include <flatbuffers/flatbuffers.h>
#include <nlohmann/json.hpp>

#include <thread>

#include <common/properties.hh>
#include <common/json.hh>

#include <write_cache/write_cache_fbs_generated.h>
#include <write_cache/write_cache_server.hh>

namespace springtail {
    
    void worker_thread(std::unique_ptr<grpc::ServerCompletionQueue> cq)
    {
        while(true) {
            void* tag;  // uniquely identifies a request.
            bool ok;
            if (!cq->Next(&tag, &ok) || !ok) {
                // shutdown
                break;
            }
        }

        cq->Shutdown();
    }

    /* static initialization must happen outside of class */
    WriteCacheServer* WriteCacheServer::_instance {nullptr};
    std::mutex WriteCacheServer::_instance_mutex;

    WriteCacheServer *
    WriteCacheServer::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCacheServer();
        }

        return _instance;
    }

    WriteCacheServer::WriteCacheServer()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;        
        
        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        if (!Json::get_to<std::string>(server_json, "host", _server_host)) {
            throw Error("Write cache 'server.host' setting not found");
        }

        Json::get_to<int>(server_json, "threads", _thread_count);
    }

    void
    WriteCacheServer::startup()
    {
        WriteCacheService service;
        grpc::ServerBuilder builder;

        builder.AddListeningPort(_server_host, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        for (int i = 0; i < _thread_count; i++) {
            std::unique_ptr<grpc::ServerCompletionQueue> cq = builder.AddCompletionQueue();
            _threads.push_back(std::thread(worker_thread, cq));
        }

        _server = builder.BuildAndStart();
        _server->Wait();

        for (int i = 0; i < _thread_count; i++) {
            _threads[i].join();
        }
        _threads.erase();
    }

    void
    WriteCacheServer::shutdown()
    {
        WriteCacheServer *wc = get_instance();
        wc->_server->Shutdown();
    }

    grpc::Status 
    WriteCacheService::AddRows(grpc::ServerContext* context, 
                               const flatbuffers::grpc::Message<AddRowRequest>* request, 
                               flatbuffers::grpc::Message<StatusResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }

    grpc::Status
    WriteCacheService::GetRow(grpc::ServerContext* context,
                              const flatbuffers::grpc::Message<GetRowRequest>* request,
                              flatbuffers::grpc::Message<RowResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }

    grpc::Status
    WriteCacheService::GetRows(grpc::ServerContext* context,
                               const flatbuffers::grpc::Message<GetRowsRequest>* request,
                               flatbuffers::grpc::Message<RowsResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }

    grpc::Status
    WriteCacheService::RemoveExtent(grpc::ServerContext* context, 
                                    const flatbuffers::grpc::Message<RemoveExtentRequest>* request, 
                                    flatbuffers::grpc::Message<StatusResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }

    grpc::Status
    WriteCacheService::Evict(grpc::ServerContext* context,
                             const flatbuffers::grpc::Message<EvictRequest>* request,
                             flatbuffers::grpc::Message<StatusResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }

    grpc::Status 
    WriteCacheService::ListExtents(grpc::ServerContext* context,
                                   const flatbuffers::grpc::Message<ListExtentsRequest>* request, 
                                   flatbuffers::grpc::Message<ListExtentsResponse>* response)
    {
        // Return an OK status.
        return grpc::Status::OK;
    }
}

int main (void)
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());

}