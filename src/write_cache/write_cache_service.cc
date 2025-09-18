#include <write_cache/write_cache_service.hh>

#include <common/json.hh>
#include <common/properties.hh>
#include <grpc/grpc_server.hh>
#include <nlohmann/json.hpp>
#include <proto/write_cache.grpc.pb.h>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail {

grpc::Status WriteCacheService::Ping(grpc::ServerContext* context,
                    const google::protobuf::Empty* request,
                    google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "Ping");
    std::cout << "Got ping\n";
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::GetExtents(grpc::ServerContext* context,
                            const proto::GetExtentsRequest* request,
                            proto::GetExtentsResponse* response)
{
    ServerSpan span(context, "WriteCacheService", "GetExtents");
    WriteCacheServer* server = WriteCacheServer::get_instance();
    WriteCacheIndexPtr index = server->get_index(request->db_id());

    uint64_t cursor = request->cursor();
    WriteCacheTableSet::Metadata md;
    std::vector<WriteCacheIndexExtentPtr> extents = index->get_extents(
        request->table_id(), request->xid(), request->count(), cursor, md);

    for (const auto& e : extents) {
        auto* extent = response->add_extents();
        extent->set_xid(e->xid);
        extent->set_xid_seq(e->xid_seq);
        extent->set_data(e->data->serialize());
    }

    response->set_cursor(cursor);
    response->set_table_id(request->table_id());
    response->set_commit_ts(md.pg_commit_ts.micros());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::EvictTable(grpc::ServerContext* context,
                              const proto::EvictTableRequest* request,
                              google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "EvictTable");
    WriteCacheServer* server = WriteCacheServer::get_instance();
    WriteCacheIndexPtr index = server->get_index(request->db_id());

    index->evict_table(request->table_id(), request->xid());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::EvictXid(grpc::ServerContext* context,
                            const proto::EvictXidRequest* request,
                            google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "EvictXid");
    WriteCacheServer* server = WriteCacheServer::get_instance();
    WriteCacheIndexPtr index = server->get_index(request->db_id());

    index->evict_xid(request->xid());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::ListTables(grpc::ServerContext* context,
                              const proto::ListTablesRequest* request,
                              proto::ListTablesResponse* response)
{
    ServerSpan span(context, "WriteCacheService", "ListTables");
    WriteCacheServer* server = WriteCacheServer::get_instance();
    WriteCacheIndexPtr index = server->get_index(request->db_id());

    uint64_t cursor = request->cursor();

    auto&& tids = index->get_tids(request->xid(), request->count(), cursor);
    for (auto tid : tids) {
        response->add_table_ids(tid);
    }

    response->set_cursor(cursor);
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

}  // namespace springtail
