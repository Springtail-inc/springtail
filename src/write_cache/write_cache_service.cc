#include <write_cache/write_cache_service.hh>

#include <common/json.hh>
#include <common/properties.hh>
#include <common/grpc_server.hh>
#include <nlohmann/json.hpp>
#include <proto/write_cache.grpc.pb.h>
#include <write_cache/extent_mapper.hh>
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
    PostgresTimestamp commit_ts;
    std::vector<WriteCacheIndexExtentPtr> extents = index->get_extents(
        request->table_id(), request->xid(), request->count(), cursor, commit_ts);

    for (const auto& e : extents) {
        auto* extent = response->add_extents();
        extent->set_xid(e->xid);
        extent->set_xid_seq(e->xid_seq);
        extent->set_data(e->data->serialize());
    }

    response->set_cursor(cursor);
    response->set_table_id(request->table_id());
    response->set_commit_ts(commit_ts.micros());

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

grpc::Status WriteCacheService::SetLookup(grpc::ServerContext* context,
                             const proto::SetLookupRequest* request,
                             google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "SetLookup");
    ExtentMapper* mapper = ExtentMapper::get_instance(request->db_id());
    mapper->set_lookup(request->table_id(), request->target_xid(), request->extent_id());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::ForwardMap(grpc::ServerContext* context,
                              const proto::ForwardMapRequest* request,
                              proto::ExtentMapResponse* response)
{
    ServerSpan span(context, "WriteCacheService", "ForwardMap");
    ExtentMapper* mapper = ExtentMapper::get_instance(request->db_id());
    auto&& result =
        mapper->forward_map(request->table_id(), request->target_xid(), request->extent_id());

    for (const auto& eid : result) {
        response->add_extent_ids(eid);
    }

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::ReverseMap(grpc::ServerContext* context,
                              const proto::ReverseMapRequest* request,
                              proto::ExtentMapResponse* response)
{
    ServerSpan span(context, "WriteCacheService", "ReverseMap");
    ExtentMapper* mapper = ExtentMapper::get_instance(request->db_id());
    auto&& result = mapper->reverse_map(request->table_id(), request->access_xid(),
                                        request->target_xid(), request->extent_id());

    for (const auto& eid : result) {
        response->add_extent_ids(eid);
    }

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::ExpireMap(grpc::ServerContext* context,
                             const proto::ExpireMapRequest* request,
                             google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "ExpireMap");
    ExtentMapper* mapper = ExtentMapper::get_instance(request->db_id());
    mapper->expire(request->table_id(), request->commit_xid());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status WriteCacheService::AddMapping(grpc::ServerContext* context,
                              const proto::AddMappingRequest* request,
                              google::protobuf::Empty* response)
{
    ServerSpan span(context, "WriteCacheService", "AddMapping");
    ExtentMapper* mapper = ExtentMapper::get_instance(request->db_id());

    std::vector<uint64_t> new_eids(request->new_eids().begin(), request->new_eids().end());

    mapper->add_mapping(request->table_id(), request->target_xid(), request->old_eid(), new_eids);

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

}  // namespace springtail
