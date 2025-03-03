#include <absl/log/check.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>
#include <proto/write_cache.grpc.pb.h>

#include <cassert>
#include <memory>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <grpc/grpc_client.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <write_cache/write_cache_client.hh>

namespace springtail {

WriteCacheClient::WriteCacheClient() : GrpcClient<WriteCacheClient>()
{
    nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
    nlohmann::json rpc_json;

    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("Write cache RPC settings are not found");
    }

    std::string server = Properties::get_write_cache_hostname();
    _channel = create_channel(server, rpc_json);
    _stub = proto::WriteCache::NewStub(_channel);
}

void
WriteCacheClient::ping()
{
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->Ping(&context, request, &response);
        },
        "Ping");
}

std::vector<uint64_t>
WriteCacheClient::list_tables(uint64_t db_id,
                             uint64_t xid,
                             uint32_t count,
                             uint64_t &cursor)
{
    proto::ListTablesRequest request;
    proto::ListTablesResponse response;

    request.set_db_id(db_id);
    request.set_xid(xid);
    request.set_count(count);
    request.set_cursor(cursor);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->ListTables(&context, request, &response);
        },
        "ListTables");

    cursor = response.cursor();
    return std::vector<uint64_t>(response.table_ids().begin(),
                                response.table_ids().end());
}

std::vector<WriteCacheClient::WriteCacheExtent>
WriteCacheClient::get_extents(uint64_t db_id,
                              uint64_t tid,
                              uint64_t xid,
                              uint32_t count,
                              uint64_t &cursor,
                              PostgresTimestamp &commit_ts)
{
    proto::GetExtentsRequest request;
    proto::GetExtentsResponse response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_xid(xid);
    request.set_count(count);
    request.set_cursor(cursor);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->GetExtents(&context, request, &response);
        },
        "GetExtents");

    CHECK_EQ(response.table_id(), tid);
    cursor = response.cursor();
    commit_ts = PostgresTimestamp(response.commit_ts());

    std::vector<WriteCacheExtent> extents;
    for (const auto &e : response.extents()) {
        WriteCacheExtent extent;
        extent.xid = e.xid();
        extent.lsn = e.xid_seq();
        extent.data = e.data();
        extents.push_back(std::move(extent));
    }
    return extents;
}

void
WriteCacheClient::evict_table(uint64_t db_id, uint64_t tid, uint64_t xid)
{
    proto::EvictTableRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_xid(xid);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->EvictTable(&context, request, &response);
        },
        "EvictTable");
}

void
WriteCacheClient::evict_xid(uint64_t db_id, uint64_t xid)
{
    proto::EvictXidRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_xid(xid);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->EvictXid(&context, request, &response);
        },
        "EvictXid");
}

void
WriteCacheClient::add_mapping(uint64_t db_id,
                              uint64_t tid,
                              uint64_t target_xid,
                              uint64_t old_eid,
                              const std::vector<uint64_t> &new_eids)
{
    proto::AddMappingRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_target_xid(target_xid);
    request.set_old_eid(old_eid);
    *request.mutable_new_eids() = {new_eids.begin(), new_eids.end()};

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->AddMapping(&context, request, &response);
        },
        "AddMapping");
}

void
WriteCacheClient::set_lookup(uint64_t db_id, uint64_t tid, uint64_t target_xid, uint64_t extent_id)
{
    proto::SetLookupRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_target_xid(target_xid);
    request.set_extent_id(extent_id);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->SetLookup(&context, request, &response);
        },
        "SetLookup");
}

std::vector<uint64_t>
WriteCacheClient::forward_map(uint64_t db_id,
                              uint64_t tid,
                              uint64_t target_xid,
                              uint64_t extent_id)
{
    proto::ForwardMapRequest request;
    proto::ExtentMapResponse response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_target_xid(target_xid);
    request.set_extent_id(extent_id);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->ForwardMap(&context, request, &response);
        },
        "ForwardMap");

    return std::vector<uint64_t>(response.extent_ids().begin(),
                                response.extent_ids().end());
}

std::vector<uint64_t>
WriteCacheClient::reverse_map(
    uint64_t db_id, uint64_t tid, uint64_t access_xid, uint64_t target_xid, uint64_t extent_id)
{
    proto::ReverseMapRequest request;
    proto::ExtentMapResponse response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_access_xid(access_xid);
    request.set_target_xid(target_xid);
    request.set_extent_id(extent_id);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->ReverseMap(&context, request, &response);
        },
        "ReverseMap");

    return std::vector<uint64_t>(response.extent_ids().begin(),
                                response.extent_ids().end());
}

void
WriteCacheClient::expire_map(uint64_t db_id, uint64_t tid, uint64_t commit_xid)
{
    proto::ExpireMapRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_table_id(tid);
    request.set_commit_xid(commit_xid);

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->ExpireMap(&context, request, &response);
        },
        "ExpireMap");
}

}  // namespace springtail
