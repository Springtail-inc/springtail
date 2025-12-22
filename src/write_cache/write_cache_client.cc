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

WriteCacheClient::WriteCacheClient() : Singleton<WriteCacheClient>(ServiceId::WriteCacheClientId)
{
    nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
    nlohmann::json rpc_json;

    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("Write cache RPC settings are not found");
    }

    std::string server = Properties::get_write_cache_hostname();
    _channel = grpc_client::create_channel("WriteCache", server, rpc_json);
    _stub = proto::WriteCache::NewStub(_channel);
}

void
WriteCacheClient::ping()
{
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    grpc_client::retry_rpc("WriteCache", "Ping",
                           [this, &request, &response](grpc::ClientContext* context) {
                               return _stub->Ping(context, request, &response);
                           });
}

std::vector<uint64_t>
WriteCacheClient::list_tables(uint64_t db_id, uint64_t xid, uint32_t count, uint64_t& cursor)
{
    proto::ListTablesRequest request;
    proto::ListTablesResponse response;

    request.set_db_id(db_id);
    request.set_xid(xid);
    request.set_count(count);
    request.set_cursor(cursor);

    grpc_client::retry_rpc("WriteCache", "ListTables",
                           [this, &request, &response](grpc::ClientContext* context) {
                               return _stub->ListTables(context, request, &response);
                           });

    cursor = response.cursor();
    return std::vector<uint64_t>(response.table_ids().begin(), response.table_ids().end());
}

std::vector<WriteCacheClient::WriteCacheExtent>
WriteCacheClient::get_extents(uint64_t db_id,
                              uint64_t tid,
                              uint64_t xid,
                              uint32_t count,
                              uint64_t& cursor,
                              PostgresTimestamp& commit_ts)
{
    proto::GetExtentsResponse response;
    bool found = false;

    // Try cache first if starting from beginning
    auto cache = _extents_cache.load();
    if (cache && cursor == 0) {
        auto msg = cache->find(db_id, tid, xid);
        if (msg) {
            found = response.ParseFromString(msg.value());
            DCHECK(found);
        }
    }

    if (!found) {
        proto::GetExtentsRequest request;
        request.set_db_id(db_id);
        request.set_table_id(tid);
        request.set_xid(xid);
        request.set_count(count);
        request.set_cursor(cursor);

        grpc_client::retry_rpc("WriteCache", "GetExtents",
                               [this, &request, &response](grpc::ClientContext* context) {
                                   return _stub->GetExtents(context, request, &response);
                               });
    }

    if (cache && cursor == 0 && response.extents_size() < count) {
        // we only cache complete responses starting from beginning
        auto response_str = response.SerializeAsString();
        cache->insert(db_id, tid, xid, response_str);
    }

    CHECK_EQ(response.table_id(), tid);
    cursor = response.cursor();
    commit_ts = PostgresTimestamp(response.commit_ts());

    std::vector<WriteCacheExtent> extents;
    for (const auto& e : response.extents()) {
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

    grpc_client::retry_rpc("WriteCache", "EvictTable",
                           [this, &request, &response](grpc::ClientContext* context) {
                               return _stub->EvictTable(context, request, &response);
                           });
}

void
WriteCacheClient::evict_xid(uint64_t db_id, uint64_t xid)
{
    proto::EvictXidRequest request;
    google::protobuf::Empty response;

    request.set_db_id(db_id);
    request.set_xid(xid);

    grpc_client::retry_rpc("WriteCache", "EvictXid",
                           [this, &request, &response](grpc::ClientContext* context) {
                               return _stub->EvictXid(context, request, &response);
                           });
}

void
WriteCacheClient::use_extents_cache(std::shared_ptr<sys_tbl_mgr::ShmCache> c)
{
    _extents_cache.store(std::move(c));
}

}  // namespace springtail
