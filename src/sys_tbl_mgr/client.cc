#include <grpcpp/grpcpp.h>
#include <proto/sys_tbl_mgr.grpc.pb.h>

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
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/exception.hh>
#include <sys_tbl_mgr/request_helper.hh>

namespace springtail::sys_tbl_mgr {

Client::Client() : Singleton<Client>(ServiceId::SysTblMgrClientId)
{
    nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    std::string server = Properties::get_sys_tbl_mgr_hostname();
    uint64_t cache_size;

    if (!Json::get_to<uint64_t>(json, "cache_size", cache_size)) {
        throw Error("Sys tbl mgr cache size settings not found");
    }

    nlohmann::json rpc_json;
    if (!Json::get_to<nlohmann::json>(json, "rpc_config", rpc_json)) {
        throw Error("Sys tbl mgr RPC settings not found");
    }

    auto channel = grpc_client::create_channel("SysTblMgr", server, rpc_json);
    _stub = proto::SysTblMgr::NewStub(channel);
    _schema_cache = std::make_shared<SchemaCache>(cache_size);
}

void
Client::ping()
{
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    grpc_client::retry_rpc("SysTblMgr", "Ping",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->Ping(context, request, &response);
                           });
}

void Client::use_roots_cache(std::shared_ptr<ShmCache> c)
{
    _roots_cache.store(std::move(c));
}

void Client::use_schema_cache(std::shared_ptr<ShmCache> c)
{
    _schema_shm_cache.store(std::move(c));
}

void Client::use_usertype_cache(std::shared_ptr<ShmCache> c)
{
    _usertype_shm_cache.store(std::move(c));
}

TableMetadataPtr
Client::get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    proto::GetRootsResponse response;
    bool found = false;

    auto cache = _roots_cache.load();
    if (cache) {
        auto msg = cache->find(db_id, table_id, xid);
        if (msg) {
            found = response.ParseFromString(msg.value());
            CHECK(found);
        }
    }

    if (!found) {
        proto::GetRootsRequest request;
        request.set_db_id(db_id);
        request.set_table_id(table_id);
        request.set_xid(xid);

        grpc_client::retry_rpc("SysTblMgr", "GetRoots",
                               [this, &request, &response](grpc::ClientContext *context) {
                                   return _stub->GetRoots(context, request, &response);
                               });

        if (cache) {
            std::string msg = response.SerializeAsString();
            cache->insert(db_id, table_id, xid, msg);
        }
    }

    auto metadata = std::make_shared<TableMetadata>();
    for (const auto &root : response.roots()) {
        metadata->roots.push_back({root.index_id(), root.extent_id()});
    }
    metadata->stats.row_count = response.stats().row_count();
    metadata->stats.end_offset = response.stats().end_offset();
    metadata->stats.last_internal_row_id = response.stats().last_internal_row_id();
    metadata->snapshot_xid = response.snapshot_xid();

    return metadata;
}

std::shared_ptr<const SchemaMetadata>
Client::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    // First check the shared memory cache
    // note: if shared memory cache is used, in-process cache is bypassed
    auto shm_cache = _schema_shm_cache.load();
    if (shm_cache) {
        auto msg = shm_cache->find(db_id, table_id, xid);
        if (msg) {
            proto::GetSchemaResponse response;
            bool parsed = response.ParseFromString(msg.value());
            if (parsed) {
                return RequestHelper::pack_metadata(response);
            }
        }
        proto::GetSchemaRequest request;
        request.set_db_id(db_id);
        request.set_table_id(table_id);
        request.set_xid(xid.xid);
        request.set_lsn(xid.lsn);

        proto::GetSchemaResponse response;
        grpc_client::retry_rpc("SysTblMgr", "GetSchema",
                               [this, &request, &response](grpc::ClientContext *context) {
                                   return _stub->GetSchema(context, request, &response);
                               });
        auto response_str = response.SerializeAsString();
        shm_cache->insert(db_id, table_id, xid, response_str);
        return RequestHelper::pack_metadata(response);
    }

    // Use the in-process cache
    auto populate = [this, shm_cache](uint64_t db, uint64_t tid, const XidLsn &xid) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "populate callback called for: db:tid {}:{}, xid:lsn {}:{}",
                db, tid, xid.xid, xid.lsn);
        proto::GetSchemaRequest request;
        request.set_db_id(db);
        request.set_table_id(tid);
        request.set_xid(xid.xid);
        request.set_lsn(xid.lsn);

        proto::GetSchemaResponse response;
        grpc_client::retry_rpc("SysTblMgr", "GetSchema",
                               [this, &request, &response](grpc::ClientContext *context) {
                                   return _stub->GetSchema(context, request, &response);
                               });

        return RequestHelper::pack_metadata(response);
    };

    // Retrieve through the in-process schema cache
    return _schema_cache->get(db_id, table_id, xid, populate);
}

SchemaMetadataPtr
Client::get_target_schema(uint64_t db_id,
                          uint64_t table_id,
                          const XidLsn &access_xid,
                          const XidLsn &target_xid)
{
    proto::GetTargetSchemaRequest request;
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.set_access_xid(access_xid.xid);
    request.set_access_lsn(access_xid.lsn);
    request.set_target_xid(target_xid.xid);
    request.set_target_lsn(target_xid.lsn);

    proto::GetSchemaResponse response;
    grpc_client::retry_rpc("SysTblMgr", "GetTargetSchema",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->GetTargetSchema(context, request, &response);
                           });

    return RequestHelper::pack_metadata(response);
}

bool
Client::exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    // prepare the request
    proto::ExistsRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(table_id);
    google::protobuf::Empty response;

    auto status = grpc_client::retry_rpc_status(
        "SysTblMgr", "Exists", [this, &request, &response](grpc::ClientContext *context) {
            return _stub->Exists(context, request, &response);
        });

    if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
        return false;
    } else if (!status.ok()) {
        throw SysTblMgrError(status.error_message());
    }

    return true;
}

void
Client::invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    _schema_cache->invalidate_table(db_id, table_id, xid);
}

void
Client::invalidate_db(uint64_t db_id, const XidLsn &xid)
{
    _schema_cache->invalidate_db(db_id, xid);
}

std::shared_ptr<UserType>
Client::get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid)
{
    // First check the shared memory cache
    auto shm_cache = _usertype_shm_cache.load();
    if (shm_cache) {
        auto msg = shm_cache->find(db_id, type_id, xid);
        if (msg) {
            proto::GetUserTypeResponse response;
            bool parsed = response.ParseFromString(msg.value());
            if (parsed) {
                auto user_type = std::make_shared<UserType>(response.type_id(),
                                                            response.namespace_id(),
                                                            response.type(),
                                                            response.name(),
                                                            response.value_json(),
                                                            response.exists());
                return user_type;
            }
        }
    }

    // Cache miss or no cache - fetch from server
    proto::GetUserTypeRequest request;
    request.set_db_id(db_id);
    request.set_type_id(type_id);
    request.set_xid(xid.xid);
    request.set_lsn(xid.lsn);

    proto::GetUserTypeResponse response;
    grpc_client::retry_rpc("SysTblMgr", "GetUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->GetUserType(context, request, &response);
                           });

    // Insert into cache if cache is available
    if (shm_cache) {
        auto msg = response.SerializeAsString();
        shm_cache->insert(db_id, type_id, xid, msg);
    }

    if (!response.exists()) {
        return std::make_shared<UserType>(response.type_id(), false);
    }

    return std::make_shared<UserType>(response.type_id(),
                                                response.namespace_id(),
                                                response.type(),
                                                response.name(),
                                                response.value_json(),
                                                response.exists());
}

}  // namespace springtail::sys_tbl_mgr
