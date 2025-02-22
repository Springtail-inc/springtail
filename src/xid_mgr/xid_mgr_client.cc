#include <absl/log/log.h>
#include <google/protobuf/empty.pb.h>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <xid_mgr/xid_mgr_client.hh>

namespace springtail {

XidMgrClient::XidMgrClient()
{
    nlohmann::json json = Properties::get(Properties::XID_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the xid mgr client
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("XID Mgr RPC settings are not found");
    }

    std::string server = Properties::get_xid_mgr_hostname();
    _channel = create_channel(server, rpc_json);
    _stub = proto::XidManager::NewStub(_channel);
}

void
XidMgrClient::ping()
{
    retry_rpc(
        [this]() {
            grpc::ClientContext context;
            google::protobuf::Empty request;
            google::protobuf::Empty response;
            return _stub->Ping(&context, request, &response);
        },
        "Ping");
}

void
XidMgrClient::commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_changes)
{
    proto::CommitXidRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);
    request.set_has_schema_changes(has_schema_changes);
    google::protobuf::Empty response;

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->CommitXid(&context, request, &response);
        },
        "CommitXid");
}

void
XidMgrClient::record_ddl_change(uint64_t db_id, uint64_t xid)
{
    proto::RecordDdlChangeRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);
    google::protobuf::Empty response;

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->RecordDdlChange(&context, request, &response);
        },
        "RecordDdlChange");
}

uint64_t
XidMgrClient::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
{
    proto::GetCommittedXidRequest request;
    request.set_db_id(db_id);
    request.set_schema_xid(schema_xid);
    proto::GetCommittedXidResponse response;

    retry_rpc(
        [&]() {
            grpc::ClientContext context;
            return _stub->GetCommittedXid(&context, request, &response);
        },
        "GetCommittedXid");

    return response.xid();
}

}  // namespace springtail
