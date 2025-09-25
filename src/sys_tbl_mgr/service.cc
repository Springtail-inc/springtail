#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include <grpc/grpc_server.hh>
#include <sys_tbl_mgr/exception.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/service.hh>

namespace springtail::sys_tbl_mgr {

grpc::Status
Service::Ping(grpc::ServerContext* context,
              const google::protobuf::Empty* request,
              google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "Ping");
    return grpc::Status::OK;
}

grpc::Status
Service::GetRoots(grpc::ServerContext* context,
                  const proto::GetRootsRequest* request,
                  proto::GetRootsResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetRoots");

    LOG_INFO("got GetRoots(): db {} tid {} xid {}", request->db_id(), request->table_id(), request->xid());

    boost::shared_lock lock(_srv._read_mutex);

    XidLsn xid(request->xid(), constant::MAX_LSN);

    // make sure that the table exists at this XID
    auto table_info = _srv._get_table_info(request->db_id(), request->table_id(), xid);
    if (table_info == nullptr) {
        // We just return an empty response if the table doesn't exist
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk, "Table does not exist");
        return grpc::Status::OK;
    }

    // get the roots
    auto info = _srv._get_roots_info(request->db_id(), request->table_id(), xid);

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetSchema(grpc::ServerContext* context,
                   const proto::GetSchemaRequest* request,
                   proto::GetSchemaResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetSchema");

    LOG_INFO("got GetSchema() -- db {} tid {} xid {} lsn {}", request->db_id(),
             request->table_id(), request->xid(), request->lsn());

    boost::shared_lock lock(_srv._read_mutex);

    XidLsn xid(request->xid(), request->lsn());
    auto info = _srv._get_schema_info(request->db_id(), request->table_id(), xid, xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Returning start_xid {}:{}, end_xid {}:{}",
                        info->access_xid_start(), info->access_lsn_start(), info->access_xid_end(),
                        info->access_lsn_end());

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetTargetSchema(grpc::ServerContext* context,
                         const proto::GetTargetSchemaRequest* request,
                         proto::GetSchemaResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetTargetSchema");

    LOG_INFO("got GetTargetSchema() -- {}, {}", request->access_xid(), request->target_xid());

    boost::shared_lock lock(_srv._read_mutex);

    XidLsn access_xid(request->access_xid(), request->access_lsn());
    XidLsn target_xid(request->target_xid(), request->target_lsn());

    auto info = _srv._get_schema_info(request->db_id(), request->table_id(), access_xid, target_xid);

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::Exists(grpc::ServerContext* context,
                const proto::ExistsRequest* request,
                google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "Exists");

    LOG_INFO("got Exists()");

    boost::shared_lock lock(_srv._read_mutex);

    XidLsn xid(request->xid(), request->lsn());
    auto info = _srv._get_table_info(request->db_id(), request->table_id(), xid);
    if (info == nullptr) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, "Table not found");
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Table not found");
    }
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetUserType(grpc::ServerContext* context,
                     const proto::GetUserTypeRequest* request,
                     proto::GetUserTypeResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetUserType");

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got GetUserType() -- db {} type_id {} xid {} lsn {}", request->db_id(),
              request->type_id(), request->xid(), request->lsn());

    boost::shared_lock lock(_srv._read_mutex);

    XidLsn xid(request->xid(), constant::MAX_LSN);
    auto info = _srv._get_usertype_info(request->db_id(), request->type_id(), xid);
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2, "info is null? {}", info == nullptr);

    if (info != nullptr) {
        response->set_type_id(info->id);
        response->set_name(info->name);
        response->set_namespace_id(info->namespace_id);
        response->set_type(info->type);
        response->set_value_json(info->value_json);
        response->set_exists(info->exists);
    } else {
        response->set_exists(false);
    }

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

