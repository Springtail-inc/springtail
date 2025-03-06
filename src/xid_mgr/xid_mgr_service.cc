#include <iostream>

#include <grpc/grpc_server.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail {

grpc::Status
GrpcXidMgrService::Ping(grpc::ServerContext* context,
                        const google::protobuf::Empty* request,
                        google::protobuf::Empty* response)
{
    ServerSpan span(context, "XidMgrService", "Ping");

    try {
        std::cout << "Got ping\n";
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status
GrpcXidMgrService::CommitXid(grpc::ServerContext* context,
                             const proto::CommitXidRequest* request,
                             google::protobuf::Empty* response)
{
    ServerSpan span(context, "XidMgrService", "CommitXid");

    try {
        xid_mgr::XidMgrServer* server = xid_mgr::XidMgrServer::get_instance();
        server->commit_xid(request->db_id(), request->xid(), request->has_schema_changes());

        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status
GrpcXidMgrService::RecordDdlChange(grpc::ServerContext* context,
                                   const proto::RecordDdlChangeRequest* request,
                                   google::protobuf::Empty* response)
{
    ServerSpan span(context, "XidMgrService", "RecordDdlChange");

    try {
        xid_mgr::XidMgrServer* server = xid_mgr::XidMgrServer::get_instance();
        server->record_ddl_change(request->db_id(), request->xid());
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

grpc::Status
GrpcXidMgrService::GetCommittedXid(grpc::ServerContext* context,
                                   const proto::GetCommittedXidRequest* request,
                                   proto::GetCommittedXidResponse* response)
{
    ServerSpan span(context, "XidMgrService", "GetCommittedXid");

    try {
        xid_mgr::XidMgrServer* server = xid_mgr::XidMgrServer::get_instance();
        uint64_t xid = server->get_committed_xid(request->db_id(), request->schema_xid());
        response->set_xid(xid);
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

}  // namespace springtail
