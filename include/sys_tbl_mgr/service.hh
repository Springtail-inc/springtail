#pragma once

#include <common/logging.hh>
#include <common/singleton.hh>
#include <grpcpp/grpcpp.h>
#include <proto/sys_tbl_mgr.grpc.pb.h>
#include <sys_tbl_mgr/table.hh>
#include <sys_tbl_mgr/system_tables.hh>

namespace springtail::sys_tbl_mgr {
    class Server;

/**
 * This is the implementation of the gRPC SysTblMgr service defined in sys_tbl_mgr.proto.
 * It contains the service (handler) for actually implementing the remote procedure calls.
 *
 * The Service object maintains specialized caches of system metadata for uncommitted changes to
 * the system tables that are used to retrieve uncommitted metadata changes needed by in-flight
 * data mutations since there is currently no way to query a MutableTable.  We currently don't
 * cache any metadata once it's written to disk, instead relying on the StorageCache to keep
 * table extents in-memory for fast retrieval.
 */
class Service final : public proto::SysTblMgr::Service {
public:

    explicit Service(Server &server) : _srv(server) {}

    /** Simple interface to help ensure that the server is still running. */
    grpc::Status Ping(grpc::ServerContext* context,
                      const google::protobuf::Empty* request,
                      google::protobuf::Empty* response) override;

    /** Retrieves the user defined type at a given XID/LSN. */
    grpc::Status GetUserType(grpc::ServerContext* context,
                             const proto::GetUserTypeRequest* request,
                             proto::GetUserTypeResponse* response) override;

    /** Retrieve the root extents and stats for a given table. */
    grpc::Status GetRoots(grpc::ServerContext* context,
                          const proto::GetRootsRequest* request,
                          proto::GetRootsResponse* response) override;

    /** Retrieve the schema information for a given table at a given XID/LSN. */
    grpc::Status GetSchema(grpc::ServerContext* context,
                           const proto::GetSchemaRequest* request,
                           proto::GetSchemaResponse* response) override;

    /** Retrieve the schema information for a given table at a given XID/LSN along with the
        history of any changes to bring it to a target XID/LSN. */
    grpc::Status GetTargetSchema(grpc::ServerContext* context,
                                 const proto::GetTargetSchemaRequest* request,
                                 proto::GetSchemaResponse* response) override;

    /** Returns NOT_FOUND if the table does not exist at a given XID. */
    grpc::Status Exists(grpc::ServerContext* context,
                        const proto::ExistsRequest* request,
                        google::protobuf::Empty* response) override;

private:
    Server &_srv;
};
}  // namespace springtail::sys_tbl_mgr
