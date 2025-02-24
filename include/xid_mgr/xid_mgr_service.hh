#pragma once

#include <common/singleton.hh>
#include <common/logging.hh>

#include <proto/xid_manager.grpc.pb.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

namespace springtail {

    /**
     * @brief This is the implementation of the XidManager gRPC service that is generated
     *        from the .proto file. It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class GrpcXidMgrService final : public proto::XidManager::Service, public Singleton<GrpcXidMgrService>
    {
        friend class Singleton<GrpcXidMgrService>;
    public:
        grpc::Status Ping(grpc::ServerContext* context,
                         const google::protobuf::Empty* request,
                         google::protobuf::Empty* response) override;

        grpc::Status CommitXid(grpc::ServerContext* context,
                              const proto::CommitXidRequest* request,
                              google::protobuf::Empty* response) override;

        grpc::Status RecordDdlChange(grpc::ServerContext* context,
                                    const proto::RecordDdlChangeRequest* request,
                                    google::protobuf::Empty* response) override;

        grpc::Status GetCommittedXid(grpc::ServerContext* context,
                                    const proto::GetCommittedXidRequest* request,
                                    proto::GetCommittedXidResponse* response) override;

    private:
        GrpcXidMgrService() = default;
        ~GrpcXidMgrService() = default;
    };
}
