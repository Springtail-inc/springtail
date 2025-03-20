#pragma once

#include <common/singleton.hh>
#include <proto/write_cache.grpc.pb.h>

namespace springtail {

/**
 * @brief gRPC service implementation for the WriteCache service
 */
class WriteCacheService final : public Singleton<WriteCacheService>,
                                public proto::WriteCache::Service {
    friend class Singleton<WriteCacheService>;

public:
    // Service method implementations
    grpc::Status Ping(grpc::ServerContext* context,
                      const google::protobuf::Empty* request,
                      google::protobuf::Empty* response) override;

    grpc::Status GetExtents(grpc::ServerContext* context,
                            const proto::GetExtentsRequest* request,
                            proto::GetExtentsResponse* response) override;

    grpc::Status EvictTable(grpc::ServerContext* context,
                            const proto::EvictTableRequest* request,
                            google::protobuf::Empty* response) override;

    grpc::Status EvictXid(grpc::ServerContext* context,
                          const proto::EvictXidRequest* request,
                          google::protobuf::Empty* response) override;

    grpc::Status ListTables(grpc::ServerContext* context,
                            const proto::ListTablesRequest* request,
                            proto::ListTablesResponse* response) override;

private:
    WriteCacheService() = default;
    ~WriteCacheService() override = default;
};

}  // namespace springtail
