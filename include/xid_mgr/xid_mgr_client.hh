#pragma once

#include <memory>

#include <common/init.hh>
#include <grpcpp/grpcpp.h>
#include <proto/xid_manager.grpc.pb.h>
#include <grpc/grpc_client.hh>

namespace springtail {

class XidMgrClient : public Singleton<XidMgrClient>,
                     public AutoRegisterShutdown<XidMgrClient, ServiceId::XidMgrClientId> {
    friend class Singleton<XidMgrClient>;

public:
    /**
     * @brief Ping the server
     */
    void ping();

    /**
     * @brief Get the latest committed xid
     * @param db_id database id
     * @param schema_xid last known schema xid
     * @return uint64_t latest committed xid
     */
    uint64_t get_committed_xid(uint64_t db_id, uint64_t schema_xid);

    /**
     * @brief This returns the GRPC channel.
     */
    std::shared_ptr<grpc::Channel> get_channel() const {
        return _channel;
    }

private:
    XidMgrClient();
    ~XidMgrClient() override = default;

    std::shared_ptr<grpc::Channel> _channel;
    std::unique_ptr<proto::XidManager::Stub> _stub;
};

}  // namespace springtail
