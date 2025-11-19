#include <proto/xid_manager.grpc.pb.h>
#include <condition_variable>
#include <vector>
#include <grpc/grpc_client.hh>
#include <xid_mgr/xid_mgr_client.hh>

namespace springtail {

struct XidMgrSubscriber : public grpc::ClientReadReactor<proto::XidPushResponse>
{
    //void PushCallback(uint64_t db_id, uint64_t xid, bool has_schema_change, bool real_commit, std::vector<uint64_t> table_ids);
    using PushCallback = std::function<void(uint64_t, uint64_t, bool, bool, std::vector<uint64_t>)>;
    using DisconnectCallback = std::function<void()>;

    /**
     * @brief Subscriber callbacks. The callbacks are called in the context of
     *        an internal thread.
     */
    struct Callbacks
    {
        PushCallback push;
        DisconnectCallback disconnect;
    };

    /**
     * @brief Subscriber calls the callback function on XID commits.
     * @param ch The GRPC channel.
     * @param cb Notification callbacks. 
     */
    XidMgrSubscriber(std::shared_ptr<grpc::Channel> ch, Callbacks cb);
    ~XidMgrSubscriber();

    void start();

private:
    // GRPC callbacks
    void OnReadDone(bool ok) override;
    void OnDone(const grpc::Status& s) override;

    std::shared_ptr<grpc::Channel> _channel;
    std::optional<Callbacks> _cb;

    std::unique_ptr<proto::XidManager::Stub> _stub;

    grpc::ClientContext _context;
    proto::XidPushResponse _push_response;

    std::mutex _mutex;
    std::condition_variable _cv;
    bool _finished = false;
};

}
