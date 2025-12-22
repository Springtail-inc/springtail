#include <absl/log/log.h>
#include <google/protobuf/empty.pb.h>

#include <common/common.hh>
#include <common/logging.hh>
#include <xid_mgr/xid_mgr_subscriber.hh>

namespace springtail {

XidMgrSubscriber::XidMgrSubscriber(std::shared_ptr<grpc::Channel> ch, Callbacks cb)
    :_channel{std::move(ch)},
    _cb{std::move(cb)}
{
    _stub = proto::XidManager::NewStub(_channel);
    CHECK(_stub);
}

void 
XidMgrSubscriber::start()
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Starting subscriber");
    proto::SubscribeRequest req;
    // From this point the lifetime of this object is managed by gRPC
    // According to gRPC docs, we must not delete this object until OnDone callback
    // is called. The callback call time is not guaranteed, so we should make 
    // the best effort to make sure that the object is valid until then.
    _stub->async()->Subscribe(&_context, &req, this);
    StartCall();
    StartRead(&_push_response);
}


XidMgrSubscriber::~XidMgrSubscriber()
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Deleting");

    // note: the comment above about lifetime.
    // We are trying to delete the object, so we should make the best effort waiting
    // for OnDone to be called.

    _context.TryCancel(); // TryCancel is thread-safe.

    {
        std::unique_lock<std::mutex> lock(_mutex);
        // After an attempt to cancel, wait for 1sec to finish,
        // after that all bets are off and we'll blame gRPC...
        _cv.wait_until(lock,
                std::chrono::steady_clock::now() + std::chrono::seconds(4), 
                [this] { return _finished; });
        CHECK(_finished);
    }

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Discconnected");
    _stub.reset();
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Deleted");
}

void XidMgrSubscriber::OnReadDone(bool ok)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "OnReadDone");
    if (ok) {
        _cb->push(std::move(_push_response));
        StartRead(&_push_response);
        return;
    }

    // something went wrong, disconnect
    if (_cb.has_value()) {
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "OnReadDone: ok=false");
        _cb->disconnect();
        _cb = {};
    }
}

void XidMgrSubscriber::OnDone(const grpc::Status& s)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "OnDone");
    if (_cb.has_value()) {
        _cb->disconnect();
        _cb = {};
    }

    std::unique_lock<std::mutex> lock(_mutex);
    _finished = true;
    _cv.notify_one();
}

}
