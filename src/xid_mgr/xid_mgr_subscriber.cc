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

    proto::SubscribeRequest req;
    _stub->async()->Subscribe(&_context, &req, this);
    StartRead(&_push_response);
    StartCall();
}

XidMgrSubscriber::~XidMgrSubscriber()
{
    LOG_DEBUG(LOG_XID_MGR, "XidMgrSubscriber::~XidMgrSubscriber: Deleted");
    _stub.reset();
}

void XidMgrSubscriber::cancel()
{
    _context.TryCancel();
}

void XidMgrSubscriber::OnReadDone(bool ok)
{
    LOG_DEBUG(LOG_XID_MGR, "XidMgrSubscriber::OnReadDone");
    if (ok) {
        _cb->push(_push_response.db_id(), _push_response.xid());
        StartRead(&_push_response);
        return;
    }

    if (_cb.has_value()) {
        _cb->disconnect();
        _cb = {};
    }
}

void XidMgrSubscriber::OnDone(const grpc::Status& s)
{
    LOG_DEBUG(LOG_XID_MGR, "XidMgrSubscriber::OnDone");
    if (_cb.has_value()) {
        _cb->disconnect();
        _cb = {};
    }
    delete this; //NOSONAR reason: The object lifetime is controlled by GRPC
}

}
