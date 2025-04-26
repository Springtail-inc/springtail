#include <iostream>
#include <mutex>

#include <grpc/grpc_server.hh>
#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail {

GrpcXidMgrService::GrpcXidMgrService(xid_mgr::XidMgrServer& s) : _srv{s}
{
    _notification_thread = std::make_unique<NotificationThread>(*this);
}

void GrpcXidMgrService::shutdown()
{
    _notification_thread.reset();
    std::scoped_lock<std::mutex> l(_m);
    for (auto notifier: _push_notifiers) {
        notifier->finish();
    }
}

grpc::ServerUnaryReactor*
GrpcXidMgrService::Ping(grpc::CallbackServerContext* context,
                        const google::protobuf::Empty* request,
                        google::protobuf::Empty* response)
{
    ServerSpan span(context, "XidMgrService", "Ping");
    auto* reactor = context->DefaultReactor();
    try {
        std::cout << "Got ping\n";
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        reactor->Finish(grpc::Status::OK);
    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
    }
    return reactor;
}

grpc::ServerUnaryReactor*
GrpcXidMgrService::GetCommittedXid(grpc::CallbackServerContext* context,
                                   const proto::GetCommittedXidRequest* request,
                                   proto::GetCommittedXidResponse* response)
{
    ServerSpan span(context, "XidMgrService", "GetCommittedXid");
    auto* reactor = context->DefaultReactor();

    try {
        uint64_t xid = _srv.get_committed_xid(request->db_id(), request->schema_xid());
        response->set_xid(xid);
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        reactor->Finish(grpc::Status::OK);

    } catch (const std::exception& e) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
    }
    return reactor;
}

grpc::ServerWriteReactor<proto::XidPushResponse>*
GrpcXidMgrService::Subscribe(
        grpc::CallbackServerContext* context,
        const proto::SubscribeRequest* request)
{
    auto notifier = new Notifier(*this); //NOSONAR reason: The object lifetime is controlled by GRPC

    // register the notifiers
    std::scoped_lock<std::mutex> l(_m);

    CHECK(_push_notifiers.find(notifier) == _push_notifiers.end());

    _push_notifiers.emplace(notifier);

    if (!_notification_thread) {
        _notification_thread = std::make_unique<NotificationThread>(*this);
    }

    // TODO: check if we can enumerate all current db/xid
    // and push them to the notifier
    //

    return notifier;
}

void
GrpcXidMgrService::_unsubscribe(Notifier* key)
{
    std::scoped_lock<std::mutex> l(_m);
    auto it = _push_notifiers.find(key);
    if (it != _push_notifiers.end()) {
        _push_notifiers.erase(it);
    }
}

void
GrpcXidMgrService::_push_notifications(const proto::XidPushResponse& xid)
{
    std::scoped_lock<std::mutex> l(_m);
    for(auto& notifier: _push_notifiers) {
        notifier->notify(xid);
    }
}

void
GrpcXidMgrService::Notifier::notify(const proto::XidPushResponse& msg)
{
    std::scoped_lock<std::mutex> l(_m);
    if (_writing) {
        _last_msg = msg;
    } else {
        StartWrite(&msg);
        _writing = true;
        _last_msg = {};
    }
}
void GrpcXidMgrService::Notifier::finish()
{
    _finish = true;
    Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Server shutdown"));
}

void
GrpcXidMgrService::Notifier::OnWriteDone(bool ok)
{
    if (!ok) {
        Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "OnWriteDone failed"));
        return;
    }
    std::scoped_lock<std::mutex> l(_m);
    _writing = false;
    // new value
    if (_last_msg.has_value()) {
        StartWrite(&(*_last_msg));
        _writing = true;
        _last_msg = {};
    }
};

void
GrpcXidMgrService::Notifier::OnDone()
{
    // NOTE: this is the  last call to
    // the Notifier instance. GRPC recommends
    // to delete itself here. oh well...
    //
    // if finish is set it means that the service
    // could have been deleted by this point, so
    // we don't call _unsubscribe.
    if (!_finish) {
        _service._unsubscribe(this);
    }
    delete this; //NOSONAR reason: The object lifetime is controlled by GRPC
}

void
GrpcXidMgrService::Notifier::OnCancel()
{
    LOG_DEBUG(LOG_XID_MGR, "OnCancel");
    // Will OnDone still be called if OnCancel is called? Yes.
}

void
GrpcXidMgrService::NotificationThread::notify(const proto::XidPushResponse& msg)
{
    {
        std::scoped_lock g(_m);
        _last_msg = msg;
    }
    _cv.notify_one();
}

void
GrpcXidMgrService::NotificationThread::task(std::stop_token st)
{
    while(!st.stop_requested()) {
        std::optional<proto::XidPushResponse> msg;
        {
            std::unique_lock g(_m);
            if (!_cv.wait(g, st, [&] { return _last_msg.has_value(); })) {
                // stop requested
                break;
            }
            msg.swap(_last_msg);
        }
        _service._push_notifications(*msg);
    }
}

}  // namespace springtail
