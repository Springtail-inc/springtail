#pragma once

#include <common/logging.hh>
#include <proto/xid_manager.grpc.pb.h>

namespace springtail {
namespace xid_mgr {
    class XidMgrServer;
};
    /**
     * @brief This is the implementation of the XidManager gRPC service that is generated
     *        from the .proto file. It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class GrpcXidMgrService final : public proto::XidManager::CallbackService
    {
    public:
        explicit GrpcXidMgrService(xid_mgr::XidMgrServer& s);

        ~GrpcXidMgrService() = default;

        void shutdown();

        grpc::ServerUnaryReactor*
        Ping(grpc::CallbackServerContext* context,
             const google::protobuf::Empty* request,
             google::protobuf::Empty* response) override;

        grpc::ServerUnaryReactor*
        GetCommittedXid(grpc::CallbackServerContext* context,
                        const proto::GetCommittedXidRequest* request,
                        proto::GetCommittedXidResponse* response) override;

        grpc::ServerWriteReactor<proto::XidPushResponse>*
        Subscribe(grpc::CallbackServerContext* context,
                  const proto::SubscribeRequest* request) override;

        /**
         * @brief Notify xid manager subscriber of the change
         *
         * @param db_id - database id
         * @param xid - transaction id
         */
        void
        notify_subscriber(uint64_t db_id, uint64_t xid)
        {
            proto::XidPushResponse msg;
            msg.set_db_id(db_id);
            msg.set_xid(xid);
            _notification_thread->notify(msg);
        }

    private:
        xid_mgr::XidMgrServer& _srv;

        /**
         * Notifier is a subscription notifier. Each Notifier instance
         * represents a single subscription or server-side
         * stream of push notifications to one client.
         * Mainly it impelments async GRPC callbacks
         * to manage the streaming process.
         */
        struct Notifier : grpc::ServerWriteReactor<proto::XidPushResponse>
        {
            GrpcXidMgrService& _service;

            explicit Notifier(GrpcXidMgrService& s)
                :_service{s}
            {}
            ~Notifier() = default;

            /**
             * This will push the message to the client.
             */
            void notify(const proto::XidPushResponse& msg);
            void finish();

        private:
            // GRPC callbacks
            void OnWriteDone(bool ok) override;
            void OnDone() override;
            void OnCancel() override;

            std::mutex _m;
            bool _writing = false;
            // we only notify the last xid
            std::optional<proto::XidPushResponse> _last_msg;
            bool _finish = false;
        };

        void _unsubscribe(Notifier*);

        std::mutex _m;
        std::unordered_set<Notifier*> _push_notifiers;

        void _push_notifications(const proto::XidPushResponse& xid);

        /**
         * When the user calls CommitXid, we pass the xid to
         * NotificationThread and return immediately.
         * The thread will iterate subscritions represented by
         * Notifier objects and notify the subscribers.
         */
        struct NotificationThread
        {
            GrpcXidMgrService& _service;

            explicit NotificationThread(GrpcXidMgrService& s) :
                _service{s},
                _t{[this](std::stop_token st) { task(st); }}
            {
                pthread_setname_np(_t.native_handle(), "XidNotification");
            }

            void notify(const proto::XidPushResponse& xid);

        private:
            void task(std::stop_token st);

            std::mutex _m;
            std::condition_variable_any _cv;
            std::optional<proto::XidPushResponse> _last_msg;
            std::jthread _t;
        };
        std::unique_ptr<NotificationThread> _notification_thread;
    };
}
