#pragma once

#include <common/logging.hh>

#include <proto/xid_manager.grpc.pb.h>
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <stop_token>
#include <thread>
#include <queue>
#include <unordered_set>

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
        explicit GrpcXidMgrService(xid_mgr::XidMgrServer& s) : _srv{s}
        {}
        ~GrpcXidMgrService() = default;

        void shutdown();

        grpc::ServerUnaryReactor* Ping(grpc::CallbackServerContext* context,
                         const google::protobuf::Empty* request,
                         google::protobuf::Empty* response) override;

        grpc::ServerUnaryReactor* CommitXid(grpc::CallbackServerContext* context,
                              const proto::CommitXidRequest* request,
                              google::protobuf::Empty* response) override;

        grpc::ServerUnaryReactor* RecordDdlChange(grpc::CallbackServerContext* context,
                                    const proto::RecordDdlChangeRequest* request,
                                    google::protobuf::Empty* response) override;

        grpc::ServerUnaryReactor* GetCommittedXid(grpc::CallbackServerContext* context,
                                    const proto::GetCommittedXidRequest* request,
                                    proto::GetCommittedXidResponse* response) override;

        grpc::ServerWriteReactor<proto::XidPushResponse>* Subscribe( 
                grpc::CallbackServerContext* context,
                const proto::SubscribeRequest* request) override;

    private:
        xid_mgr::XidMgrServer& _srv;

        struct Notifier : grpc::ServerWriteReactor<proto::XidPushResponse>
        {
            GrpcXidMgrService& _service;

            explicit Notifier(GrpcXidMgrService& s)
                :_service{s}
            {}
            ~Notifier() = default;

            void notify(const proto::XidPushResponse& msg);
            void finish();

        private:
            // GRPC callbacks
            void OnWriteDone(bool ok) override;
            void OnDone() override;
            void OnCancel() override;

            std::mutex _m;
            bool _writing = false;
            std::optional<proto::XidPushResponse> _last_msg;
            bool _finish = false;
        };

        void _unsubscribe(Notifier*);

        std::mutex _m;
        std::unordered_set<Notifier*> _push_notifiers;

        void _push_notifications(const proto::XidPushResponse& xid);

        struct NotificationThread
        {
            GrpcXidMgrService& _service;

            explicit NotificationThread(GrpcXidMgrService& s) : 
                _service{s},
                _t{[this](std::stop_token st) { task(st); }} 
            {}
            void notify(const proto::XidPushResponse& xid);
            void task(std::stop_token st);

            std::mutex _m;
            std::condition_variable_any _cv;
            std::optional<proto::XidPushResponse> _last_msg;
            std::jthread _t;
        };
        std::unique_ptr<NotificationThread> _notification_thread;
    };
}
