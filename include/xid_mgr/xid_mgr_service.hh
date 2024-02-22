#include <iostream>

#include <thrift/transport/TSocket.h>

#include <common/logging.hh>

#include "ThriftXidMgr.h"

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftXidMgrIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftXidMgrService : public thrift::ThriftXidMgrIf
    {
    public:
        ThriftXidMgrService() = default;

        void ping(thrift::Status& _return) override;
        void commit_xid(thrift::Status& _return, const thrift::xid_t request) override;
        thrift::xid_t get_committed_xid() override;
    };


    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftXidMgrCloneFactory : virtual public thrift::ThriftXidMgrIfFactory {
        public:
            ~ThriftXidMgrCloneFactory() override = default;

            /**
             * @brief Override the thrift getHandler call, allows for logging
             * @param connInfo Thrift connection info object
             * @return thrift::ThriftXidMgrIf*
             */
            thrift::ThriftXidMgrIf*
            getHandler(const apache::thrift::TConnectionInfo &connInfo) override
            {
                std::shared_ptr<apache::thrift::transport::TSocket> sock =
                    std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

                SPDLOG_DEBUG("Incoming connection\n");
                SPDLOG_DEBUG("\tSocketInfo: {}\n", sock->getSocketInfo());
                SPDLOG_DEBUG("\tPeerHost: {}\n", sock->getPeerHost());
                SPDLOG_DEBUG("\tPeerAddress: {}\n", sock->getPeerAddress());
                SPDLOG_DEBUG("\tPeerPort: {}\n", sock->getPeerPort());

                return new ThriftXidMgrService();
            }

            void
            releaseHandler(thrift::ThriftXidMgrIf *handler) override {
                delete handler;
            }
    };
}