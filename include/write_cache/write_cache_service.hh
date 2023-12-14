#include <iostream>

#include <thrift/transport/TSocket.h>

#include <common/logging.hh>

#include "ThriftWriteCache.h"

namespace springtail {
    /**
     * @brief This is the implementation of the ThriftWriteCacheIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftWriteCacheService : public thrift::ThriftWriteCacheIf
    {
    public:
        ThriftWriteCacheService() = default;

        void addRows(thrift::Status& _return, const thrift::AddRowRequest& request) override;
        void ping(thrift::Status& _return) override;
    };

    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     */
    class ThriftWriteCacheCloneFactory : virtual public thrift::ThriftWriteCacheIfFactory {
        public:
            ~ThriftWriteCacheCloneFactory() override = default;
            
            thrift::ThriftWriteCacheIf* 
            getHandler(const apache::thrift::TConnectionInfo &connInfo) override
            {
                std::shared_ptr<apache::thrift::transport::TSocket> sock = 
                    std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);
                
                std::cout << "Incoming connection\n";
                std::cout << "\tSocketInfo: "  << sock->getSocketInfo() << "\n";
                std::cout << "\tPeerHost: "    << sock->getPeerHost() << "\n";
                std::cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
                std::cout << "\tPeerPort: "    << sock->getPeerPort() << "\n";

                return new ThriftWriteCacheService();
            }

            void
            releaseHandler(thrift::ThriftWriteCacheIf *handler) override {
                delete handler;
            }
    };
}