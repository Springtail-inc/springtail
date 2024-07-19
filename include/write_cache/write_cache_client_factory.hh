#pragma once

#include <string>
#include <memory>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>

#include <common/object_pool.hh>

#include <thrift/write_cache/ThriftWriteCache.h>

namespace springtail {
    /**
     * @brief Object pool factory for thrift cache client objects
     */
    class WriteCacheThriftObjectFactory : public ObjectPoolFactory<thrift::write_cache::ThriftWriteCacheClient>
    {
    public:
        WriteCacheThriftObjectFactory(const std::string &server, int port)
            : _server(server), _port(port)
        {}

        /**
         * @brief Allocate a new client; transport is not connected
         * @return std::shared_ptr<thrift::ThriftWriteCacheClient>
         */
        std::shared_ptr<thrift::write_cache::ThriftWriteCacheClient> allocate() override
        {
            std::shared_ptr<apache::thrift::transport::TSocket> socket =
                std::make_shared<apache::thrift::transport::TSocket>(_server, _port);

            socket->setKeepAlive(true); // set keepalive
            socket->setNoDelay(true);   // should be on by default
            // can also set connTimeout, sendTimeout, recvTimeout (default is disabled)

            std::shared_ptr<apache::thrift::transport::TTransport> transport =
                std::make_shared<apache::thrift::transport::TFramedTransport>(socket);
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol =
                std::make_shared<apache::thrift::protocol::TCompactProtocol>(transport);
            std::shared_ptr<thrift::write_cache::ThriftWriteCacheClient> client =
                std::make_shared<thrift::write_cache::ThriftWriteCacheClient>(protocol);

            return client;
        }

        /**
         * @brief The get callback from the object pool.  Check that transport is connected
         *        before returning.
         * @param client
         */
        void get_cb(std::shared_ptr<thrift::write_cache::ThriftWriteCacheClient> client) override
        {
            // validate that the transport is connected
            std::shared_ptr<apache::thrift::protocol::TProtocol> proto = client->getOutputProtocol();
            if (proto->getTransport()->isOpen()) {
                return;
            }
            proto->getTransport()->open();
        }

    private:
        std::string _server;
        int _port;
    };
}
