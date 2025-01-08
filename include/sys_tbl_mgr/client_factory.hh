#pragma once

#include <string>
#include <memory>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include <common/object_pool.hh>

#include <thrift/sys_tbl_mgr/Service.h>

constexpr useconds_t RECONNECT_SLEEP_INTERVAL_USEC = 1000000;

namespace springtail::sys_tbl_mgr {
    /**
     * @brief Object pool factory for thrift cache client objects
     */
    class ObjectFactory : public ObjectPoolFactory<ServiceClient>
    {
    public:
        ObjectFactory(const std::string &server, int port)
            : _server(server), _port(port)
        {}

        /**
         * @brief Allocate a new client; transport is not connected
         * @return std::shared_ptr<thrift::ThriftSysTblMgrClient>
         */
        std::shared_ptr<ServiceClient> allocate() override
        {
            auto socket = std::make_shared<apache::thrift::transport::TSocket>(_server, _port);

            socket->setKeepAlive(true); // set keepalive
            socket->setNoDelay(true);   // should be on by default
            // can also set connTimeout, sendTimeout, recvTimeout (default is disabled)

            auto transport = std::make_shared<apache::thrift::transport::TFramedTransport>(socket);
            auto protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
            std::shared_ptr<ServiceClient> client = std::make_shared<ServiceClient>(protocol);

            return client;
        }

        /**
         * @brief The get callback from the object pool.  Check that transport is connected
         *        before returning.
         * @param client
         */
        void get_cb(std::shared_ptr<ServiceClient> client) override
        {
            // validate that the transport is connected
            auto proto = client->getOutputProtocol();
            std::shared_ptr<apache::thrift::transport::TTransport> trans = proto->getTransport();
            apache::thrift::transport::TFramedTransport *framed_transport = (apache::thrift::transport::TFramedTransport *)trans.get();
            std::shared_ptr<apache::thrift::transport::TTransport> another_transport = framed_transport->getUnderlyingTransport();
            apache::thrift::transport::TSocket *socket = (apache::thrift::transport::TSocket *)another_transport.get();
            while (!proto->getTransport()->isOpen()) {
                try {
                    proto->getTransport()->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "ServiceClient: Failed to connect to thrift server: ", e.what());
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                }
            }

            int fd = socket->getSocketFD();
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "ServiceClient: Acquired thrift client: fd = {}, client = {}", fd, (void *)client.get());
            // cpptrace::generate_trace().print();
        }

        void put_cb(std::shared_ptr<ServiceClient> client) override
        {
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "ServiceClient: Releasing thrift client: {}", (void *)client.get());
        }

    private:
        std::string _server;
        int _port;
    };
}
