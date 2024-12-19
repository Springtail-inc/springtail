#pragma once

#include <string>
#include <memory>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>

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
            auto protocol = std::make_shared<apache::thrift::protocol::TCompactProtocol>(transport);
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
            while (!proto->getTransport()->isOpen()) {
                try {
                    proto->getTransport()->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "Failed to connect to thrift server: ", e.what());
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                }
            }
        }

    private:
        std::string _server;
        int _port;
    };
}
