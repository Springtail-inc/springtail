#pragma once

#include <boost/core/demangle.hpp>

#include <string>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include <common/object_pool.hh>
#include <common/logging.hh>

constexpr useconds_t RECONNECT_SLEEP_INTERVAL_USEC = 1000000;

namespace springtail {
namespace thrift {
    /**
     * @brief Object pool factory for thrift cache client objects
     */
    // P - the class that is going to inherit from Client
    // T - the class that represents generated thrift client, for example thrift::xid_mgr::ThriftXidMgrClient
    template <typename P, typename T>
    class ObjectFactory : public ObjectPoolFactory<T>
    {
    public:
        ObjectFactory(const std::string &server, int port)
            : _server(server), _port(port) {
                _type_name = boost::core::demangle(typeid(P).name());
            }

        ~ObjectFactory() override = default;

        /**
         * @brief Allocate a new client; transport is not connected
         * @return std::shared_ptr<T>
         */
        std::shared_ptr<T> allocate() override
        {
            std::shared_ptr<apache::thrift::transport::TSocket> socket =
                std::make_shared<apache::thrift::transport::TSocket>(_server, _port);

            socket->setKeepAlive(true); // set keepalive
            socket->setNoDelay(true);   // should be on by default
            // can also set connTimeout, sendTimeout, recvTimeout (default is disabled)

            std::shared_ptr<apache::thrift::transport::TTransport> transport =
                std::make_shared<apache::thrift::transport::TFramedTransport>(socket);
            std::shared_ptr<apache::thrift::protocol::TProtocol> protocol =
                std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
            std::shared_ptr<T> client = std::make_shared<T>(protocol);

            return client;
        }

        /**
         * @brief The get callback from the object pool.  Check that transport is connected
         *        before returning.
         * @param client
         */
        void get_cb(std::shared_ptr<T> client) override
        {
            // validate that the transport is connected
            std::shared_ptr<apache::thrift::protocol::TProtocol> proto = client->getOutputProtocol();
            std::shared_ptr<apache::thrift::transport::TTransport> trans = proto->getTransport();
            apache::thrift::transport::TFramedTransport *framed_transport = (apache::thrift::transport::TFramedTransport *)trans.get();
            std::shared_ptr<apache::thrift::transport::TTransport> another_transport = framed_transport->getUnderlyingTransport();
            apache::thrift::transport::TSocket *socket = (apache::thrift::transport::TSocket *)another_transport.get();
            while (!proto->getTransport()->isOpen()) {
                try {
                    socket->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed to connect to thrift server: {}", _type_name, e.what());
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                }
            }

            int fd = socket->getSocketFD();
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Acquired thrift client: fd = {}, client = {}", _type_name, fd, (void *)client.get());
        }

        /**
         * @brief Releasing callback from the object pool.
         *
         * @param client
         */
        void put_cb(std::shared_ptr<T> client) override
        {
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Releasing thrift client: {}", _type_name, (void *)client.get());
        }

    protected:
        std::string _type_name;
        std::string _server;
        int _port;

    };

    /**
     * @brief Thrift client class
     *
     * @tparam P - the class that is going to inherit from Client
     * @tparam T - the class that represents generated thrift client
     */
    template <typename P, typename T>
    class Client {
    protected:
        /**
         * @brief Construct a new Client object
         *
         */
        Client() {
            static_assert(std::is_base_of<Client, P>::value, "type parameter of P must derive from Client");

            _type_name = boost::core::demangle(typeid(P).name());
        }
        /**
         * @brief Destroy the Client object
         *
         */
        virtual ~Client() = default;

        /**
         * @brief Initialize client parameters
         *
         * @param server - host name or ip
         * @param port - port number to connect to
         * @param max_connections - maximum connection number
         */
        void init(const std::string &server, int port, int max_connections) {
            // construct the thrift client pool.
            // First argument is a factory object that constructs a thrift clients
            // using the host and port from above
            _thrift_client_pool = std::make_shared<ObjectPool<T>>(
                std::make_shared<ObjectFactory<P, T>>(server, port),
                max_connections/2,
                max_connections,
                ObjectPool<T>::LIFO
            );
        }

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<T>> _thrift_client_pool;

        /** Derived class name for logging */
        std::string _type_name;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<T>> pool;
            std::shared_ptr<T> client;
            ~ThriftClient() {
                pool->put(client);
            }
        };

        /**
         * @brief Helper function to fetch a thrift client from the object pool wrapped in
         *        a struct to ensure its proper release to the pool
         */
        inline ThriftClient _get_client()
        {
            std::shared_ptr<T> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

        /**
         * @brief Helper function to reconnect thrift client to the server
         *
         * @param c - reference to thrift client object
         */
        void _reconnect_client(ThriftClient &c) {
            std::shared_ptr<apache::thrift::protocol::TProtocol> proto = c.client->getOutputProtocol();
            std::shared_ptr<apache::thrift::transport::TTransport> trans = proto->getTransport();
            apache::thrift::transport::TFramedTransport *framed_transport = (apache::thrift::transport::TFramedTransport *)trans.get();
            std::shared_ptr<apache::thrift::transport::TTransport> another_transport = framed_transport->getUnderlyingTransport();
            apache::thrift::transport::TSocket *socket = (apache::thrift::transport::TSocket *)another_transport.get();
            socket->close();
            while (!proto->getTransport()->isOpen()) {
                try {
                    socket->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed to connect to thrift server: {}", _type_name, e.what());
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                }
            }
        }
    };

};
};