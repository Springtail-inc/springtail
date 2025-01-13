#pragma once

#include <boost/core/demangle.hpp>

#include <string>
#include <functional>
#include <atomic>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include <common/json.hh>
#include <common/logging.hh>
#include <common/object_pool.hh>

constexpr useconds_t RECONNECT_SLEEP_INTERVAL_USEC = 1000000;

namespace springtail::thrift {
    /**
     * @brief Object pool factory for thrift cache client objects
     *
     * @tparam P - the class that is going to inherit from Client
     * @tparam T - the class that represents generated thrift client
     */
    template <typename P, typename T>
    class ObjectFactory : public ObjectPoolFactory<T>
    {
    public:
        /**
         * @brief Construct a new Object Factory object
         *
         * @param server - server name
         * @param port - server port
         * @param ssl - should use ssl
         */
        ObjectFactory(const std::string &server, int port, bool ssl)
            : _server(server), _port(port), _ssl(ssl) {
                _type_name = boost::core::demangle(typeid(P).name());
            }

        /**
         * @brief Destroy the Object Factory object
         *
         */
        ~ObjectFactory() override = default;

        /**
         * @brief Method for turning on shutdown flag for breaking out of retry loop
         *
         */
        void notify_shutdown() {
            _shutting_down = true;
        }

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
            auto proto = client->getOutputProtocol();
            auto trans = proto->getTransport();
            auto framed_transport = std::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport, apache::thrift::transport::TTransport>(trans);
            auto another_transport = framed_transport->getUnderlyingTransport();
            auto socket = std::dynamic_pointer_cast<apache::thrift::transport::TSocket, apache::thrift::transport::TTransport>(another_transport);
            while (!proto->getTransport()->isOpen()) {
                try {
                    socket->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed to connect to thrift server: {}", _type_name, e.what());
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                    if (_shutting_down) {
                        throw e;
                    }
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
        /** type name of the client class */
        std::string _type_name;

        /** server host */
        std::string _server;

        /** server port */
        int _port;

        /** Require SSL */
        bool _ssl = false;

        /** shutdown flag that allows to interrupt retry loop */
        std::atomic<bool> _shutting_down = false;
    };

    /**
     * @brief Thrift client class
     *
     * @tparam P - the class that is going to inherit from Client
     * @tparam T - the class that represents generated thrift client
     */
    template <typename P, typename T>
    class Client {
    public:
        /**
         * @brief Method for turning on shutdown flag for breaking out of retry loop
         *
         */
        void notify_shutdown() {
            _shutting_down = true;
            if (_thrift_client_factory.get() != nullptr) {
                _thrift_client_factory->notify_shutdown();
            }
        }
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
        void init(const std::string &server, int port, int max_connections, bool ssl) {
            // construct the thrift client pool.
            // First argument is a factory object that constructs a thrift clients
            // using the host and port from above
            _thrift_client_factory = std::make_shared<ObjectFactory<P, T>>(server, port, ssl);
            _thrift_client_pool = std::make_shared<ObjectPool<T>>(
                _thrift_client_factory,
                max_connections/2,
                max_connections,
                ObjectPool<T>::LIFO
            );
        }

        void init(const std::string &server, nlohmann::json &rpc_json) {
            int max_connections = Json::get_or<int>(rpc_json, "client_connections", 8);
            int port = 0;
            Json::get_to<int>(rpc_json, "server_port", port);
            int ssl = Json::get_or<bool>(rpc_json, "ssl", false);

            _thrift_client_factory = std::make_shared<ObjectFactory<P, T>>(server, port, ssl);
            _thrift_client_pool = std::make_shared<ObjectPool<T>>(
                _thrift_client_factory,
                max_connections/2,
                max_connections,
                ObjectPool<T>::LIFO
            );
        }

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<T>> _thrift_client_pool;
        std::shared_ptr<ObjectFactory<P, T>> _thrift_client_factory;

        /** Derived class name for logging */
        std::string _type_name;

        /** Shutting down flag for breaking out of the retry loop */
        std::atomic<bool> _shutting_down = false;

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
            auto proto = c.client->getOutputProtocol();
            auto trans = proto->getTransport();
            auto framed_transport = std::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport, apache::thrift::transport::TTransport>(trans);
            auto another_transport = framed_transport->getUnderlyingTransport();
            auto socket = std::dynamic_pointer_cast<apache::thrift::transport::TSocket, apache::thrift::transport::TTransport>(another_transport);
            socket->close();
            while (!proto->getTransport()->isOpen()) {
                try {
                    socket->open();
                } catch (const apache::thrift::transport::TTransportException& e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed to connect to thrift server: {}", _type_name, e.what());
                    if (_shutting_down) {
                        throw e;
                    }
                    ::usleep(RECONNECT_SLEEP_INTERVAL_USEC);
                }
            }
        }

        /**
         * @brief Invokes a function that actually performs an API call. If thrift throws an exception,
         *          it will be caught, and the client will attempt to reconnect and retry an API call.
         *
         * @param api_call - function that will perform an API call
         */
        void _invoke_with_retries(std::function<void (ThriftClient &)> api_call) {
            ThriftClient c = _get_client();

            bool call_successful = false;
            while (!call_successful) {
                try {
                    api_call(c);
                    call_successful = true;
                } catch (const apache::thrift::transport::TTransportException &e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed API call : ", _type_name, e.what());
                    if (_shutting_down) {
                        throw e;
                    }
                    _reconnect_client(c);
                }
            }
        }
    };
};
