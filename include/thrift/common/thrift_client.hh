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
#include <common/tracing.hh>

#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/provider.h>

constexpr useconds_t RECONNECT_SLEEP_INTERVAL_USEC = 1000000;

namespace springtail::thrift {
    /**
     * @brief Object pool factory for thrift cache client objects
     *
     * @tparam P - the class that is going to inherit from Client
     * @tparam T - the class that represents generated thrift client
     */
    template <typename T>
    class ObjectFactory : public ObjectPoolFactory<T>
    {
    private:
        /**
         * @brief Override default access manager in thrift to prevent it from doing some additional
         *      certificate information checks
         *
         */
        class ObjectFactoryAccessManager : public apache::thrift::transport::DefaultClientAccessManager {
        public:
            /**
             * @brief - function for verifying socket address
             *
             * @param sa - socket address
             * @return Decision
             */
            Decision verify(const sockaddr_storage& sa) noexcept override {
                (void)sa;
                return apache::thrift::transport::AccessManager::ALLOW;
            }
        };
    public:
        /**
         * @brief Construct a new Object Factory object
         *
         * @param server - server name
         * @param rpc_json - rpc configuration json
         * @param type_name - name of the client type
         */
        ObjectFactory(const std::string &server, nlohmann::json &rpc_json, const std::string &type_name)
            : _server(server) {
            _type_name = type_name;

            Json::get_to<int>(rpc_json, "server_port", _port);

            _ssl = Json::get_or<bool>(rpc_json, "ssl", false);
            if (_ssl) {
                _ssl_socket_factory = std::make_shared<apache::thrift::transport::TSSLSocketFactory>();
                _ssl_socket_factory->authenticate(true);

                std::string cert_file_path;
                Json::get_to<std::string>(rpc_json, "client_cert", cert_file_path);
                if (cert_file_path.empty() || !std::filesystem::exists(cert_file_path)) {
                    SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Invalid configuration for certificate file {}", _type_name, cert_file_path);
                    throw Error("Certificate file path is misconfigured");
                }

                std::string key_file_path;
                Json::get_to<std::string>(rpc_json, "client_key", key_file_path);
                if (key_file_path.empty() || !std::filesystem::exists(key_file_path)) {
                    SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Invalid configuration for key file {}", _type_name, key_file_path);
                    throw Error("Key file path is misconfigured");
                }

                std::string trusted_file_path;
                Json::get_to<std::string>(rpc_json, "client_trusted", trusted_file_path);
                if (trusted_file_path.empty() || !std::filesystem::exists(trusted_file_path)) {
                    SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Invalid configuration for trusted certificates file {}", _type_name, trusted_file_path);
                    throw Error("Trusted certificates file path is misconfigured");
                }

                _ssl_socket_factory->ciphers("ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
                _ssl_socket_factory->loadTrustedCertificates(trusted_file_path.c_str(), nullptr);
                _ssl_socket_factory->loadCertificate(cert_file_path.c_str(), "PEM");
                _ssl_socket_factory->loadPrivateKey(key_file_path.c_str(), "PEM");

                auto access_manager = std::make_shared<ObjectFactoryAccessManager>();
                _ssl_socket_factory->access(access_manager);
            }
        }

        /**
         * @brief Destroy the Object Factory object
         *
         */
        ~ObjectFactory() override = default;

        /**
         * @brief Allocate a new client; transport is not connected
         * @return std::shared_ptr<T>
         */
        std::shared_ptr<T> allocate() override
        {
            std::shared_ptr<apache::thrift::transport::TSocket> socket;

            if (_ssl) {
                socket = std::dynamic_pointer_cast<apache::thrift::transport::TSocket, apache::thrift::transport::TSSLSocket>(_ssl_socket_factory->createSocket(_server, _port));
            } else {
                socket = std::make_shared<apache::thrift::transport::TSocket>(_server, _port);
            }

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
            // open socket when using it for the first time
            if (!proto->getTransport()->isOpen()) {
                socket->open();
            }

            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Acquired thrift client: fd = {}, client = {}", _type_name,
                    socket->getSocketFD(), (void *)client.get());
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

        /** default SSL socket factory */
        std::shared_ptr<apache::thrift::transport::TSSLSocketFactory> _ssl_socket_factory = {nullptr};
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
         * @param rpc_json - rpc configuration json
         */
        void init(const std::string &server, nlohmann::json &rpc_json) {
            int max_connections = Json::get_or<int>(rpc_json, "client_connections", 8);

            _thrift_client_factory = std::make_shared<ObjectFactory<T>>(server, rpc_json, _type_name);
            _thrift_client_pool = std::make_shared<ObjectPool<T>>(
                _thrift_client_factory,
                max_connections/2,
                max_connections,
                ObjectPool<T>::LIFO
            );
        }

        /** OpenTelemetry span */
        tracing::SpanPtr _span;

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<T>> _thrift_client_pool;
        std::shared_ptr<ObjectFactory<T>> _thrift_client_factory;

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
            while (!proto->getTransport()->isOpen()) {
                socket->close();
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

            auto provider = opentelemetry::trace::Provider::GetTracerProvider();
            auto tracer = provider->GetTracer("Thrift");
            _span = tracer->StartSpan("Thrift_Rpc");
            _span->SetAttribute("type_name", _type_name);
            tracing::increment_counter("thrift", "rpc_calls", "calls", 1);

            bool call_successful = false;
            while (!call_successful) {
                try {
                    api_call(c);
                    call_successful = true;
                     _span->End();
                } catch (const apache::thrift::transport::TTransportException &e) {
                    SPDLOG_LOGGER_ERROR(spdlog::default_logger_raw(), "{}: Failed API call : {}", _type_name, e.what());
                    if (_shutting_down) {
                        throw e;
                    }
                    _reconnect_client(c);
                }
            }
        }
    };
};
