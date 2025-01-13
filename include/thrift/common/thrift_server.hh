#pragma once

#include <boost/core/demangle.hpp>

#include <thrift/server/TServer.h>
#include <thrift/concurrency/ThreadManager.h>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TNonblockingSSLServerSocket.h>

#include <common/json.hh>
#include <common/logging.hh>

namespace springtail::thrift {

    /**
     * @brief Private helper class to override handler creation;
     *        can be used to store per connection state or log incoming connections
     *
     * @tparam T - derived Server class
     * @tparam S - service class that implements I
     * @tparam F - service interface factory class
     * @tparam I - service interface class
     */
   template <typename T, typename S, typename F, typename I >
    class CloneFactory : virtual public F {
    public:
        /**
         * @brief Construct a new Clone Factory object
         *
         */
        CloneFactory() {
            _type_name = boost::core::demangle(typeid(T).name());
        }

        /**
         * @brief Destroy the Clone Factory object
         *
         */
        ~CloneFactory() override = default;

        /**
         * @brief Override the thrift getHandler call, allows for logging
         * @param connInfo Thrift connection info object
         * @return I* - service implementation handler
         */
        I*
        getHandler(const apache::thrift::TConnectionInfo &connInfo) override
        {
            auto sock = std::dynamic_pointer_cast<apache::thrift::transport::TSocket>(connInfo.transport);

            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: Incoming connection", _type_name);
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}:\tSocketInfo: {}", _type_name, sock->getSocketInfo());
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}:\tPeerHost: {}", _type_name, sock->getPeerHost());
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}:\tPeerAddress: {}", _type_name, sock->getPeerAddress());
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}:\tPeerPort: {}", _type_name, sock->getPeerPort());

            return S::get_instance();
        }

        /**
         * @brief
         *
         * @param handler - service implementation handler
         */
        void
        releaseHandler(I *handler) override {
        }
    private:
        /** Name of the class that inherits from server */
        std::string _type_name;
    };

    /**
     * @brief Template Server class for implementation of a thrift service
     *
     * @tparam T - derived Server class
     * @tparam P - processor factory class
     * @tparam S - service class that implements I
     * @tparam F - service interface factory class
     * @tparam I - service interface class
     */
    template <typename T, typename P, typename S, typename F, typename I>
    class Server {
    protected:
        /**
         * @brief Construct a new Server object
         *
         */
        Server() {
            static_assert(std::is_base_of<I, S>::value, "type parameter of S must derive from I");
            static_assert(std::is_base_of<::apache::thrift::TProcessorFactory, P>::value, "type parameter of P must derive from ::apache::thrift::TProcessorFactory");
            static_assert(std::is_base_of<Server, T>::value, "type parameter of T must derive from Server");

            _type_name = boost::core::demangle(typeid(T).name());
        }
        /**
         * @brief Destroy the Server object
         *
         */
        virtual ~Server() = default;

        /**
         * @brief Initialize server object
         *
         * @param worker_thread_count - number of threads to use for server thread pool
         * @param port - port number for the server to listen on
         */
        void init(int worker_thread_count, int port, bool ssl) {
            _worker_thread_count = worker_thread_count;
            _port = port;
            _ssl = ssl;
        }

        void init(nlohmann::json &rpc_json) {
            Json::get_to<int>(rpc_json, "server_port", _port);
            Json::get_to<int>(rpc_json, "server_worker_threads", _worker_thread_count);
            _ssl = Json::get_or<bool>(rpc_json, "ssl", false);
        }

    public:
        /**
         * Startup thrift threaded server
         */
        void startup()
        {
            SPDLOG_LOGGER_DEBUG(spdlog::default_logger_raw(), "{}: creating thread manager with {} threads", _type_name, _worker_thread_count);

            // create a thread manager with right number of worker threads
            _thread_manager = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(_worker_thread_count);

            // use thread factory with attached threads
            _thread_manager->threadFactory(std::make_shared<apache::thrift::concurrency::ThreadFactory>());
            _thread_manager->start();

            auto server_socket = std::make_shared<apache::thrift::transport::TNonblockingServerSocket>(_port);

            _server = std::make_shared<apache::thrift::server::TNonblockingServer>(
                std::make_shared<P>(std::make_shared<CloneFactory<T, S, F, I>>()),
                std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>(),
                server_socket,
                _thread_manager
            );

           _server->serve();
        }

        /**
         * @brief Stop the server
         *
         */
        void stop() {
            _server->stop();
            _thread_manager->stop();
        }

    protected:
         /** number of worker threads */
        int _worker_thread_count = 0;

        /** server port */
        int _port = 0;

        /** Require SSL */
        bool _ssl = false;

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** thread manager that is used by the server */
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> _thread_manager = {nullptr};

        /** default SSL socket factory */
        std::shared_ptr<apache::thrift::transport::TSSLSocketFactory> _ssl_socket_factory = {nullptr};

        /** Demangled name of the class derived from Server */
        std::string _type_name;

    };

};
