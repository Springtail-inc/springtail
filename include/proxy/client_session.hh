#pragma once

#include <memory>
#include <vector>

#include <proxy/request_handler.hh>
#include <proxy/buffer.hh>
#include <proxy/connection.hh>

namespace springtail {
    class ClientSession : public std::enable_shared_from_this<ClientSession>
    {
    public:
        constexpr static int32_t SSL_NEG = 80877103;
        constexpr static int32_t PROTO_V3 = 196608;
        constexpr static char SERVER_VERSION[] = "16.0 (Springtail)";

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /// Construct a connection with the given socket.
        explicit ClientSession(ProxyConnectionPtr connection,
                               ProxyRequestHandlerPtr handler);

        ~ClientSession();

        /// Start the first asynchronous operation for the connection.
        void start();

    private:
        /// Object containing socket for the connection.
        ProxyConnectionPtr _connection;

        /// The handler used to process the incoming request.
        ProxyRequestHandlerPtr _request_handler;

        ProxyBuffer _read_buffer{1024};
        ProxyBuffer _write_buffer{1024};

        int _id;

        int32_t _pid = 1123;
        int32_t _key = 793746;

        void _do_startup();

        void _encode_parameter_status(const std::string &key, const std::string &value);

        void _handle_requests();
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
}