#pragma once

#include <memory>
#include <vector>
#include <boost/asio.hpp>

#include <proxy/request_handler.hh>
#include <proxy/buffer.hh>

namespace springtail {
    class ClientSession : public std::enable_shared_from_this<ClientSession>
    {
    public:

        enum State : uint8_t {
            STARTUP=1,
            AUTH=2,
            READY=3
        };

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /// Construct a connection with the given socket.
        explicit ClientSession(boost::asio::ip::tcp::socket socket,
                               boost::asio::io_context &context,
                               ProxyRequestHandler& handler);

        ~ClientSession();

        /// Start the first asynchronous operation for the connection.
        void start();

        /// Perform an asynchronous write operation.
        void async_write();

    private:
        /// Socket for the connection.
        boost::asio::ip::tcp::socket _socket;

        /// The handler used to process the incoming request.
        ProxyRequestHandler& _request_handler;

        boost::asio::strand<boost::asio::io_context::executor_type> _strand;

        ProxyBuffer _read_buffer{1024};
        ProxyBuffer _write_buffer{1024};

        int _id;

        State _state = STARTUP;

        int32_t _pid = 1123;
        int32_t _key = 793746;

        /// Perform an asynchronous read operation.
        void _do_read();

        boost::asio::awaitable<void> _main_loop(std::shared_ptr<ClientSession> self);
        boost::asio::awaitable<void> _startup(std::shared_ptr<ClientSession> self);
        std::string _get_remote_endpoint();

        void _handle_read(const boost::system::error_code& error,
                          std::size_t bytes_transferred);
    };
}