#pragma once

#include <boost/asio.hpp>

#include <proxy/request_handler.hh>

namespace springtail {
    class ProxyServer
    {
    public:
        ProxyServer(const ProxyServer&) = delete;
        ProxyServer& operator=(const ProxyServer&) = delete;

        ProxyServer(const std::string& address,
                    const std::string& port,
                    int thread_pool_size);

        void run();

    private:
        /// Perform an asynchronous accept operation.
        void do_accept();

        /// Wait for a request to stop the server.
        void do_await_stop();

        /// The number of threads that will call io_context::run().
        int _thread_pool_size;

        /// The io_context used to perform asynchronous operations.
        boost::asio::io_context _io_context;

        /// The signal_set is used to register for process termination notifications.
        boost::asio::signal_set _signals;

        /// Acceptor used to listen for incoming connections.
        boost::asio::ip::tcp::acceptor _acceptor;

        /// The handler for all incoming requests.
        ProxyRequestHandler _request_handler;
    };
}