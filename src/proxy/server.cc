#include <signal.h>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio.hpp>

#include <common/logging.hh>

#include <proxy/server.hh>
#include <proxy/client_session.hh>

/** NOTE: heavily borrowed from: https://www.boost.org/doc/libs/1_84_0/doc/html/boost_asio/example/cpp11/http/server3/server.cpp */

namespace springtail {

    ProxyServer::ProxyServer(const std::string& address,
                             const std::string& port,
                             int thread_pool_size)
      : _thread_pool_size(thread_pool_size),
        _signals(_io_context),
        _acceptor(_io_context),
        _request_handler()
    {
        // Register to handle the signals that indicate when the server should exit.
        // It is safe to register for the same signal multiple times in a program,
        // provided all registration for the specified signal is made through Asio.
        _signals.add(SIGINT);
        _signals.add(SIGTERM);
        #if defined(SIGQUIT)
        _signals.add(SIGQUIT);
        #endif // defined(SIGQUIT)

        do_await_stop();

        SPDLOG_DEBUG("Starting proxy server: {}:{}", address, port);

        // Open the acceptor with the option to reuse the address (i.e. SO_REUSEADDR).
        boost::asio::ip::tcp::resolver resolver(_io_context);
        boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(address, port).begin();

        _acceptor.open(endpoint.protocol());
        _acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
        _acceptor.bind(endpoint);
        _acceptor.listen();

        do_accept();
    }

    void
    ProxyServer::run()
    {
        _thread_pool_size = 1;
        // Create a pool of threads to run the io_context.
        std::vector<std::thread> threads;
        for (int i = 0; i < _thread_pool_size; ++i) {
            threads.emplace_back([this]{ _io_context.run(); });
        }

        // Wait for all threads in the pool to exit.
        for (int i = 0; i < threads.size(); ++i) {
            threads[i].join();
        }
    }

    void
    ProxyServer::do_accept()
    {
        SPDLOG_DEBUG("Do accept");
        // The newly accepted socket is put into its own strand to ensure that all
        // completion handlers associated with the connection do not run concurrently.
        _acceptor.async_accept(boost::asio::make_strand(_io_context),
            [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
            // Check whether the server was stopped by a signal before this
            // completion handler had a chance to run.
            SPDLOG_DEBUG("Got a connection");

            if (!_acceptor.is_open()) {
                return;
            }

            if (!ec) {
                std::make_shared<ClientSession>(std::move(socket), _io_context, _request_handler)->start();
            }

            do_accept();
        });
    }

    void
    ProxyServer::do_await_stop()
    {
        _signals.async_wait(
            [this](boost::system::error_code /*ec*/, int /*signo*/) {
                _io_context.stop();
        });
    }
}