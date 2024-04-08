#include <iostream>
#include <sstream>
#include <cassert>

#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/client_session.hh>

namespace springtail {
    static int id=0;

    ClientSession::ClientSession(boost::asio::ip::tcp::socket socket,
                                 boost::asio::io_context &context,
                                 ProxyRequestHandler &handler)

        : _socket(std::move(socket)),
          _request_handler(handler),
          _strand(context.get_executor()),
          _id(id++)
    {
        SPDLOG_DEBUG("Client connected: {}", _get_remote_endpoint());

        boost::system::error_code ec;
        _socket.set_option(boost::asio::ip::tcp::no_delay(true), ec);
        if (ec) {
            SPDLOG_WARN("Error setting no delay on socket: {} {}\n",
                        ec.value(), ec.message());
        }
    }

    ClientSession::~ClientSession()
    {
        SPDLOG_WARN("Client session being deallocated");
    }

    void
    ClientSession::start()
    {
        // entry point for connection
        boost::asio::co_spawn(_strand, _startup(shared_from_this()), boost::asio::detached);
        // _do_read();
    }

    boost::asio::awaitable<void>
    ClientSession::_startup(std::shared_ptr<ClientSession> self)
    {
        _read_buffer.reset();
        std::size_t n = co_await boost::asio::async_read(_socket, boost::asio::buffer(_read_buffer.data(), _read_buffer.capacity()),
                                                         boost::asio::transfer_at_least(8),
                                                         boost::asio::use_awaitable);

        int32_t msg_length = _read_buffer.get32();
        int32_t code = _read_buffer.get32();

        assert(n == msg_length);

        // check for SSL negotiation
        if (code == 80877103) {
            _write_buffer.reset();
            _write_buffer.put('N');

            co_await boost::asio::async_write(_socket,
                                              boost::asio::buffer(_write_buffer.data(), 1),
                                              boost::asio::use_awaitable);

            _read_buffer.reset();
            n = co_await boost::asio::async_read(_socket, boost::asio::buffer(_read_buffer.data(), _read_buffer.capacity()),
                                                 boost::asio::transfer_at_least(8),
                                                 boost::asio::use_awaitable);

            msg_length = _read_buffer.get32();
            code = _read_buffer.get32();

            assert(n == msg_length);
        }

        if (code == 196608) {
            // proto version 3.0
            _state = AUTH;
            _write_buffer.reset();

            _write_buffer.put('R');
            _write_buffer.put32(0);

            _write_buffer.put('K');
            _write_buffer.put32(12);
            _write_buffer.put32(_pid);
            _write_buffer.put32(_key);


            co_await boost::asio::async_write(_socket,
                                              boost::asio::buffer(_write_buffer.data(), _write_buffer.size()),
                                              boost::asio::use_awaitable);
        }

        co_await _main_loop(self);
    }


    boost::asio::awaitable<void>
    ClientSession::_main_loop(std::shared_ptr<ClientSession> self)
    {
        while (_socket.is_open()) {
            try {
                SPDLOG_DEBUG("Mainloop doing async read: id={}", _id);

                // read some data and let the _handle_read() handler handle it
                std::size_t n = co_await _socket.async_read_some(boost::asio::buffer(_read_buffer.data(), _read_buffer.capacity()),
                                                                 boost::asio::use_awaitable);

                SPDLOG_DEBUG("Read data to process: id={}, bytes={}", _id, n);

                _request_handler.process(_read_buffer.data(), n);

                SPDLOG_DEBUG("Writing response to client: id={}", _id);

                n = co_await boost::asio::async_write(_socket, boost::asio::buffer(_read_buffer.data(), n),
                                                      boost::asio::use_awaitable);

                SPDLOG_DEBUG("Wrote response to client: id={}, bytes={}", _id, n);

            } catch (boost::system::system_error const &se) {
                if (se.code() == boost::asio::error::operation_aborted ||
                    se.code() == boost::asio::error::eof) {
                    SPDLOG_DEBUG("Client closed connection");
                    break;
                } else {
                    SPDLOG_ERROR("Caught exception in connection main loop: {}", se.what());
                    throw se;
                }
            }
        }
    }

    void
    ClientSession::_do_read()
    {
        // read some data and let the _handle_read() handler handle it
        _socket.async_read_some(boost::asio::buffer(_read_buffer.data(), _read_buffer.capacity()),
            boost::bind(&ClientSession::_handle_read, shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));

        SPDLOG_DEBUG("After do_read cowait");
    }


    void
    ClientSession::_handle_read(const boost::system::error_code &error,
                                std::size_t length)
    {
        if (error) {
            std::string remote_addr = _get_remote_endpoint();

            if (error == boost::asio::error::misc_errors::eof){
                SPDLOG_ERROR("Client disconnected: addr={}\n", remote_addr);
            } else {
                SPDLOG_ERROR("Client error: addr={}, error={}, msg={}\n",
                             remote_addr, error.value(), error.message());
            }
            return;
        }

        // handle the read
        _request_handler.process(_read_buffer.data(), length);

        //_do_read();
    }



    void
    ClientSession::async_write()
    {

    }

    std::string
    ClientSession::_get_remote_endpoint()
    {
        std::stringstream ss;
        ss << _socket.remote_endpoint();
        return ss.str();
    }
}