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

    ClientSession::ClientSession(ProxyConnectionPtr connection,
                                 ProxyRequestHandlerPtr handler)

        : _connection(connection),
          _request_handler(handler),
          _id(id++)
    {
        SPDLOG_DEBUG("Client connected: {}", connection->get_endpoint());
    }

    ClientSession::~ClientSession()
    {
        SPDLOG_WARN("Client session being deallocated");
    }

    void
    ClientSession::start()
    {
        // send startup message, do SSL and authentication
        _do_startup();

        // ready for query -- handle requests
        _handle_requests();
    }

    void
    ClientSession::_do_startup()
    {
        ssize_t n = _connection->read(_read_buffer, 8);
        int32_t msg_length = _read_buffer.get32();
        int32_t code = _read_buffer.get32();

        assert(n == msg_length);

        SPDLOG_DEBUG("Startup message: msg_length={}, code={}", msg_length, code);

        // check for SSL negotiation
        if (code == SSL_NEG) {
            SPDLOG_DEBUG("SSL negotiation requested");
            _write_buffer.reset();
            _write_buffer.put('N');

            _connection->write(_write_buffer.data(), 1);

            n = _connection->read(_read_buffer, 8);
            msg_length = _read_buffer.get32();
            code = _read_buffer.get32();

            assert(n == msg_length);
        }

        SPDLOG_DEBUG("Startup message: msg_length={}, code={}", msg_length, code);

        // proto version 3.0
        if (code == PROTO_V3) {
            SPDLOG_DEBUG("Proto version 3.0 requested");

            // read parameter strings
            std::string key;
            std::string value;
            // seems to be a trailing null byte on the end
            while (_read_buffer.remaining() > 1) {
                key = _read_buffer.getString();
                value = _read_buffer.getString();

                SPDLOG_DEBUG("Parameter: {}={}", key, value);
            }

            _write_buffer.reset();

            // authentication request -- auth ok, no auth needed
            _write_buffer.put('R');
            _write_buffer.put32(8);
            _write_buffer.put32(0);

            // parameter status
            _encode_parameter_status("server_encoding", "UTF8");
            _encode_parameter_status("client_encoding", "UTF8");
            _encode_parameter_status("server_version", SERVER_VERSION);

            // backend key data -- for cancellation
            _write_buffer.put('K');
            _write_buffer.put32(12);
            _write_buffer.put32(_pid);
            _write_buffer.put32(_key);

            // ready for query
            _write_buffer.put('Z');
            _write_buffer.put32(5);
            _write_buffer.put('I');

            n = _connection->write(_write_buffer.data(), _write_buffer.size());
            assert(n == _write_buffer.size());
        } else {
            SPDLOG_ERROR("Unsupported protocol version: {}", code);
            _connection->close();
            throw std::runtime_error("Unsupported protocol version");
        }
    }

    void
    ClientSession::_encode_parameter_status(const std::string &key, const std::string &value)
    {
        _write_buffer.put('S');
        _write_buffer.put32(key.size() + value.size() + 6); // 4B len + 2B nulls
        _write_buffer.putString(key);
        _write_buffer.putString(value);
    }

    void
    ClientSession::_handle_requests()
    {
        while (!_connection->closed()) {
            ssize_t n = _connection->read(_read_buffer, 5);
            if (n <= 0) {
                break;
            }

            // op code
            char code = _read_buffer.get();
            // message length including length itself but not code
            // so really msg_length -= 4
            int32_t msg_length = _read_buffer.get32();

            SPDLOG_DEBUG("Request: msg_length={}/{}, code={}", n, msg_length, code);

            // if we didn't read the whole message, read the rest
            if (msg_length + 1 < n) {
                SPDLOG_DEBUG("Need to read more data for message");
                _connection->read_fully(_read_buffer, msg_length + 1);
            }

            // handle request
            switch (code) {
            case 'Q': {
                // query
                std::string query = _read_buffer.getString();
                SPDLOG_DEBUG("Query: {}", query);

                //_request_handler->handle_query(shared_from_this(), query);
                break;
            }
            case 'X': {
                // terminate
                SPDLOG_DEBUG("Terminate request");
                _connection->close();
                return;
            }
            default:
                SPDLOG_ERROR("Unsupported request code: {}", code);
                _connection->close();
                return;
            }
        }
    }

}