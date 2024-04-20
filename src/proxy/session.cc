#include <memory>
#include <cassert>
#include <poll.h>

#include <proxy/session.hh>
#include <proxy/server.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {

    void
    Session::operator()()
    {
        // thread entry point from server
        int pkt_count = 0;
        bool has_data = false;

        do {
            // thread entry point
            _process_connection();

            // cleanup connection and remove from server list if closed or error
            if (_state == ERROR || _connection->closed()) {
                _handle_error();
                return;
            }

            if (_waiting_on_session) {
                SPDLOG_DEBUG("Waiting on external session, socket: {}", _connection->get_socket());
                // note: this will not add the connection back to the server
                // poll list. Once the associated session is done it will
                // call back into this session to continue processing
                // at that time it should be added back to the server poll list
                return;
            }

            // check socket if we want to see if more data is available
            // call poll() which is a function call
            struct pollfd pfd = { _connection->get_socket(), POLLIN, 0 };
            int n = poll(&pfd, 1, 0);
            if (n > 0 && pfd.revents & POLLIN) {
                has_data = true;
            } else {
                has_data = false;
            }

            pkt_count++;
        } while (pkt_count < PKT_ITER_MAX_COUNT && has_data);

        // signal server to wait on this connection
        SPDLOG_DEBUG("Adding connection to server poll list: {}", _connection->get_socket());
        _server->signal(_connection);
    }

    UserLoginPtr
    Session::_get_user_login()
    {
        if (_user == nullptr) {
            return nullptr;
        }
        return _user->get_user_login();
    }

    std::pair<char,int32_t>
    Session::_read_hdr()
    {
        // if there is still data in the buffer use that
        if (_read_buffer.remaining() < 5) {
            _read_buffer.reset();
            ssize_t n = _connection->read(_read_buffer, 5, 5); // read at most 5B
            if (n <= 0) {
                _state = ERROR;
                return {'X', -1};
            }
        }

        // op code
        char code = _read_buffer.get();
        // message length includes length field but not code byte
        // so really msg_length -= 4
        int32_t msg_length = _read_buffer.get32() - 4;

        return {code, msg_length};
    }

    std::pair<char,int32_t>
    Session::_read_msg()
    {
        // if there is still data in the buffer use that
        if (_read_buffer.remaining() < 5) {
            _read_buffer.reset();
            ssize_t n = _connection->read(_read_buffer, 5); // read at least 5B
            if (n <= 0) {
                _state = ERROR;
                return {'X', -1};
            }
        }

        // op code
        char code = _read_buffer.get();
        // message length includes length field but not code byte
        // so really msg_length -= 4
        int32_t msg_length = _read_buffer.get32() - 4;

        // if we didn't read the whole message, read the rest
        if (msg_length > _read_buffer.remaining()) {
            SPDLOG_DEBUG("Need to read more data for message, this may block");
            _connection->read_fully(_read_buffer, msg_length - _read_buffer.remaining());
        }

        return {code, msg_length};
    }

    void
    Session::_read_remaining(int32_t msg_length)
    {
        if (msg_length > _read_buffer.remaining()) {
            _connection->read_fully(_read_buffer, msg_length - _read_buffer.remaining());
        }
    }

    void
    Session::_stream_to_remote_session(char code, int32_t msg_length)
    {
        assert(_associated_session != nullptr);
        char buffer[4096];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);
        int n = _associated_session->get_connection()->write(buffer, 5);
        if (n <= 0) {
            SPDLOG_ERROR("Error writing to remote socket: {}",
                         _associated_session->get_connection()->get_socket());
            _state = ERROR;
            return;
        }
        SPDLOG_DEBUG("Streamed header to remote session: code={}, msg_length={}", code, msg_length);

        // iterate reading buffer from local session and write to remote session
        while (msg_length > 0) {
            SPDLOG_DEBUG("Reading {} bytes from local socket", std::min(msg_length, 4096));
            int n = _connection->read(buffer, std::min(msg_length, 4096));
            if (n <= 0) {
                SPDLOG_ERROR("Error reading from local socket: {}", _connection->get_socket());
                _state = ERROR;
                return;
            }

            int m = _associated_session->get_connection()->write(buffer, n);
            if (m <= 0 || m != n) {
                SPDLOG_ERROR("Error writing to remote socket: {}",
                             _associated_session->get_connection()->get_socket());
                _state = ERROR;
                return;
            }

            SPDLOG_DEBUG("Streamed {} bytes to remote session", m);

            msg_length -= m;
        }
    }

    void
    Session::_send_to_remote_session(char code, int32_t msg_length, const char *data)
    {
        assert(_associated_session != nullptr);
        char buffer[5];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);
        int n = _associated_session->get_connection()->write(buffer, 5);
        if (n <= 0) {
            SPDLOG_ERROR("Error writing to remote socket: {}", _connection->get_socket());
            _state = ERROR;
            return;
        }
        n = _associated_session->get_connection()->write(data, msg_length);
        if (n <= 0 || n != msg_length) {
            SPDLOG_ERROR("Error writing to remote socket: {}", _connection->get_socket());
            _state = ERROR;
            return;
        }
    }

    void
    Session::enable_processing()
    {
        if (_waiting_on_session) {
            // if we are waiting on a session, we should not be processing
            // incoming data from our connection
            assert(_associated_session != nullptr);
            return;
        }

        if (_state == ERROR || _connection->closed()) {
            _handle_error();
            return;
        }
        // add session connection to server poll list
        _server->signal(_connection);
    }

    void
    Session::_handle_error()
    {
        // general error handling
        // cleanup this session and check for associated session
        SPDLOG_ERROR("Error state, closing connection");

        // close connection
        _connection->close();

        // let server handle full cleanup
        _server->shutdown_session(this);

        // check for associated client session, and notify of error
        if ((_type == Type::PRIMARY || _type == Type::REPLICA) &&
            _associated_session != nullptr) {
            // notify client session of error
            notify_client(SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR);
        }
    }
}