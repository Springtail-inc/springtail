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
                SPDLOG_DEBUG("Waiting on external session");
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
    Session::_read_msg()
    {
        _read_buffer.reset();
        ssize_t n = _connection->read(_read_buffer, 5);
        if (n <= 0) {
            _state = ERROR;
            return {'X', -1};
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

        return {code, msg_length-4};
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

        assert(_associated_session == nullptr);

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
            notify_client(SessionMsg::MSG_SERVER_CLIENT_ERROR);
        }
    }
}