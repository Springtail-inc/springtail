#include <memory>
#include <poll.h>

#include <proxy/session.hh>
#include <proxy/server.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {

    void
    Session::operator()()
    {
        int pkt_count = 0;
        bool has_data = false;

        do {
            // thread entry point
            _process();

            // cleanup connection and remove from server list if closed or error
            if (_state == ERROR || _connection->closed()) {
                SPDLOG_ERROR("Error state, closing connection");
                _connection->close();
                _server->shutdown_session(this);
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
        UserPtr user = _server->get_user_mgr()->get_user(_username, _database);
        if (user == nullptr) {
            return nullptr;
        }
        return user->get_user_login();
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
}