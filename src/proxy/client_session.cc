#include <iostream>
#include <sstream>
#include <cassert>

#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/client_session.hh>
#include <proxy/errors.hh>
#include <proxy/server.hh>

#include <proxy/auth/md5.h>

namespace springtail {
    static int id=0;

    ClientSession::ClientSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server)

        : _connection(connection),
          _server(server),
          _id(id++)
    {
        SPDLOG_DEBUG("Client connected: {}", connection->get_endpoint());
    }

    ClientSession::~ClientSession()
    {
        SPDLOG_WARN("Client session being deallocated");
    }

    void
    ClientSession::_process()
    {
        SPDLOG_DEBUG("Processing client session: state={}", (int8_t)_state);

        switch(_state) {
            case STARTUP:
                _handle_startup();
                break;
            case AUTH:
                _handle_auth();
                break;
            case READY:
                _handle_request();
                break;
            default:
                SPDLOG_ERROR("Invalid state: {}", (int8_t)_state);
                _connection->close();
                break;
        }

        if (_state == ERROR) {
            SPDLOG_ERROR("Error state, closing connection");
            _connection->close();
            _server->shutdown(this);
        } else {
            _server->signal(_connection);
        }
    }

    void
    ClientSession::_handle_startup()
    {
        ssize_t n = _connection->read(_read_buffer, 8);
        if (n <= 0) {
            _state = ERROR;
            return;
        }

        int32_t msg_length = _read_buffer.get32();
        int32_t code = _read_buffer.get32();

        SPDLOG_DEBUG("Startup message: msg_length={}, code={}", msg_length, code);

        switch (code) {
            case MSG_SSLREQ:
                SPDLOG_DEBUG("SSL negotiation requested");
                _write_buffer.reset();
                _write_buffer.put('N');
                _connection->write(_write_buffer.data(), 1);
                break;

            case MSG_STARTUP_V2:
                SPDLOG_WARN("Startup message version 2.0, not supported");
                // not supported
                _state = ERROR;
                break;

            case MSG_STARTUP_V3:
                _process_startup_msg(code, msg_length);
                break;

            default:
                SPDLOG_ERROR("Invalid startup message code: {}", code);
                _state = ERROR;
                break;
        }
    }

    void
    ClientSession::_process_startup_msg(int32_t code, int32_t msg_length)
    {
        SPDLOG_DEBUG("Proto version 3.0 requested");

        // read parameter strings
        std::string key;
        std::string value;

        // seems to be a trailing null byte on the end
        while (_read_buffer.remaining() > 1) {
            key = _read_buffer.getString();
            value = _read_buffer.getString();

            SPDLOG_DEBUG("Parameter: {}={}", key, value);

            if (key == "user") {
                _username = value;
            } else if (key == "database") {
                _database = value;
            }
        }

        // get user login info and store it
        _login = _get_user_login();

        // handle authentication -- send auth request
        _send_auth_req();
    }

    ClientSession::UserLoginPtr
    ClientSession::_get_user_login()
    {
        if (_username == "test") {
            // TRUST no password
            return std::make_shared<UserLogin>(UserLogin::TRUST);
        } else {
            // MD5 password
            std::string passwd = "test";
            std::string name = _username;

            // md5sum('pwd'+'user') = md5+digest
            char md5[36];
            pg_md5_encrypt(passwd.c_str(), name.c_str(), strlen(name.c_str()), md5);

            uint32_t salt;
            get_random_bytes((uint8_t*)&salt, 4);

            return std::make_shared<UserLogin>(UserLogin::MD5, md5+3, salt);
        }
    }

    void
    ClientSession::_send_auth_req()
    {
        _state = AUTH;

        _write_buffer.reset();

        switch(_login->_type) {
            case UserLogin::TRUST:
                SPDLOG_DEBUG("User {} authenticated with trust", _username);
                _send_auth_done();
                return; // did send above so we return here

            case UserLogin::MD5:
                SPDLOG_DEBUG("User {} authenticated with md5", _username);
                _encode_auth_md5();
                break;

            case UserLogin::SCRAM:
                SPDLOG_DEBUG("User {} authenticated with scram", _username);
                _encode_auth_scram();
                break;

            default:
                SPDLOG_ERROR("User {} not found", _username);
                _state = ERROR;
                ProxyError::encode_error(_write_buffer, ProxyError::INVALID_PASSWORD, "password authentication failed");
                break;
        }

        // we've encoded the auth message above, now we send it, for AUTH_OK it is already sent
        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());
    }

    void
    ClientSession::_handle_auth()
    {
        auto [code, msg_length] = _read_msg();
        if (_state == ERROR) {
            return;
        }

        assert(code == 'p');

        switch(_login->_type) {
            case UserLogin::MD5: {
                char md5[MD5_PASSWD_LEN + 1];

                std::string client_passwd = _read_buffer.getString();
                if (client_passwd.empty() || client_passwd.size() != MD5_PASSWD_LEN) {
                    SPDLOG_ERROR("Empty password received; or password length mismatch");
                    _state = ERROR;
                    return;
                }

                // calculate md5 hash
                if (!pg_md5_encrypt(_login->_password.c_str(), reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
                    SPDLOG_ERROR("Failed to calculate MD5 hash");
                    _state = ERROR;
                    return;
                }

                if (strcmp(md5, client_passwd.c_str()) == 0) {
                    SPDLOG_DEBUG("MD5 password match");
                    _send_auth_done();
                    return;
                }

                SPDLOG_ERROR("MD5 password mismatch: : {} <> {}", md5, client_passwd);
                ProxyError::encode_error(_write_buffer, ProxyError::INVALID_PASSWORD, "password authentication failed");
                _connection->write(_write_buffer.data(), _write_buffer.size());
                _state = ERROR;

                break;
            }

            case UserLogin::SCRAM:
                // not implemented
                SPDLOG_ERROR("SCRAM authentication not implemented");
                break;

            default:
                SPDLOG_ERROR("Invalid auth continue state");
                _state = ERROR;
                break;
        }
    }

    void
    ClientSession::_send_auth_done()
    {
        _write_buffer.reset();

        // encode auth ok
        _encode_auth_ok();

        // send final set of params followed by ready for query
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

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());

        _state = READY;
    }

    void
    ClientSession::_encode_auth_ok()
    {
        _write_buffer.put('R');
        _write_buffer.put32(8);
        _write_buffer.put32(0);
    }

    void
    ClientSession::_encode_auth_md5()
    {
        _write_buffer.put('R');
        _write_buffer.put32(12);
        _write_buffer.put32(5); // 5 == md5
        _write_buffer.putBytes(reinterpret_cast<char*>(&_login->_salt), 4);
    }

    void
    ClientSession::_encode_auth_scram()
    {
        _write_buffer.put('R');
        _write_buffer.put32(12);
        _write_buffer.put32(10); // 10 == scram
        _write_buffer.putString("SCRAM-SHA-256");
    }

    void
    ClientSession::_encode_parameter_status(const std::string &key, const std::string &value)
    {
        _write_buffer.put('S');
        _write_buffer.put32(key.size() + value.size() + 6); // 4B len + 2B nulls
        _write_buffer.putString(key);
        _write_buffer.putString(value);
    }

    std::pair<char,int32_t>
    ClientSession::_read_msg()
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
    ClientSession::_handle_request()
    {
        auto [code, msg_length] = _read_msg();

        if (_state == ERROR) {
            return;
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