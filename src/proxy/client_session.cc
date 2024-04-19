#include <iostream>
#include <sstream>
#include <cassert>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/server_session.hh>
#include <proxy/client_session.hh>
#include <proxy/user_mgr.hh>
#include <proxy/errors.hh>
#include <proxy/server.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {
    static int id=0;

    ClientSession::ClientSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server)

        : Session(connection, server, CLIENT),
          _id(id++)
    {
        SPDLOG_DEBUG("Client connected: {}, id={}", connection->endpoint(), _id);

        // initialize pid and key for cancellation
        get_random_bytes(reinterpret_cast<uint8_t*>(&_pid), 4);
        // clear top bit to make pid not signed, some historic issue
        _pid &= 0x7FFFFFFF;
        get_random_bytes(reinterpret_cast<uint8_t*>(&_cancel_key), 4);
    }

    ClientSession::~ClientSession()
    {
        SPDLOG_WARN("Client session being deallocated");
    }

    void
    ClientSession::notify_server_available(SessionPtr server)
    {
        // called from pool indicating there is a server session available
    }

    void
    ClientSession::_process_msg(SessionMsg msg)
    {
        // entry point for messages from server session
        switch(msg) {
        case MSG_SERVER_CLIENT_AUTH_DONE:
            SPDLOG_DEBUG("Client session got auth done from server session");
            clear_associated_session();
            _send_auth_done();
            break;

        case MSG_SERVER_CLIENT_ERROR:
            _handle_server_error(get_associated_session()->get_err_msg());
            break;

        default:
            SPDLOG_WARN("Invalid message recevied by client session: {}", (int8_t)msg);
            break;
        }

        enable_processing();
    }

    void
    ClientSession::_process_connection()
    {
        // entry point for network connection message
        SPDLOG_DEBUG("Processing client session: state={}", (int8_t)_state);

        // main entry point for thread processing
        // resume from where we left off
        switch(_state) {
            case STARTUP:
                // startup messages, no auth done yet
                _handle_startup();
                break;
            case AUTH:
                // completed startup, now handling auth requests
                _handle_auth();
                break;
            case READY:
                // completed auth ready for queries
                _handle_request();
                break;
            default:
                SPDLOG_ERROR("Invalid state: {}", (int8_t)_state);
                _state = ERROR;
                break;
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
        std::string username;
        std::string database;

        // seems to be a trailing null byte on the end
        while (_read_buffer.remaining() > 1) {
            key = _read_buffer.getString();
            value = _read_buffer.getString();

            SPDLOG_DEBUG("Parameter: {}={}", key, value);

            if (key == "user") {
                username = value;
            } else if (key == "database") {
                database = value;
            }
        }
        _read_buffer.reset(); // skip null byte, reset buffer

        // get user info and store it
        _user = _server->get_user_mgr()->get_user(username, database);
        if (_user == nullptr) {
            SPDLOG_ERROR("User {} not found", username);
            _state = ERROR;
            return;
        }

        // get login info for the user
        _login = _user->get_user_login();

        // handle authentication -- send auth request
        _send_auth_req();
    }

    void
    ClientSession::_send_auth_req()
    {
        _state = AUTH;

        _write_buffer.reset();

        switch(_login->_type) {
            case TRUST:
                SPDLOG_DEBUG("User {} authenticated with trust", _user->username());
                _do_server_auth();
                return; // did send above so we return here

            case MD5:
                SPDLOG_DEBUG("User {} authenticating with md5", _user->username());
                _encode_auth_md5();
                break;

            case SCRAM:
                SPDLOG_DEBUG("User {} authenticating with scram", _user->username());
                _encode_auth_scram();
                break;

            default:
                SPDLOG_ERROR("User {} not found", _user->username());
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

        SPDLOG_DEBUG("Auth continue: msg_length={}", msg_length);

        switch(_login->_type) {
            case MD5: {
                char md5[MD5_PASSWD_LEN + 1];

                std::string client_passwd = _read_buffer.getString();
                if (client_passwd.empty() || client_passwd.size() != MD5_PASSWD_LEN) {
                    SPDLOG_ERROR("Empty password received; or password length mismatch");
                    _state = ERROR;
                    return;
                }

                // calculate md5 hash; skip the 'md5' prefix on the password
                if (!pg_md5_encrypt(_login->_password.c_str()+3, reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
                    SPDLOG_ERROR("Failed to calculate MD5 hash");
                    _state = ERROR;
                    return;
                }
                md5[MD5_PASSWD_LEN] = '\0';

                if (strcmp(md5, client_passwd.c_str()) != 0) {
                    SPDLOG_ERROR("MD5 password mismatch: : {} <> {}", md5, client_passwd);
                    ProxyError::encode_error(_write_buffer, ProxyError::INVALID_PASSWORD, "password authentication failed");
                    _connection->write(_write_buffer.data(), _write_buffer.size());
                    _state = ERROR;
                    return;
                }

                SPDLOG_DEBUG("MD5 password match");
                _do_server_auth();

                return;
            }

            case SCRAM: {
                // see if this is the first or second message
                if (_login->scram_state.server_nonce == nullptr) {
                    SPDLOG_DEBUG("Handling SCRAM SASL initial response");

                    // process as SASLInitialResponse
                    std::string scram_type = _read_buffer.getString();
                    if (scram_type != "SCRAM-SHA-256") {
                        SPDLOG_ERROR("Unsupported scram type: {}", scram_type);
                        _state = ERROR;
                        return;
                    }

                    int32_t len = _read_buffer.get32();
                    std::string data = _read_buffer.getBytes(len);
                    _handle_scram_auth(data);
                } else {
                    SPDLOG_DEBUG("Handling SCRAM SASL response");
                    // process as SASLResponse
                    std::string data = _read_buffer.getBytes(msg_length);
                    _handle_scram_auth_continue(data);
                }

                break;
            }

            default:
                SPDLOG_ERROR("Invalid auth continue state");
                _state = ERROR;
                break;
        }
    }

    void
    ClientSession::_handle_scram_auth(const std::string &data)
    {
        char *raw = ::strdup(data.c_str()); // copy to remove constness
        if (!read_client_first_message(raw,
                                        &_login->scram_state.cbind_flag,
                                        &_login->scram_state.client_first_message_bare,
                                        &_login->scram_state.client_nonce)) {
            SPDLOG_ERROR("Failed to read client first message");
            _state = ERROR;
            free (raw);
            return;
        }

        // note: some code inside of here could be optimized based on how the password is stored
        if (!build_server_first_message(&_login->scram_state, _user->username().c_str(), _login->_password.c_str())) {
            SPDLOG_ERROR("Failed to build server first message");
            _state = ERROR;
            free (raw);
            return;
        }

        // Send SASL continue message
        _write_buffer.reset();
        _write_buffer.put('R');
        _write_buffer.put32(strlen(_login->scram_state.server_first_message)+8);
        _write_buffer.put32(11); // 11 == SASL continue
        _write_buffer.putBytes(_login->scram_state.server_first_message,
                                strlen(_login->scram_state.server_first_message));

        free (raw);

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());
    }

    void
    ClientSession::_handle_scram_auth_continue(const std::string &data)
    {
        char *raw = ::strdup(data.c_str()); // copy to remove constness
        const char *client_final_nonce = nullptr;
	    char *proof = nullptr;

        // decode the final message from client
        if (!read_client_final_message(&_login->scram_state,
                                        reinterpret_cast<const uint8_t *>(data.c_str()),
                                        raw, &client_final_nonce, &proof)) {
            SPDLOG_ERROR("Failed to read client final message");
            _state = ERROR;
            free (raw);
            return;
        }

        // verify the nonce and the proof from client
        if (!verify_final_nonce(&_login->scram_state, client_final_nonce) ||
            !verify_client_proof(&_login->scram_state, proof)) {
		    SPDLOG_ERROR("Invalid SCRAM response (nonce or proof does not match)");
            _state = ERROR;
            free (raw);
            free (proof);
            return;
	    }

        // after verifying the client proof, we now have the client key
        _user->set_client_scram_key(_login->scram_state.ClientKey);

        // finally send the final message to the client
        char *server_final_message = build_server_final_message(&_login->scram_state);
        if (server_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build server final message");
            _state = ERROR;
            free (raw);
            free (proof);
            return;
        }

        _write_buffer.reset();
        _write_buffer.put('R');
        _write_buffer.put32(strlen(server_final_message)+8);
        _write_buffer.put32(12); // 12 == SASL final
        _write_buffer.putBytes(server_final_message, strlen(server_final_message));

        free (raw);
        free (server_final_message);
        free (proof);

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());

        _do_server_auth();
    }

    void
    ClientSession::_do_server_auth()
    {
        // create a new server session; will initiate the connection to the server
        ServerSessionPtr server_session = ServerSession::create(_server, _user);
        if (server_session == nullptr) {
            SPDLOG_ERROR("Failed to create server session");
            _state = ERROR;
            return;
        }
        // register server session connection with server
        _server->register_session(server_session);

        // link the server session to the client session
        set_associated_session(server_session);

        // add callback to notify client session when server session is done
        notify_server(SessionMsg::MSG_CLIENT_SERVER_STARTUP, server_session);

        // at this point we'll return through process()
        // most likely with _waiting_on_session set, in which case this
        // session will be removed from the server poll list
    }

    void
    ClientSession::_handle_server_error(const std::string_view msg)
    {
        // called from server context
        SPDLOG_ERROR("Client session got error from server session: {}", msg);
        // handle server error
        _state = ERROR;
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
        _write_buffer.put32(_cancel_key);

        // ready for query
        _write_buffer.put('Z');
        _write_buffer.put32(5);
        _write_buffer.put('I');

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());

        // free login info
        _login.reset();

        SPDLOG_DEBUG("Authentication complete, setting READY state");
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
        _write_buffer.put32(12); // length
        _write_buffer.put32(5);  // 5 == md5
        _write_buffer.putBytes(reinterpret_cast<char*>(&_login->_salt), 4);
    }

    void
    ClientSession::_encode_auth_scram()
    {
        _write_buffer.put('R');
        _write_buffer.put32(23); // length
        _write_buffer.put32(10); // 10 == scram
        _write_buffer.putString("SCRAM-SHA-256");
        _write_buffer.put(0);
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
            _state = ERROR;
            return;
        }
        default:
            SPDLOG_ERROR("Unsupported request code: {}", code);
            _state = ERROR;
            return;
        }
    }

}