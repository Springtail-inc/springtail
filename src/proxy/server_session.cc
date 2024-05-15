#include <openssl/ssl.h>
#include <openssl/err.h>

#include <common/logging.hh>

#include <proxy/server_session.hh>
#include <proxy/server.hh>
#include <proxy/connection.hh>
#include <proxy/errors.hh>
#include <proxy/exception.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/sha256.h>
#include <proxy/auth/scram.hh>

namespace springtail {
    ServerSession::ServerSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server,
                                 UserPtr user,
                                 std::string database,
                                 DatabaseInstancePtr instance,
                                 Session::Type type)
        : Session(instance, connection, server, user, database, type)
    {
        _state = STARTUP;
        SPDLOG_DEBUG("Server connected: endpoint={}, id={}", connection->endpoint(), _id);
    }

    void
    ServerSession::_process_msg(SessionMsgPtr msg)
    {
        // entry point for message processing from client session
        switch(msg->type) {
        case SessionMsg::MSG_CLIENT_SERVER_STARTUP:
            // this is the startup message from client session
            if (_server->is_ssl_enabled()) {
                // send ssl request to server
                _send_ssl_req();
            } else {
                // otherwise send the startup message
                _send_startup_msg();
            }
            break;

        case SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY:
            _handle_simple_query(std::get<std::string>(msg->data));
            break;

        default:
            SPDLOG_WARN("Unknown message: {:d}", (int8_t)msg->type);
            break;
        }
    }

    void
    ServerSession::_process_connection()
    {
        SPDLOG_DEBUG("Server session processing connection: state={:d}", (int8_t)_state);

        // entry point for connection message processing
        // called from operator() in session
        switch(_state) {
        case STARTUP:
            _handle_ssl_response();
            break;
        case SSL_HANDSHAKE:
            _handle_ssl_handshake();
            break;
        case AUTH:
        case AUTH_DONE:
        case READY:
            // ready for query, handle requests
            _handle_message();
            break;
        default:
            _state = ERROR;
            break;
        }
    }

    void
    ServerSession::_handle_message()
    {
        // Read messages from server session

        // May be in AUTH_DONE or READY state
        // If in AUTH_DONE state:
        // Completed authentication; first expecting auth ok 'R' message
        // followed by 'S' messages for parameter status (may be multiple),
        // followed by 'K' message for backend key data,
        // followed by 'Z' for ready for query

        _read_buffer.reset();
        auto [code, msg_length] = _read_hdr();

        SPDLOG_DEBUG("Server session message: code={}, length={}", code, msg_length);

        switch(code) {
            case 'R': {
                _read_remaining(msg_length);

                if (_state == AUTH) {
                    // still in auth negotiation state
                    _handle_auth(msg_length);
                    break;
                }

                // done with auth, this should be last message
                assert(_state == AUTH_DONE);

                // auth response, at this point should be AUTH_OK
                int32_t status = _read_buffer.get32();
                if (status != 0) {
                    SPDLOG_ERROR("Auth failed: {}", status);
                    throw ProxyAuthError();
                }

                // free auth data
                assert(_login != nullptr); // should have been set in _handle_auth
                _login = nullptr;

                break;
            }

            case 'S': {
                assert(_state == AUTH_DONE);
                _read_remaining(msg_length);

                // Parameter status
                std::string key = _read_buffer.getString();
                std::string value = _read_buffer.getString();

                // may want to store this to give back to client
                SPDLOG_DEBUG("Parameter status from server: {}={}", key, value);

                break;
            }

            case 'K':
                assert(_state == AUTH_DONE);
                _read_remaining(msg_length);
                // Backend key data
                _pid = _read_buffer.get32();
                _cancel_key = _read_buffer.get32();
                break;

            case 'E': {
                // Error response
                _read_remaining(msg_length);
                const char *data = _read_buffer.current_data();

                _handle_error_code(data, msg_length);
                break;
            }

            case 'C':
                // Command complete
                SPDLOG_DEBUG("Command complete");
                _stream_to_remote_session(code, msg_length);
                break;

            case 'Z': {
                // Ready for query
                SPDLOG_DEBUG("Ready for query");

                if (_state == AUTH_DONE) {
                    _read_remaining(msg_length);
                    _state = READY;
                    // at this point we should notify client session
                    // server authentication is done, and we can complete
                    // the client session authentication
                    SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE);
                    notify_client(msg);
                    break;
                }

                // send ready for query to client
                _stream_to_remote_session(code, msg_length);

                // notify client session that we are ready for query
                SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_READY);
                notify_client(msg);

                break;
            }

            case 'T':
                // Row description
                SPDLOG_DEBUG("Row description");
                _stream_to_remote_session(code, msg_length);
                break;

            case 'D':
                // Data row
                SPDLOG_DEBUG("Data row");
                _stream_to_remote_session(code, msg_length);
                break;

            case 'N':
                // Notice response
                SPDLOG_DEBUG("Notice response");
                _stream_to_remote_session(code, msg_length);
                break;

            case 'X':
                // Terminate
                SPDLOG_DEBUG("Terminate");
                _stream_to_remote_session(code, msg_length);
                _state = ERROR;
                break;

            default:
                SPDLOG_ERROR("Unknown message: {}", code);
                _state = ERROR;
                break;
        }

        SPDLOG_DEBUG("Done msg handling: Remaining: {}", _read_buffer.remaining());
    }

    void
    ServerSession::_send_ssl_req()
    {
        // Send ssl message
        _write_buffer.reset();
        _write_buffer.put32(8); // length
        _write_buffer.put32(MSG_SSLREQ); // SSL request code
        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());
    }

    void
    ServerSession::_handle_ssl_response()
    {
        // Read startup ssl message from server in response to send_startup
        // Just one character: 'N' no ssl or 'S' yes ssl
        _read_buffer.reset();
        ssize_t n = _connection->read(_read_buffer, 1);
        assert(n==1);

        char ssl_response = _read_buffer.get();
        SPDLOG_DEBUG("SSL response from server: {}", ssl_response);
        if (ssl_response == 'S') {
            // server is ready for ssl negotiation
            _send_ssl_handshake();
        } else {
            // server is ready for startup message
            _send_startup_msg();
        }
    }

    void
    ServerSession::_send_ssl_handshake()
    {
        // create the SSL object from the server's context, acting as a client
        SSL *ssl = _server->SSL_new(false);
        if (ssl == nullptr) {
            SPDLOG_ERROR("Failed to create SSL object");
            _state = ERROR;
            return;
        }

        // set the SSL object on the connection
        _connection->setup_SSL(ssl);

        // do the SSL handshake
        _state = SSL_HANDSHAKE;

        _handle_ssl_handshake();
    }

    void
    ServerSession::_handle_ssl_handshake()
    {
        // do the SSL handshake; exception thrown on fatal error
        int rc = _connection->SSL_connect();
        if (rc < 0) {
            SPDLOG_DEBUG("SSL server handshake in progress, need more data");
            return;
        }

        SPDLOG_DEBUG("SSL server handshake complete");

        // send the startup message and then move to AUTH state
        _send_startup_msg();
    }

    void
    ServerSession::_send_startup_msg()
    {
        // Send startup message
        _write_buffer.reset();
        _write_buffer.put32(8 + 5 + 9 + 17 + 11 + 16 + 5 + _user->username().size() + _database.size() + 3); // length
        _write_buffer.put32(MSG_STARTUP_V3); // protocol version
        _write_buffer.putString("user");
        _write_buffer.putString(_user->username());
        _write_buffer.putString("database");
        _write_buffer.putString(_database);
        _write_buffer.putString("application_name");
        _write_buffer.putString("Springtail");
        _write_buffer.putString("client_encoding");
        _write_buffer.putString("UTF8");
        _write_buffer.put(0); // null terminator

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());

        _state = AUTH;
    }

    void
    ServerSession::_handle_auth(int32_t msg_length)
    {
        // Read auth response 'R', we are still in auth flow
        // for SASL, we may have multiple messages
        // for MD5, we have one message
        int32_t auth_type = _read_buffer.get32();
        bool do_msg_send = true;

        switch (auth_type) {
            case MSG_AUTH_OK:
                SPDLOG_DEBUG("Auth type: OK");
                _state = AUTH_DONE;
                do_msg_send = false;
                break;

            case MSG_AUTH_MD5: {
                SPDLOG_DEBUG("Auth type: MD5");
                // read in the salt from the server
                int32_t salt;
                _read_buffer.getBytes(reinterpret_cast<char*>(&salt), 4);

                // get user login info
                _login = _get_user_login();
                if (_login == nullptr) {
                    throw ProxyAuthError();
                }
                _login->_salt = salt;

                // encode md5 auth response
                _encode_auth_md5();

                // set state to auth done
                _state = AUTH_DONE;
                break;
            }

            case MSG_AUTH_SASL: {
                // first message in SASL flow (SCRAM-SHA-256)
                SPDLOG_DEBUG("Auth type: SASL");

                // get user login info
                _login = _get_user_login();
                if (_login == nullptr) {
                    SPDLOG_ERROR("Failed to get user login info");
                    throw ProxyAuthError();
                }

                // check that the server supports the SCRAM-SHA-256 mechanism
                bool found = false;
                do {
                    std::string mechanism = _read_buffer.getString();
                    if (mechanism == "SCRAM-SHA-256") {
                        found = true;
                    }
                } while (!found && _read_buffer.remaining() > 0);

                if (!found) {
                    SPDLOG_ERROR("No SASL mechanism found matching: SCRAM-SHA-256");
                    throw ProxyAuthError();
                }

                // encode reply for first scram message to server
                _encode_auth_scram();
                break;
            }

            case MSG_AUTH_SASL_CONTINUE: {
                // continue SASL flow
                SPDLOG_DEBUG("Auth type: SASL continue");

                // encode reply to continue message
                std::string data = _read_buffer.getBytes(msg_length-4);
                _encode_auth_scram_continue(data);

                break;
            }

            case MSG_AUTH_SASL_COMPLETE: {
                // complete SASL flow
                SPDLOG_DEBUG("Auth type: SASL complete");

                // verify the server signature
                std::string data = _read_buffer.getBytes(msg_length-4);
                _handle_auth_scram_complete(data);

                SPDLOG_DEBUG("SASL authentication complete: set AUTH_DONE");
                _state = AUTH_DONE;

                do_msg_send = false;
                break;
            }

            default:
                SPDLOG_ERROR("Unknown auth type: {}", auth_type);
                throw ProxyAuthError();
        }

        if (_state == ERROR) {
            return;
        }

        // Send auth response
        if (do_msg_send) {
            ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
            assert(n == _write_buffer.size());
        }

        SPDLOG_DEBUG("Auth response sent to server: remaining: {}", _read_buffer.remaining());
    }

    void
    ServerSession::_encode_auth_md5()
    {
        _write_buffer.reset();
        _write_buffer.put('p');
        _write_buffer.put32(40); // length

        char md5[MD5_PASSWD_LEN+1];

        // calculate md5 hash; skip the 'md5' prefix on the password; add salt and compute
        assert(_login->_password.starts_with("md5"));
        if (!pg_md5_encrypt(_login->_password.c_str()+3, reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
            SPDLOG_ERROR("Failed to calculate MD5 hash");
            throw ProxyAuthError();
        }
        md5[MD5_PASSWD_LEN] = '\0';

        _write_buffer.putString(md5);
    }

    void
    ServerSession::_encode_auth_scram()
    {
        char *client_first_message = build_client_first_message(&_login->scram_state);
        if (client_first_message == nullptr) {
            SPDLOG_ERROR("Failed to build client first message");
            throw ProxyAuthError();
        }

        int32_t len = strlen(client_first_message);

        _write_buffer.reset();
        _write_buffer.put('p');
        _write_buffer.put32(4+14+4+len); // length
        _write_buffer.putString("SCRAM-SHA-256");
        _write_buffer.put32(len); // length of data
        _write_buffer.putBytes(client_first_message, len);

        free(client_first_message);
    }

    void
    ServerSession::_encode_auth_scram_continue(const std::string &data)
    {
        if (_login->scram_state.client_nonce == nullptr) {
            SPDLOG_ERROR("No client nonce set");
            throw ProxyAuthError();
        }

        if (_login->scram_state.server_first_message != nullptr) {
            SPDLOG_ERROR("Received second SCRAM-SHA-256 continue message");
            throw ProxyAuthError();
        }

        int salt_len;
        char *input = strdup(data.c_str());

        if (!read_server_first_message(&_login->scram_state, input,
                                       &_login->scram_state.server_nonce,
                                       &_login->scram_state.salt,
                                       &salt_len,
                                       &_login->scram_state.iterations)) {
            SPDLOG_ERROR("Failed to read server first message");
            free (input);
            throw ProxyAuthError();
        }

        PgUser user;
        user.scram_ClientKey = _login->scram_state.ClientKey;
        user.has_scram_keys = true;

        char *client_final_message = build_client_final_message(&_login->scram_state,
			&user, _login->scram_state.server_nonce,
			_login->scram_state.salt, salt_len, _login->scram_state.iterations);

        free(input);

        if (client_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build client final message");
            throw ProxyAuthError();
        }

        _write_buffer.reset();
        _write_buffer.put('p');
        _write_buffer.put32(4+strlen(client_final_message)); // length
        _write_buffer.putBytes(client_final_message, strlen(client_final_message));

        free(client_final_message);
    }

    void
    ServerSession::_handle_auth_scram_complete(const std::string &data)
    {
        // make sure we are in right flow
        if (_login->scram_state.server_first_message == nullptr) {
            SPDLOG_ERROR("No server first message set");
            throw ProxyAuthError();
        }

        char *input = strdup(data.c_str());
        char ServerSignature[SHA256_DIGEST_LENGTH];

        // decode the final message from server
        if (!read_server_final_message(input, ServerSignature)) {
            SPDLOG_ERROR("Failed to read server final message");
            free(input);
            throw ProxyAuthError();
        }

        PgUser user;
        user.scram_ClientKey = _login->scram_state.ClientKey;
        user.scram_ServerKey = _login->scram_state.ServerKey; // XXX need to get this from somewhere
        user.has_scram_keys = true;

        // last step, verify the server signature
        if (!verify_server_signature(&_login->scram_state, &user, ServerSignature)) {
            SPDLOG_ERROR("Failed to verify server signature");
            free(input);
            throw ProxyAuthError();
        }

        free(input);
    }

    void
    ServerSession::_handle_simple_query(const std::string &query)
    {
        // Send simple query to server
        _write_buffer.reset();
        _write_buffer.put('Q');
        _write_buffer.put32(4 + query.size() + 1); // length
        _write_buffer.putString(query);

        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());
    }

    void
    ServerSession::_handle_error_code(const char *data, int32_t msg_length)
    {
        // Error response
        SPDLOG_ERROR("Error response from server");

        std::string severity;
        std::string text;
        std::string code;
        std::string message;

        ProxyProtoError::decode_error(_read_buffer, severity, text, code, message);

        // send error to client
        _send_to_remote_session('E', msg_length, data);

        // depending on error, behavior is different
        // if text is "FATAL" or "PANIC" we should stop, sever connection
        if (text == "FATAL" || text == "PANIC") {
            _state = ERROR;
        }

        // if not fatal then wait for ready for query from server
        return;
    }

    /** factory to create session */
    std::shared_ptr<ServerSession>
    ServerSession::create(ProxyServerPtr server,
                          UserPtr user,
                          const std::string &database,
                          DatabaseInstancePtr instance,
                          Session::Type type)
    {
        if (instance == nullptr) {
            assert (type == Session::Type::PRIMARY);
            instance = server->get_primary_instance();
        }

        auto connection = instance->create_connection();
        if (connection == nullptr) {
            SPDLOG_ERROR("Failed to create connection for server db");
            return nullptr;
        }

        SPDLOG_DEBUG("Created connection for server session, to: db={}", database);

        return std::make_shared<ServerSession>(connection, server, user, database, instance, type);
    }
}