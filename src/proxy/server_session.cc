#include <common/logging.hh>

#include <proxy/server_session.hh>
#include <proxy/server.hh>
#include <proxy/connection.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/sha256.h>
#include <proxy/auth/scram.hh>

namespace springtail {
    ServerSession::ServerSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server,
                                 UserPtr user)
        : Session(connection, server, user, PRIMARY)
    {}

    void
    ServerSession::_process_msg(SessionMsg msg)
    {
        switch(msg) {
        case MSG_CLIENT_SERVER_STARTUP:
            // startup message from client
            _handle_startup();
            break;

        default:
            SPDLOG_WARN("Unknown message: {}", (int8_t)msg);
            break;
        }

        enable_processing();
    }

    // entry point for connection message processing
    void
    ServerSession::_process_connection()
    {
        switch(_state) {
        case AUTH:
            // auth handling
            _handle_auth();
            break;
        case AUTH_DONE:
            // post auth handling, prior to ready for query
            _handle_auth_done();
            break;
        case READY:
            // ready for query, handle requests
            break;
        default:
            _state = ERROR;
            break;
        }
    }

    void
    ServerSession::_handle_startup()
    {
        // Send startup message
        _write_buffer.reset();
        _write_buffer.put32(4 + 5 + 9 + 17 + 11 + 16 + 5 + _user->username().size() + _user->dbname().size() + 3); // length
        _write_buffer.put32(MSG_STARTUP_V3); // protocol version
        _write_buffer.putString("user");
        _write_buffer.putString(_user->username());
        _write_buffer.putString("database");
        _write_buffer.putString(_user->dbname());
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
    ServerSession::_handle_auth()
    {
        // Read auth response 'R', we are still in auth flow
        // for SASL, we may have multiple messages
        // for MD5, we have one message
        auto [code, msg_length] = _read_msg();
        assert(code == 'R');

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
                int32_t salt = _read_buffer.get32();

                // get user login info
                _get_user_login();
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
                    _state = ERROR;
                    return;
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

                do_msg_send = false;
                break;
            }

            default:
                SPDLOG_ERROR("Unknown auth type: {}", auth_type);
                _state = ERROR;
                return;
        }

        if (_state == ERROR) {
            return;
        }

        // Send auth response
        if (do_msg_send) {
            ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
            assert(n == _write_buffer.size());
        }
    }

    void
    ServerSession::_handle_auth_done()
    {
        // Completed authentication; first expecting auth ok 'R' message
        // followed by 'S' messages for parameter status (may be multiple),
        // followed by 'K' message for backend key data,
        // followed by 'Z' for ready for query
        auto [code, msg_length] = _read_msg();
        if (code == 'E') {
            SPDLOG_ERROR("Error response from server");
            _state = ERROR;
            return;
        }

        switch(code) {
            case 'R': {
                // auth response, at this point should be AUTH_OK
                int32_t status = _read_buffer.get32();
                if (status != 0) {
                    SPDLOG_ERROR("Auth failed: {}", status);
                    _state = ERROR;
                    return;
                }
                // free auth data
                _login = nullptr;
                break;
            }

            case 'S': {
                // Parameter status
                std::string key = _read_buffer.getString();
                std::string value = _read_buffer.getString();

                // may want to store this to give back to client
                SPDLOG_DEBUG("Parameter status from server: {}={}", key, value);

                break;
            }

            case 'K':
                // Backend key data
                _pid = _read_buffer.get32();
                _cancel_key = _read_buffer.get32();
                break;

            case 'Z': {
                // Ready for query
                int32_t status = _read_buffer.get32(); // IDLE = 73
                SPDLOG_DEBUG("Ready for query: {}", status);
                _state = READY;

                // at this point we should notify client session
                // server authentication is done, and we can complete
                // the client session authentication
                notify_client(SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE);

                break;
            }

            case 'N':
                // Notice response
                // ignore for now
                break;

            case 'E':
                // Error response
                SPDLOG_ERROR("Error response from server");
                _state = ERROR;
                break;

            default:
                SPDLOG_ERROR("Unexpected message: {}", code);
                _state = ERROR;
                break;
        }
        // nothing to send back
    }

    void
    ServerSession::_encode_auth_md5()
    {
        _write_buffer.reset();
        _write_buffer.put('p');
        _write_buffer.put32(40); // length

        char md5[MD5_PASSWD_LEN];

        // calculate md5 hash; skip the 'md5' prefix on the password
        assert(_login->_password.starts_with("md5"));
        if (!pg_md5_encrypt(_login->_password.c_str()+3, reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
            SPDLOG_ERROR("Failed to calculate MD5 hash");
            _state = ERROR;
            return;
        }

        _write_buffer.putString(md5);
    }

    void
    ServerSession::_encode_auth_scram()
    {
        char *client_first_message = build_client_first_message(&_login->scram_state);
        if (client_first_message == nullptr) {
            SPDLOG_ERROR("Failed to build client first message");
            _state = ERROR;
            return;
        }

        int32_t len = strlen(client_first_message);

        _write_buffer.reset();
        _write_buffer.put('p');
        _write_buffer.put32(4+14+4+len); // length
        _write_buffer.putString("SCRAM-SHA-256");
        _write_buffer.put(len); // length of data
        _write_buffer.putBytes(client_first_message, len);

        free(client_first_message);
    }

    void
    ServerSession::_encode_auth_scram_continue(const std::string &data)
    {
        if (_login->scram_state.client_nonce == nullptr) {
            SPDLOG_ERROR("No client nonce set");
            _state = ERROR;
            return;
        }

        if (_login->scram_state.server_first_message != nullptr) {
            SPDLOG_ERROR("Received second SCRAM-SHA-256 continue message");
            _state = ERROR;
            return;
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
            _state = ERROR;
            return;
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
            _state = ERROR;
            return;
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
            _state = ERROR;
            return;
        }

        char *input = strdup(data.c_str());
        char ServerSignature[SHA256_DIGEST_LENGTH];

        // decode the final message from server
        if (!read_server_final_message(input, ServerSignature)) {
            SPDLOG_ERROR("Failed to read server final message");
            free(input);
            _state = ERROR;
            return;
        }

        PgUser user;
        user.scram_ClientKey = _login->scram_state.ClientKey;
        user.scram_ServerKey = _login->scram_state.ServerKey; // XXX need to get this from somewhere
        user.has_scram_keys = true;

        // last step, verify the server signature
        if (!verify_server_signature(&_login->scram_state, &user, ServerSignature)) {
            SPDLOG_ERROR("Failed to verify server signature");
            free(input);
            _state = ERROR;
            return;
        }

        free(input);

        _state = AUTH_DONE;
    }

    /** factory to create session */
    std::shared_ptr<ServerSession>
    ServerSession::create(ProxyServerPtr server, UserPtr user)
    {
        auto database = user->get_database();
        auto connection = database->create_connection();
        return std::make_shared<ServerSession>(connection, server, user);
    }
}