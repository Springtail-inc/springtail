#include <openssl/err.h>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/authorization.hh>
#include <proxy/logging.hh>
#include <proxy/exception.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/user_mgr.hh>
#include <proxy/database.hh>
#include <proxy/errors.hh>
#include <proxy/server.hh>

namespace springtail::pg_proxy {

    bool
    ClientAuthorization::process_auth_data(uint64_t seq_id)
    {
        switch (_state) {
            case STARTUP:
                _handle_startup(seq_id);
                break;

            case SSL_HANDSHAKE:
                _handle_ssl_handshake();
                break;

            case AUTH:
                _handle_auth(seq_id);
                break;

            default:
                // do nothing
                break;
        }

        if (_state == ERROR) {
            throw ProxyAuthError();
        }

        return (_state == READY);
    }

    void ClientAuthorization::_handle_startup(uint64_t seq_id)
    {
        // read the startup message
        char buffer[8];
        ssize_t n = _connection->read(buffer, 8, 8);
        CHECK_EQ(n, 8);

        int32_t msg_length = recvint32(buffer)-4;
        int32_t code = recvint32(buffer+4);

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Startup message: msg_length={}, code={}, seq_id={}", _id, msg_length, code, seq_id);

        switch (code) {
            case MSG_SSLREQ:
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] SSL negotiation requested", _id);
                _process_ssl_request();
                break;

            case MSG_STARTUP_V2:
                SPDLOG_ERROR("Startup message version 2.0, not supported");
                // not supported
                _state = ERROR;
                break;

            case MSG_STARTUP_V3:
                _process_startup_msg(msg_length-4, seq_id);
                break;

            default:
                SPDLOG_ERROR("Invalid startup message code: {}", code);
                _state = ERROR;
                break;
        }
    }

    void
    ClientAuthorization::_handle_ssl_handshake()
    {
        // try the SSL accept
        // this will return 1 on success, -1 if more data is needed; throws exception on fatal error
        int rc = _connection->SSL_accept();
        if (rc < 0) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] SSL client handshake in progress, need more data", _id);
            return;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] SSL client handshake complete", _id);

        _state = STARTUP;
    }

    void
    ClientAuthorization::_process_startup_msg(int32_t remaining, uint64_t seq_id)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Proto version 3.0 requested", _id);

        // read parameter strings
        std::string key;
        std::string value;
        std::string username;
        std::string database;

        // this shouldn't be too big
        assert(remaining <= 4096);

        char buffer[remaining];
        ssize_t n = _connection->read(buffer, remaining, remaining);
        CHECK_EQ(n, remaining);

        Buffer read_buffer(buffer, remaining, remaining);
        Session::log_buffer(Session::Type::CLIENT, _id, true, '?', remaining, buffer, seq_id);

        // seems to be a trailing null byte on the end
        while (read_buffer.remaining() > 1) {
            key = read_buffer.get_string();
            value = read_buffer.get_string();

            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Parameter: {}={}", _id, key, value);

            if (key == "user") {
                username = value;
            } else if (key == "database") {
                database = value;
            } else {
                _parameters[key] = value;
            }
        }
        // read last null byte
        char c = read_buffer.get();
        CHECK_EQ(c, '\0');

        // get user info and store it
        _user = UserMgr::get_instance()->get_user(username, database);
        if (_user == nullptr) {
            SPDLOG_ERROR("User {} not found", username);
            _state = ERROR;
            return;
        }
        _database = database;
        auto optional_db_id = DatabaseMgr::get_instance()->get_database_id(_database);
        if (!optional_db_id.has_value()) {
            SPDLOG_ERROR("Database {} not found", _database);
            _state = ERROR;
            return;
        }
        _db_id = optional_db_id.value();

        // get login info for the user
        _login = _user->get_user_login();

        // handle authentication -- send auth request
        _send_auth_req(seq_id);
    }

    void
    ClientAuthorization::_process_ssl_request()
    {
        // send response 'S' or 'N' for SSL or no SSL respectively
        char response;
        if (!ProxyServer::get_instance()->is_ssl_enabled()) {
            response = 'N';
            _connection->write(&response, 1);
            return;
        }

        // SSL is enabled, send 'S' and start handshake
        response = 'S';
        _connection->write(&response, 1);

        // allocate ssl struct for this connection; acting as server
        SSL *ssl = ProxyServer::get_instance()->SSL_new(true);
        if (ssl == nullptr) {
            SPDLOG_ERROR("Failed to create SSL context");
            _state = ERROR;
            return;
        }

        // init ssl on connection
        _connection->setup_SSL(ssl);

        // set state to SSL Handshake
        _state = SSL_HANDSHAKE;

        // start handshake; starts ssl_accept()
        _handle_ssl_handshake();
    }

    void
    ClientAuthorization::_send_auth_req(uint64_t seq_id)
    {
        _state = AUTH;

        BufferPtr buffer = BufferPool::get_instance()->get(128);

        switch(_login->type) {
            case TRUST:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] User {} authenticated with trust", _id, _user->username());
                _state = READY;
                return; // did send above so we return here

            case MD5:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] User {} authenticating with md5", _id, _user->username());
                _encode_auth_md5(buffer);
                break;

            case SCRAM:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] User {} authenticating with scram", _id, _user->username());
                _encode_auth_scram(buffer);
                break;

            default:
                SPDLOG_ERROR("User {} not found", _user->username());
                ProxyProtoError::encode_error(buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                throw ProxyAuthError();
        }

        // we've encoded the auth message above, now we send it, for AUTH_OK it is already sent
        ssize_t n = _connection->write(buffer->data(), buffer->size());
        CHECK_EQ(n, buffer->size());

        // log the buffer
        Session::log_buffer(Session::Type::CLIENT, _id, false, '\0', buffer->size(), buffer->data(), seq_id);
    }

    void
    ClientAuthorization::_handle_auth(uint64_t seq_id)
    {
        BufferList blist;

        // read chain of messages
        Session::read_msg(_connection, blist);

        // iterate through messages
        for (auto buffer: blist.buffers)
        {
            char code = buffer->get();
            CHECK_EQ(code, 'p');

            int32_t msg_length = buffer->get32() - 4; // subtract 4 for length field

            // log buffer
            Session::log_buffer(Session::Type::CLIENT, _id, true, code, msg_length, buffer->current_data(), seq_id);

            PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Auth continue: msg_length={}, seq_id={}", _id, msg_length, seq_id);

            switch(_login->type) {
                case MD5: {
                    char md5[MD5_PASSWD_LEN + 1];

                    std::string_view client_passwd = buffer->get_string();
                    if (client_passwd.empty() || client_passwd.size() != MD5_PASSWD_LEN) {
                        SPDLOG_ERROR("Empty password received; or password length mismatch");
                        throw ProxyAuthError();
                    }

                    // calculate md5 hash; skip the 'md5' prefix on the password
                    if (!pg_md5_encrypt(_login->password.c_str()+3, reinterpret_cast<char*>(&_login->salt), 4, md5)) {
                        SPDLOG_ERROR("Failed to calculate MD5 hash");
                        throw ProxyAuthError();
                    }
                    md5[MD5_PASSWD_LEN] = '\0';

                    if (strcmp(md5, client_passwd.data()) != 0) {
                        SPDLOG_ERROR("MD5 password mismatch: : {} <> {}", md5, client_passwd);
                        char data[128];
                        BufferPtr write_buffer = std::make_shared<Buffer>(data, 128);
                        ProxyProtoError::encode_error(write_buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                        _connection->write(write_buffer->data(), write_buffer->size());

                        // log buffer
                        Session::log_buffer(Session::Type::CLIENT, _id, false, '\0', write_buffer->size(), write_buffer->data(), seq_id);

                        throw ProxyAuthError();
                    }

                    PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] MD5 password match", _id);

                    // auth successful on client side
                    _state = READY;

                    return;
                }

                case SCRAM: {
                    // see if this is the first or second message
                    if (_login->scram_state.server_nonce == nullptr) {
                        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Handling SCRAM SASL initial response", _id);

                        // process as SASLInitialResponse
                        std::string_view scram_type = buffer->get_string();
                        if (scram_type != "SCRAM-SHA-256") {
                            SPDLOG_ERROR("Unsupported scram type: {}", scram_type);
                            throw ProxyAuthError();
                        }

                        int32_t len = buffer->get32();
                        std::string_view data = buffer->get_bytes(len);
                        _handle_scram_auth(data, seq_id);
                    } else {
                        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Handling SCRAM SASL response", _id);
                        // process as SASLResponse
                        std::string_view data = buffer->get_bytes(msg_length);
                        _handle_scram_auth_continue(data, seq_id);
                    }

                    break;
                }

                default:
                    SPDLOG_ERROR("Invalid auth continue state");
                    throw ProxyAuthError();
            }
        }
    }

    void
    ClientAuthorization::_handle_scram_auth(const std::string_view data, uint64_t seq_id)
    {
        char *raw = new char[data.size() + 1];
        strncpy(raw, data.data(), data.size());
        raw[data.size()] = '\0';

        if (!read_client_first_message(raw,
                                        &_login->scram_state.cbind_flag,
                                        &_login->scram_state.client_first_message_bare,
                                        &_login->scram_state.client_nonce)) {
            SPDLOG_ERROR("Failed to read client first message");
            delete[] raw;
            throw ProxyAuthError();
        }

        delete[] raw;

        // note: some code inside of here could be optimized based on how the password is stored
        if (!build_server_first_message(&_login->scram_state, _user->username().c_str(), _login->password.c_str())) {
            SPDLOG_ERROR("Failed to build server first message");
            throw ProxyAuthError();
        }

        // Send SASL continue message
        int msg_len = strlen(_login->scram_state.server_first_message) + 8;
        BufferPtr write_buffer = BufferPool::get_instance()->get(msg_len + 1);
        write_buffer->put('R');
        write_buffer->put32(msg_len);
        write_buffer->put32(11); // 11 == SASL continue
        write_buffer->put_bytes(_login->scram_state.server_first_message,
                                 strlen(_login->scram_state.server_first_message));

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        CHECK_EQ(n, write_buffer->size());

        // log buffer
        Session::log_buffer(Session::Type::CLIENT, _id, false, '\0', write_buffer->size(), write_buffer->data(), seq_id);
    }

    void
    ClientAuthorization::_handle_scram_auth_continue(const std::string_view data, uint64_t seq_id)
    {
        char *raw = new char[data.size() + 1];
        strncpy(raw, data.data(), data.size());
        raw[data.size()] = '\0';
        const char *client_final_nonce = nullptr;
	    char *proof = nullptr;

        // decode the final message from client
        if (!read_client_final_message(&_login->scram_state,
                                        reinterpret_cast<const uint8_t *>(data.data()),
                                        raw, &client_final_nonce, &proof)) {
            SPDLOG_ERROR("Failed to read client final message");
            delete[] raw;
            throw ProxyAuthError();
        }

        // verify the nonce and the proof from client
        if (!verify_final_nonce(&_login->scram_state, client_final_nonce) ||
            !verify_client_proof(&_login->scram_state, proof)) {
		    SPDLOG_ERROR("Invalid SCRAM response (nonce or proof does not match)");
            delete[] raw;
            free (proof);

            throw ProxyAuthError();
	    }
        delete[] raw;
        free (proof);

        // after verifying the client proof, we now have the client key
        _user->set_client_scram_key(_login->scram_state.ClientKey);

        // finally send the final message to the client
        char *server_final_message = build_server_final_message(&_login->scram_state);
        if (server_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build server final message");

            throw ProxyAuthError();
        }

        int msg_len = strlen(server_final_message)+8;
        BufferPtr write_buffer = BufferPool::get_instance()->get(msg_len+1);

        write_buffer->put('R');
        write_buffer->put32(msg_len);
        write_buffer->put32(12); // 12 == SASL final
        write_buffer->put_bytes(server_final_message, strlen(server_final_message));

        free (server_final_message);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        CHECK_EQ(n, write_buffer->size());

        // log buffer
        Session::log_buffer(Session::Type::CLIENT, _id, false, '\0', write_buffer->size(), write_buffer->data(), seq_id);

        _state = READY;
    }

    void
    ClientAuthorization::send_auth_done(uint64_t seq_id,
        const std::unordered_map<std::string, std::string> &parameters)
    {
        // send auth ok, parameter status, backend key data, ready for query
        // 1024 should be more than big enough for all of these
        BufferPtr buffer = BufferPool::get_instance()->get(1024);

        // encode auth ok
        _encode_auth_ok(buffer);

        // send final set of params followed by ready for query
        // parameter status
        _encode_parameter_status(buffer, "server_encoding", "UTF8");
        _encode_parameter_status(buffer, "client_encoding", "UTF8");
        _encode_parameter_status(buffer, "server_version", SERVER_VERSION);
        _encode_parameter_status(buffer, "standard_conforming_strings", "on");
        _encode_parameter_status(buffer, "integer_datetimes", "on");
        _encode_parameter_status(buffer, "session_authorization", _user->username());

        // backend key data -- for cancellation
        buffer->put('K');
        buffer->put32(12);
        buffer->put32(_pid);
        buffer->put32(_cancel_key);

        // ready for query -- Idle state
        buffer->put('Z');
        buffer->put32(5);
        buffer->put('I');

        ssize_t n = _connection->write(buffer->data(), buffer->size());
        CHECK_EQ(n, buffer->size());

        // log buffer
        Session::log_buffer(Session::Type::CLIENT, _id, false, '\0', buffer->size(), buffer->data(), seq_id);

        // free login info
        _login.reset();

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session auth done, ready for queries", _id);
    }

    void
    ClientAuthorization::_encode_auth_ok(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(8);
        buffer->put32(0);
    }

    void
    ClientAuthorization::_encode_auth_md5(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(12); // length
        buffer->put32(5);  // 5 == md5
        buffer->put_bytes(reinterpret_cast<char*>(&_login->salt), 4);
    }

    void
    ClientAuthorization::_encode_auth_scram(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(23); // length
        buffer->put32(10); // 10 == scram
        buffer->put_string("SCRAM-SHA-256");
        buffer->put(0);
    }

    void
    ClientAuthorization::_encode_parameter_status(BufferPtr buffer, const std::string &key, const std::string &value)
    {
        buffer->put('S');
        buffer->put32(key.size() + value.size() + 6); // 4B len + 2B nulls
        buffer->put_string(key);
        if (value.empty()) {
            buffer->put(0);
        } else {
            buffer->put_string(value);
        }
    }

    ///////// Server Authorization

    bool
    ServerAuthorization::process_auth_data(uint64_t seq_id)
    {
        switch(_state) {
            case STARTUP:
                _handle_ssl_response(seq_id);
                break;
            case SSL_HANDSHAKE:
                _handle_ssl_handshake(seq_id);
                break;
            case AUTH:
            case AUTH_DONE:
                _handle_message(seq_id);
                break;
            case READY:
                CHECK_NE(_state, READY);
                break;
            case ERROR:
                throw ProxyAuthError();
        }

        if (_state == ERROR) {
            throw ProxyAuthError();
        }

        return (_state == READY);
    }

    void
    ServerAuthorization::_handle_message(uint64_t seq_id)
    {
        // If in AUTH_DONE state:
        // Completed authentication; first expecting auth ok 'R' message
        // followed by 'S' messages for parameter status (may be multiple),
        // followed by 'K' message for backend key data,
        // followed by 'Z' for ready for query
        auto [code, msg_length] = Session::read_hdr(_connection);
        BufferPtr buffer = Session::read_msg(_connection, code, msg_length, _type, _id, seq_id);

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server authorization message: code={}, length={}, state={}",
                    _id, code, msg_length, (int8)_state);

        switch(code) {
            case 'R': {
                // authentication request
                if (_state == AUTH) {
                    // still in auth negotiation state
                    _handle_auth(buffer, seq_id);
                    break;
                }
                                // done with auth, this should be last message
                CHECK_EQ(_state, AUTH_DONE);

                // auth response, at this point should be AUTH_OK
                int32_t status =  buffer->get32();
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
                // Parameter status
                std::string_view key = buffer->get_string();
                std::string_view value = buffer->get_string();

                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Parameter status from server: {}={}", _id, key, value);

                if (_state <= AUTH_DONE) {
                    // still in auth negotiation state, save these for the client
                    _server_parameters.emplace(key, value);
                }
                break;
            }

            case 'K':
                // backend key data
                CHECK_EQ(_state, AUTH_DONE);
                // get the backend pid and key for cancel
                _pid = buffer->get32();
                _cancel_key = buffer->get32();
                break;

            case 'Z': {
                // Ready for query
                // I - Idle, T - Transaction, E - Error in transaction
                char status = buffer->get();
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session: Ready for query, status={}", _id, status);

                CHECK_EQ(_state, AUTH_DONE);
                CHECK_EQ(status, 'I');
                _state = READY;

                break;
            }

            case 'E':
                _state = ERROR;
                break;

            default:
                SPDLOG_ERROR("Unknown message: {}", code);
                _state = ERROR;
                break;
        }
    }

    void
    ServerAuthorization::send_startup_msg(uint64_t seq_id)
    {
        // this is the startup message from client session
        if (ProxyServer::get_instance()->is_ssl_enabled()) {
            // send ssl request to server
            _send_ssl_req(seq_id);
        } else {
            // otherwise send the startup message
            _send_startup_msg(seq_id);
        }
    }

    void
    ServerAuthorization::_send_ssl_req(uint64_t seq_id)
    {
        // Send ssl message; small buffer, bypass buffer pool
        char data[8];
        BufferPtr buffer = std::make_shared<Buffer>(data, 8);

        buffer->put32(8); // length
        buffer->put32(MSG_SSLREQ); // SSL request code

        _send_buffer(buffer, seq_id, '-');
    }

    void
    ServerAuthorization::_handle_ssl_response(uint64_t seq_id)
    {
        // Read startup ssl message from server in response to send_startup
        // Just one character: 'N' no ssl or 'S' yes ssl
        char ssl_response;
        ssize_t n = _connection->read(&ssl_response, 1, 1);
        CHECK_EQ(n, 1);

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] SSL response from server: {}", _id, ssl_response);
        if (ssl_response == 'S') {
            // server is ready for ssl negotiation
            _send_ssl_handshake(seq_id);
        } else {
            // server is ready for startup message
            _send_startup_msg(seq_id);
        }
    }

    void
    ServerAuthorization::_send_ssl_handshake(uint64_t seq_id)
    {
        // create the SSL object from the server's context, acting as a client
        SSL *ssl = ProxyServer::get_instance()->SSL_new(false);
        if (ssl == nullptr) {
            SPDLOG_ERROR("Failed to create SSL object");
            _state = ERROR;
            return;
        }

        // set the SSL object on the connection
        _connection->setup_SSL(ssl);

        // do the SSL handshake
        _state = SSL_HANDSHAKE;

        _handle_ssl_handshake(seq_id);
    }

    void
    ServerAuthorization::_handle_ssl_handshake(uint64_t seq_id)
    {
        // do the SSL handshake; exception thrown on fatal error
        int rc = _connection->SSL_connect();
        if (rc < 0) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] SSL server handshake in progress, need more data", _id);
            return;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] SSL server handshake complete", _id);

        // send the startup message and then move to AUTH state
        _send_startup_msg(seq_id);
    }

    void
    ServerAuthorization::_send_startup_msg(uint64_t seq_id)
    {
        // Send startup message
        std::string database_name = _db_prefix + _database;

        // (msglen + protocol version) (8) + user (5) + database (9) + 3 null terminators (3)
        int msg_len = 8 + 5 + 9 + _user->username().size() + database_name.size() + 3;

        // iterate over parameters to calculate message length
        for (const auto &param : _parameters) {
            if (param.first == "user" || param.first == "database") {
                continue;
            }
            msg_len += 1 + param.first.size() + 1 + param.second.size();
        }

        BufferPtr buffer = BufferPool::get_instance()->get(msg_len + 4);
        buffer->put32(msg_len);
        buffer->put32(MSG_STARTUP_V3); // protocol version

        buffer->put_string("user");
        buffer->put_string(_user->username());
        buffer->put_string("database");
        buffer->put_string(database_name);

        for (const auto &param : _parameters) {
            if (param.first == "user" || param.first == "database") {
                continue;
            }
            // XXX what about client encoding that is not UTF8?
            buffer->put_string(param.first);
            buffer->put_string(param.second);
        }
        buffer->put(0); // null terminator

        _send_buffer(buffer, seq_id, '?');

        _state = AUTH;
    }

    void
    ServerAuthorization::_handle_auth(BufferPtr buffer, uint64_t seq_id)
    {
        // Read auth response 'R', we are still in auth flow
        // for SASL, we may have multiple messages
        // for MD5, we have one message
        int32_t auth_type = buffer->get32();

        switch (auth_type) {
            case MSG_AUTH_OK:
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Auth type: OK", _id);
                _state = AUTH_DONE;
                break;

            case MSG_AUTH_MD5:
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Auth type: MD5", _id);
                _handle_auth_md5(buffer, seq_id);
                // set state to auth done
                _state = AUTH_DONE;
                break;

            case MSG_AUTH_SASL:
                // first message in SASL flow (SCRAM-SHA-256)
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Auth type: SASL", _id);
                // encode reply for first scram message to server
                _handle_auth_scram(buffer, seq_id);
                break;

            case MSG_AUTH_SASL_CONTINUE:
                // continue SASL flow
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Auth type: SASL continue", _id);
                // encode reply to continue message
                _handle_auth_scram_continue(buffer, seq_id);
                break;

            case MSG_AUTH_SASL_COMPLETE:
                // complete SASL flow
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Auth type: SASL complete", _id);
                // verify the server signature
                _handle_auth_scram_complete(buffer);
                _state = AUTH_DONE;
                break;

            default:
                SPDLOG_ERROR("Unknown auth type: {}", auth_type);
                throw ProxyAuthError();
        }
    }

    void
    ServerAuthorization::_handle_auth_md5(BufferPtr buffer, uint64_t seq_id)
    {
        // read in the salt from the server
        int32_t salt;
        buffer->get_bytes(reinterpret_cast<char*>(&salt), 4);

        // get user login info
        if (_login == nullptr) {
            throw ProxyAuthError();
        }
        _login->salt = salt;

        char md5[MD5_PASSWD_LEN+1];
        // calculate md5 hash; skip the 'md5' prefix on the password; add salt and compute
        assert(_login->password.starts_with("md5"));
        if (!pg_md5_encrypt(_login->password.c_str()+3, reinterpret_cast<char*>(&_login->salt), 4, md5)) {
            SPDLOG_ERROR("Failed to calculate MD5 hash");
            throw ProxyAuthError();
        }
        md5[MD5_PASSWD_LEN] = '\0';

        // encode md5 auth response
        BufferPtr write_buffer = BufferPool::get_instance()->get(41);

        write_buffer->put('p');
        write_buffer->put32(40); // length
        write_buffer->put_string(md5);

        _send_buffer(write_buffer, seq_id, 'p');
    }

    void
    ServerAuthorization::_handle_auth_scram(BufferPtr buffer, uint64_t seq_id)
    {
        // get user login info
        if (_login == nullptr) {
            SPDLOG_ERROR("Failed to get user login info");
            throw ProxyAuthError();
        }

        // check that the server supports the SCRAM-SHA-256 mechanism
        bool found = false;
        do {
            std::string_view mechanism = buffer->get_string();
            if (mechanism == "SCRAM-SHA-256") {
                found = true;
            }
        } while (!found && buffer->remaining() > 0);

        if (!found) {
            SPDLOG_ERROR("No SASL mechanism found matching: SCRAM-SHA-256");
            throw ProxyAuthError();
        }

        char *client_first_message = build_client_first_message(&_login->scram_state);
        if (client_first_message == nullptr) {
            SPDLOG_ERROR("Failed to build client first message");
            throw ProxyAuthError();
        }

        int32_t len = strlen(client_first_message);
        BufferPtr write_buffer = BufferPool::get_instance()->get(4+14+4+1+len);
        write_buffer->put('p');
        write_buffer->put32(4+14+4+len); // length
        write_buffer->put_string("SCRAM-SHA-256");
        write_buffer->put32(len); // length of data
        write_buffer->put_bytes(client_first_message, len);

        free(client_first_message);

        _send_buffer(write_buffer, seq_id, 'p');
    }

    void
    ServerAuthorization::_handle_auth_scram_continue(BufferPtr buffer, uint64_t seq_id)
    {
        int data_len = buffer->remaining();
        std::string_view data = buffer->get_bytes(data_len);

        if (_login->scram_state.client_nonce == nullptr) {
            SPDLOG_ERROR("No client nonce set");
            throw ProxyAuthError();
        }

        if (_login->scram_state.server_first_message != nullptr) {
            SPDLOG_ERROR("Received second SCRAM-SHA-256 continue message");
            throw ProxyAuthError();
        }

        int salt_len;
        char *input = new char[data_len + 1];
        strncpy(input, data.data(), data_len);
        input[data_len] = '\0';

        if (!read_server_first_message(&_login->scram_state, input,
                                       &_login->scram_state.server_nonce,
                                       &_login->scram_state.salt,
                                       &salt_len,
                                       &_login->scram_state.iterations)) {
            SPDLOG_ERROR("Failed to read server first message");
            delete[] input;
            throw ProxyAuthError();
        }
        delete[] input;

        PgUser user;
        user.scram_ClientKey = _login->scram_state.ClientKey;
        user.has_scram_keys = true;

        char *client_final_message = build_client_final_message(&_login->scram_state,
			&user, _login->scram_state.server_nonce,
			_login->scram_state.salt, salt_len, _login->scram_state.iterations);

        if (client_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build client final message");
            throw ProxyAuthError();
        }

        int msg_len = 4 + strlen(client_final_message); // length

        BufferPtr write_buffer = BufferPool::get_instance()->get(1 + msg_len);
        write_buffer->put('p');
        write_buffer->put32(msg_len);
        write_buffer->put_bytes(client_final_message, msg_len - 4);

        free(client_final_message);

        _send_buffer(write_buffer, seq_id, 'p');
    }

    void
    ServerAuthorization::_handle_auth_scram_complete(BufferPtr buffer)
    {
        int data_len = buffer->remaining();
        std::string_view data = buffer->get_bytes(data_len);

        // make sure we are in right flow
        if (_login->scram_state.server_first_message == nullptr) {
            SPDLOG_ERROR("No server first message set");
            throw ProxyAuthError();
        }

        char *input = new char[data_len + 1];
        strncpy(input, data.data(), data_len);
        input[data_len] = '\0';
        char ServerSignature[SHA256_DIGEST_LENGTH];

        // decode the final message from server
        if (!read_server_final_message(input, ServerSignature)) {
            SPDLOG_ERROR("Failed to read server final message");
            delete[] input;
            throw ProxyAuthError();
        }
        delete[] input;

        PgUser user;
        user.scram_ClientKey = _login->scram_state.ClientKey;
        user.scram_ServerKey = _login->scram_state.ServerKey; // XXX need to get this from somewhere
        user.has_scram_keys = true;

        // last step, verify the server signature
        if (!verify_server_signature(&_login->scram_state, &user, ServerSignature)) {
            SPDLOG_ERROR("Failed to verify server signature");
            throw ProxyAuthError();
        }
    }

    void
    ServerAuthorization::_send_buffer(BufferPtr buffer, uint64_t seq_id, char code)
    {
        // send the buffer to the server
        ssize_t n = _connection->write(buffer->data(), buffer->size());
        CHECK_EQ(n, buffer->size());

        // log the buffer
        Session::log_buffer(_type, _id, false, code, buffer->size(), buffer->data(), seq_id);
    }

}