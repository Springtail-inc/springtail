#include <iostream>
#include <sstream>
#include <cassert>

#include <openssl/err.h>

#include <common/logging.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/server_session.hh>
#include <proxy/client_session.hh>
#include <proxy/database.hh>
#include <proxy/user_mgr.hh>
#include <proxy/errors.hh>
#include <proxy/server.hh>
#include <proxy/exception.hh>
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
        assert(0);
    }

    void
    ClientSession::_release_server_session()
    {
        ServerSessionPtr server_session = std::static_pointer_cast<ServerSession>(get_associated_session());
        assert(server_session != nullptr);

        // clear associated session from the client session
        clear_associated_session();

        // release session back to instance pool
        server_session->get_instance()->release_session(server_session);
    }

    void
    ClientSession::_process_msg(SessionMsgPtr msg)
    {
        // client session is receiving a message from the server session
        // this indicates server is done with processing
        // in future this may not be true for all message types

        // get any pending messages for the server, and clear them
        SessionMsgPtr pending_msg = get_pending_msg();
        if (pending_msg == nullptr) {
            _release_server_session();
        }

        // entry point for messages from server session
        switch(msg->type) {
            case SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE:
                SPDLOG_DEBUG("Client session got auth done from server session");
                if (_state == READY) {
                    // already ready, auth completed previously, this was new server auth completing
                    break;
                }

                assert(_state == AUTH);
                _send_auth_done();

                SPDLOG_DEBUG("Authentication complete, setting READY state");
                _state = READY;

                // the client session is established as is a server session.
                // the replica session will be created on-demand
                break;

            case SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR:
                _state = ERROR;
                break;

            case SessionMsg::MSG_SERVER_CLIENT_READY:
                SPDLOG_DEBUG("Client session got ready from server session");
                break;

            default:
                SPDLOG_WARN("Invalid message recevied by client session: {:d}", (int8_t)msg->type);
                break;
        }

        // notify server of pending message if any
        if (pending_msg != nullptr) {
            assert(get_associated_session() != nullptr);
            notify_server(pending_msg, std::static_pointer_cast<ServerSession>(get_associated_session()));
        }
    }

    void
    ClientSession::_process_connection()
    {
        // entry point for network connection message
        SPDLOG_DEBUG("Processing client session: state={:d}", (int8_t)_state);

        // main entry point for thread processing
        // resume from where we left off
        switch(_state) {
            case STARTUP:
                // startup messages, no auth done yet
                _handle_startup();
                break;
            case SSL_HANDSHAKE:
                // ssl handshake in progress
                _handle_ssl_handshake();
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
    ClientSession::_handle_ssl_handshake()
    {
        // try the SSL accept
        // this will return 1 on success, -1 if more data is needed; throws exception on fatal error
        int rc = _connection->SSL_accept();
        if (rc < 0) {
            SPDLOG_DEBUG("SSL client handshake in progress, need more data");
            return;
        }

        SPDLOG_DEBUG("SSL client handshake complete");

        _state = STARTUP;
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
                _process_ssl_request();
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
        // read last null byte
        char c = _read_buffer.get();
        assert(c == '\0');

        _read_buffer.reset();

        // get user info and store it
        _user = _server->get_user_mgr()->get_user(username);
        if (_user == nullptr) {
            SPDLOG_ERROR("User {} not found", username);
            _state = ERROR;
            return;
        }
        _database = database;

        // get login info for the user
        _login = _user->get_user_login();

        // handle authentication -- send auth request
        _send_auth_req();
    }

    void
    ClientSession::_process_ssl_request()
    {
        // send response 'S' or 'N' for SSL or no SSL respectively
        _write_buffer.reset();
        if (!_server->is_ssl_enabled()) {
            _write_buffer.put('N');
            _connection->write(_write_buffer.data(), 1);
            return;
        }

        // SSL is enabled, send 'S' and start handshake
        _write_buffer.put('S');
        _connection->write(_write_buffer.data(), 1);

        // allocate ssl struct for this connection; acting as server
        SSL *ssl = _server->SSL_new(true);
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
    ClientSession::_send_auth_req()
    {
        _state = AUTH;

        _write_buffer.reset();

        switch(_login->_type) {
            case TRUST:
                SPDLOG_DEBUG("User {} authenticated with trust", _user->username());
                _create_primary_server_session();
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
                ProxyProtoError::encode_error(_write_buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                throw ProxyAuthError();
        }

        // we've encoded the auth message above, now we send it, for AUTH_OK it is already sent
        ssize_t n = _connection->write(_write_buffer.data(), _write_buffer.size());
        assert(n == _write_buffer.size());
    }

    void
    ClientSession::_handle_auth()
    {
        auto [code, msg_length] = _read_msg();
        assert(code == 'p');

        SPDLOG_DEBUG("Auth continue: msg_length={}", msg_length);

        switch(_login->_type) {
            case MD5: {
                char md5[MD5_PASSWD_LEN + 1];

                std::string client_passwd = _read_buffer.getString();
                if (client_passwd.empty() || client_passwd.size() != MD5_PASSWD_LEN) {
                    SPDLOG_ERROR("Empty password received; or password length mismatch");
                    throw ProxyAuthError();
                }

                // calculate md5 hash; skip the 'md5' prefix on the password
                if (!pg_md5_encrypt(_login->_password.c_str()+3, reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
                    SPDLOG_ERROR("Failed to calculate MD5 hash");
                    throw ProxyAuthError();
                }
                md5[MD5_PASSWD_LEN] = '\0';

                if (strcmp(md5, client_passwd.c_str()) != 0) {
                    SPDLOG_ERROR("MD5 password mismatch: : {} <> {}", md5, client_passwd);
                    ProxyProtoError::encode_error(_write_buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                    _connection->write(_write_buffer.data(), _write_buffer.size());
                    throw ProxyAuthError();
                }

                SPDLOG_DEBUG("MD5 password match");

                // auth successful on client side
                // see if we need to create a server session
                _create_primary_server_session();

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
                        throw ProxyAuthError();
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
                throw ProxyAuthError();
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
            free (raw);
            throw ProxyAuthError();
        }

        // note: some code inside of here could be optimized based on how the password is stored
        if (!build_server_first_message(&_login->scram_state, _user->username().c_str(), _login->_password.c_str())) {
            SPDLOG_ERROR("Failed to build server first message");
            free (raw);
            throw ProxyAuthError();
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
            free (raw);
            throw ProxyAuthError();
        }

        // verify the nonce and the proof from client
        if (!verify_final_nonce(&_login->scram_state, client_final_nonce) ||
            !verify_client_proof(&_login->scram_state, proof)) {
		    SPDLOG_ERROR("Invalid SCRAM response (nonce or proof does not match)");
            free (raw);
            free (proof);

            throw ProxyAuthError();
	    }

        // after verifying the client proof, we now have the client key
        _user->set_client_scram_key(_login->scram_state.ClientKey);

        // finally send the final message to the client
        char *server_final_message = build_server_final_message(&_login->scram_state);
        if (server_final_message == nullptr) {
            SPDLOG_ERROR("Failed to build server final message");
            free (raw);
            free (proof);

            throw ProxyAuthError();
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

        _create_primary_server_session();
    }

    void
    ClientSession::_create_primary_server_session()
    {
        // see if any primary session pool exists for this user/database combo
        // if not, create a new one
        DatabaseInstancePtr primary = _server->get_primary_instance();
        assert (primary != nullptr);
        if (primary->get_pool(_database, _user->username()) != nullptr) {
            return;
        }

        // create a new server session; will initiate the connection to the server
        ServerSessionPtr server_session = ServerSession::create(_server, _user, _database, primary, REPLICA);
        if (server_session == nullptr) {
            SPDLOG_ERROR("Failed to create server session");
            _state = ERROR;
            return;
        }

        // register server session connection with server
        _server->register_session(server_session);

        // notify server of client message, response comes in _process_msg()
        SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_CLIENT_SERVER_STARTUP);
        notify_server(msg, server_session);

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
        do {
            auto [code, msg_length] = _read_msg();
            if (_state == ERROR) {
                return;
            }

            // handle request
            switch (code) {
            case 'Q': {
                // query
                std::string query = _read_buffer.getString();

                // handle simple query
                _handle_simple_query(query);
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
        } while (_read_buffer.remaining() > 0 && _state != ERROR);
    }

    void
    ClientSession::_handle_simple_query(const std::string &query)
    {
        SPDLOG_DEBUG("Simple Query: {}", query);

        // get a server session from the pool, there should be one (for now at least)
        // Type session_type = select_session_type(query);
        SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY, query);

        ServerSessionPtr server_session = _select_session_and_notify(PRIMARY, msg);
        assert (server_session != nullptr);

        // notify server of client message, response comes in _process_msg()

        notify_server(msg, server_session);
    }

    ServerSessionPtr
    ClientSession::_select_session_and_notify(Session::Type type,
                                              SessionMsgPtr pending_msg)
    {
        // get database instance from either primary or replica set
        DatabaseInstancePtr instance = nullptr;
        if (type == PRIMARY) {
            // get a primary session
            instance = _server->get_primary_instance();
        } else {
            // get a replica session
            instance = _server->get_replica_instance(_database, _user->username());
        }
        assert (instance != nullptr);

        // get a session from the instance
        ServerSessionPtr session = instance->get_session(_database, _user->username());
        if (session == nullptr) {
            // need to allocate a new session
            session = instance->allocate_session(_server, _user, _database, type);
        }

        if (session->is_ready()) {
            // session is ready, we can use it
            if (pending_msg != nullptr) {
                notify_server(pending_msg, session);
            }
            return session;
        }

        // session is not ready, we need to wait for it
        if (pending_msg != nullptr) {
            // set the pending message on the session, this message will be sent after session is ready
            set_pending_msg(pending_msg);
        }

        // we need to do authentication and wait for session to become ready
        // register server session connection with server
        _server->register_session(session);

        // notify server of client message, response comes in _process_msg()
        SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_CLIENT_SERVER_STARTUP);
        notify_server(msg, session);

        // at this point we'll return through process()
        // most likely with _waiting_on_session set, in which case this
        // session will be removed from the server poll list

        return session;
    }

}