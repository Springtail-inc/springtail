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
#include <proxy/parser.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {
    ClientSession::ClientSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server)

        : Session(connection, server, CLIENT),
          _stmt_cache(STATEMENT_CACHE_SIZE)
    {
        SPDLOG_DEBUG("Client connected: endpoint={}, id={}", connection->endpoint(), _id);

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

        SPDLOG_DEBUG("Releasing server session: id={}", server_session->id());

        // clear associated session from the client session
        clear_associated_session();

        if (server_session->is_pinned()) {
            // server session is pinned, we can't release it
            return;
        }

        // release session back to instance pool
        server_session->get_instance()->release_session(server_session);
    }

    void
    ClientSession::_process_msg(SessionMsgPtr msg)
    {
        // client session is receiving a message from the server session
        // this indicates server is done with processing
        // in future this may not be true for all message types

        SPDLOG_DEBUG("Client session got message from server session: {:d}", (int8_t)msg->type());

        // entry point for messages from server session
        switch(msg->type()) {
            case SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE:
                SPDLOG_DEBUG("Client session got auth done from server session");
                if (_state == READY) {
                    // already ready, auth completed previously, this was new server auth completing
                    break;
                }
                assert(_state == AUTH);

                _send_auth_done();
                // the client session is established as is a server session.
                // the replica session will be created on-demand
                break;

            case SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR:
                throw ProxyServerError();

            case SessionMsg::MSG_SERVER_CLIENT_READY:
                SPDLOG_DEBUG("Client session got ready from server session");

                // check if we are in/still in a transaction
                if (msg->get_char() == 'I') {
                    _in_transaction = false;
                } else {
                    // either 'E' or 'T' -- error requiring rollback or in transaction.
                    // could track transaction error state and avoid server round trips
                    // until we get a rollback...
                    _in_transaction = true;
                }

                break;

            default:
                SPDLOG_WARN("Invalid message recevied by client session: {:d}", (int8_t)msg->type());
                break;
        }

        if (is_ready()) {
            enable_messages();
        }

        // release server session if not in a transaction
        if (!_in_transaction && is_msg_queue_empty()) {
            _release_server_session();
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
        char buffer[8];
        ssize_t n = _connection->read(buffer, 8);
        assert(n == 8);

        int32_t msg_length = recvint32(buffer)-4;
        int32_t code = recvint32(buffer+4);

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
                _process_startup_msg(code, msg_length-4);
                break;

            default:
                SPDLOG_ERROR("Invalid startup message code: {}", code);
                _state = ERROR;
                break;
        }
    }

    void
    ClientSession::_process_startup_msg(int32_t code, int32_t remaining)
    {
        SPDLOG_DEBUG("Proto version 3.0 requested");

        // read parameter strings
        std::string key;
        std::string value;
        std::string username;
        std::string database;

        // this shouldn't be too big
        char buffer[remaining];
        ssize_t n = _connection->read(buffer, remaining);
        assert(n == remaining);

        Buffer read_buffer(buffer, remaining, remaining);

        // seems to be a trailing null byte on the end
        while (read_buffer.remaining() > 1) {
            key = read_buffer.get_string();
            value = read_buffer.get_string();

            SPDLOG_DEBUG("Parameter: {}={}", key, value);

            if (key == "user") {
                username = value;
            } else if (key == "database") {
                database = value;
            }
        }
        // read last null byte
        char c = read_buffer.get();
        assert(c == '\0');

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
        char response;
        if (!_server->is_ssl_enabled()) {
            response = 'N';
            _connection->write(&response, 1);
            return;
        }

        // SSL is enabled, send 'S' and start handshake
        response = 'S';
        _connection->write(&response, 1);

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

        BufferPtr buffer = BufferPool::get_instance()->get(128);

        switch(_login->_type) {
            case TRUST:
                SPDLOG_DEBUG("User {} authenticated with trust", _user->username());
                _create_server_session(Session::Type::PRIMARY);
                return; // did send above so we return here

            case MD5:
                SPDLOG_DEBUG("User {} authenticating with md5", _user->username());
                _encode_auth_md5(buffer);
                break;

            case SCRAM:
                SPDLOG_DEBUG("User {} authenticating with scram", _user->username());
                _encode_auth_scram(buffer);
                break;

            default:
                SPDLOG_ERROR("User {} not found", _user->username());
                ProxyProtoError::encode_error(buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                throw ProxyAuthError();
        }

        // we've encoded the auth message above, now we send it, for AUTH_OK it is already sent
        ssize_t n = _connection->write(buffer->data(), buffer->size());
        assert(n == buffer->size());
    }

    void
    ClientSession::_handle_auth()
    {
        BufferList blist;

        // read chain of messages
        _read_msg(blist);

        // iterate through messages
        for (auto buffer: blist.buffers)
        {
            char code = buffer->get();
            assert(code == 'p');

            int32_t msg_length = buffer->get32() - 4; // subtract 4 for length field

            SPDLOG_DEBUG("Auth continue: msg_length={}", msg_length);

            switch(_login->_type) {
                case MD5: {
                    char md5[MD5_PASSWD_LEN + 1];

                    std::string_view client_passwd = buffer->get_string();
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

                    if (strcmp(md5, client_passwd.data()) != 0) {
                        SPDLOG_ERROR("MD5 password mismatch: : {} <> {}", md5, client_passwd);
                        char data[128];
                        BufferPtr write_buffer = std::make_shared<Buffer>(data, 128);
                        ProxyProtoError::encode_error(write_buffer, ProxyProtoError::INVALID_PASSWORD, "password authentication failed");
                        _connection->write(write_buffer->data(), write_buffer->size());
                        throw ProxyAuthError();
                    }

                    SPDLOG_DEBUG("MD5 password match");

                    // auth successful on client side
                    // see if we need to create a server session
                    _create_server_session(Session::Type::PRIMARY);

                    return;
                }

                case SCRAM: {
                    // see if this is the first or second message
                    if (_login->scram_state.server_nonce == nullptr) {
                        SPDLOG_DEBUG("Handling SCRAM SASL initial response");

                        // process as SASLInitialResponse
                        std::string_view scram_type = buffer->get_string();
                        if (scram_type != "SCRAM-SHA-256") {
                            SPDLOG_ERROR("Unsupported scram type: {}", scram_type);
                            throw ProxyAuthError();
                        }

                        int32_t len = buffer->get32();
                        std::string_view data = buffer->get_bytes(len);
                        _handle_scram_auth(data);
                    } else {
                        SPDLOG_DEBUG("Handling SCRAM SASL response");
                        // process as SASLResponse
                        std::string_view data = buffer->get_bytes(msg_length);
                        _handle_scram_auth_continue(data);
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
    ClientSession::_handle_scram_auth(const std::string_view data)
    {
        char *raw = ::strdup(data.data()); // copy to remove constness
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
        int msg_len = strlen(_login->scram_state.server_first_message) + 8;
        BufferPtr write_buffer = BufferPool::get_instance()->get(msg_len + 1);
        write_buffer->put('R');
        write_buffer->put32(msg_len);
        write_buffer->put32(11); // 11 == SASL continue
        write_buffer->put_bytes(_login->scram_state.server_first_message,
                                 strlen(_login->scram_state.server_first_message));

        free (raw);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());
    }

    void
    ClientSession::_handle_scram_auth_continue(const std::string_view data)
    {
        char *raw = ::strdup(data.data()); // copy to remove constness
        const char *client_final_nonce = nullptr;
	    char *proof = nullptr;

        // decode the final message from client
        if (!read_client_final_message(&_login->scram_state,
                                        reinterpret_cast<const uint8_t *>(data.data()),
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

        int msg_len = strlen(server_final_message)+8;
        BufferPtr write_buffer = BufferPool::get_instance()->get(msg_len+1);

        write_buffer->put('R');
        write_buffer->put32(msg_len);
        write_buffer->put32(12); // 12 == SASL final
        write_buffer->put_bytes(server_final_message, strlen(server_final_message));

        free (raw);
        free (server_final_message);
        free (proof);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());

        // auth successful on client side; see if a primary server side session exists
        if (_primary_session.expired()) {
            // create a new server session for primary
            _create_server_session(Session::Type::PRIMARY);
            // wait until this is done before auth done messages are sent
            // server session will notify client through _process_message
        } else {
            // auth is done, send auth done messages
            _send_auth_done();
        }
    }

    bool
    ClientSession::_primary_pool_exists()
    {
        DatabaseInstancePtr primary = _server->get_primary_instance();
        assert (primary != nullptr);
        DatabasePoolPtr pool = primary->get_pool(_database, _user->username());
        if (pool == nullptr || pool->total_count() == 0) {
            return false;
        }

        return true;
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
        assert(n == buffer->size());

        // free login info
        _login.reset();

        // set state to ready
        _state = READY;

        SPDLOG_DEBUG("Client session auth done, ready for queries");
    }

    void
    ClientSession::_encode_auth_ok(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(8);
        buffer->put32(0);
    }

    void
    ClientSession::_encode_auth_md5(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(12); // length
        buffer->put32(5);  // 5 == md5
        buffer->put_bytes(reinterpret_cast<char*>(&_login->_salt), 4);
    }

    void
    ClientSession::_encode_auth_scram(BufferPtr buffer)
    {
        buffer->put('R');
        buffer->put32(23); // length
        buffer->put32(10); // 10 == scram
        buffer->put_string("SCRAM-SHA-256");
        buffer->put(0);
    }

    void
    ClientSession::_encode_parameter_status(BufferPtr buffer, const std::string &key, const std::string &value)
    {
        buffer->put('S');
        buffer->put32(key.size() + value.size() + 6); // 4B len + 2B nulls
        buffer->put_string(key);
        buffer->put_string(value);
    }

    void
    ClientSession::_handle_request()
    {
        BufferList blist;
        _read_msg(blist);

        for (auto buffer: blist.buffers)
        {
            char code = buffer->get();
            buffer->get32(); // skip msg len

            // handle request
            switch (code) {
            case 'P':
                // parse - save query as prepared stmt
                _handle_parse(buffer);
                break;

            case 'B':
                // bind - binds a prepared statement to a portal
                _handle_bind(buffer);
                break;

            case 'D': {
                // describe - describe row format of result when executed
                _handle_describe(buffer);
                break;
            }

            case 'C': {
                // close portal or prepared stmt -- release it
                _handle_close(buffer);
                break;
            }

            case 'E': {
                // execute portal
                _handle_execute(buffer);
                break;
            }

            case 'Q':
                // query - handle simple query (semicolon separated)
                _handle_simple_query(buffer);
                break;

            case 'X': {
                // terminate
                SPDLOG_DEBUG("Terminate request");
                _state = ERROR;
                return;
            }
            default:
                SPDLOG_ERROR("Unsupported request code: {}", code);
                throw ProxyMessageError();
            }
        }
    }

    void
    ClientSession::_handle_parse(BufferPtr buffer)
    {
        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        // query string
        std::string_view query = buffer->get_string();

        SPDLOG_DEBUG("Parse: stmt={}, query={}", stmt, query);

        // parse the query and determine if it is a read or write query
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_PARSE, buffer);
        Session::Type type = _parse_query(query, msg);

        // cache the parse packet for the server session
        QueryStmtPtr query_stmt = _stmt_cache.add(QueryStmtCache::PREPARED,
                                                  stmt, buffer, type == REPLICA);

        // cache may decide that the statement is too long to cache, and make it not readonly
        if (!query_stmt->is_read_safe()) {
            type = PRIMARY;
        }

        // generate message with dependencies/provides
        msg->add_dependency(query_stmt);

        // select a server session and notify it of this message
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
    }

    void
    ClientSession::_handle_bind(BufferPtr buffer)
    {
        // portal string
        std::string_view portal = buffer->get_string();

        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        SPDLOG_DEBUG("Bind: prepared={}, portal={}", stmt, portal);

        // get the prepared statement from the cache
        QueryStmtPtr prepared_stmt = _stmt_cache.get(QueryStmtCache::PREPARED, stmt);
        if (prepared_stmt == nullptr) {
            SPDLOG_ERROR("Prepared statement not found: {}", stmt);
            throw ProxyMessagePreparedError();
        }

        // add the portal to the cache
        QueryStmtPtr portal_stmt = _stmt_cache.add(QueryStmtCache::PORTAL,
                                                   portal, buffer, prepared_stmt->is_read_safe());
        _portal_map.insert({portal.data(), stmt.data()});

        // get session type
        Session::Type type = portal_stmt->is_read_safe() ? REPLICA : PRIMARY;

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_BIND, buffer);
        msg->add_dependency(prepared_stmt);
        msg->add_provides(portal_stmt->get_hash());

        // select a server session and notify it of this message
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
    }

    void
    ClientSession::_handle_describe(BufferPtr buffer)
    {
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement name
        std::string_view name = buffer->get_string();

        SPDLOG_DEBUG("Describe request: type={}, name={}", stmt_type, name);

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_DESCRIBE, buffer);

        // get the statement from the cache
        QueryStmtPtr query_stmt = _stmt_cache.get(stmt_type, name.data());
        if (query_stmt == nullptr) {
            SPDLOG_ERROR("Statement not found: {}", name);
            throw ProxyMessagePreparedError();
        }

        // add dependency
        msg->add_dependency(query_stmt);

        Session::Type type = query_stmt->is_read_safe() ? REPLICA : PRIMARY;

        // send to server session
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
    }

    void
    ClientSession::_handle_execute(BufferPtr buffer)
    {
        // portal name
        std::string_view name = buffer->get_string();

        SPDLOG_DEBUG("Describe request: name={}", name);

        QueryStmtPtr query_stmt = _stmt_cache.get(QueryStmtCache::PORTAL, name);
        if (query_stmt == nullptr) {
            SPDLOG_ERROR("Portal not found: {}", name);
            throw ProxyMessagePreparedError();
        }

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_EXECUTE, buffer);
        msg->add_dependency(query_stmt);

        Session::Type type = query_stmt->is_read_safe() ? REPLICA : PRIMARY;

        // select a server session and notify it of this message
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
    }

    void
    ClientSession::_handle_close(BufferPtr buffer)
    {
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement
        std::string_view name = buffer->get_string();

        SPDLOG_DEBUG("Close request: type={}, name={}", stmt_type, name);

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_CLOSE, buffer);

        QueryStmtPtr query_stmt = _stmt_cache.get(stmt_type, name);
        msg->add_dependency(query_stmt);
        _stmt_cache.remove(stmt_type, name);

        Session::Type type = PRIMARY;
        if (query_stmt != nullptr) {
            type = query_stmt->is_read_safe() ? REPLICA : PRIMARY;
        }

        // notify server session
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
    }

    void
    ClientSession::_handle_simple_query(BufferPtr buffer)
    {
        std::string_view query = buffer->get_string();

        SPDLOG_DEBUG("Simple Query: {}", query);

        // create message for server for query
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY, buffer);

        // parse the query and determine if it is a read or write query
        Session::Type type = _parse_query(query, msg);

        // select a server session and notify it of this message
        // if no server is available a new server session will be connected
        // and this message will be delayed until after the session is ready
        ServerSessionPtr server_session = _select_session_and_notify(type, msg);
        assert (server_session != nullptr);
    }

    ServerSessionPtr
    ClientSession::_select_session_and_notify(Session::Type type,
                                              SessionMsgPtr msg)
    {
        SPDLOG_DEBUG("Selecting server session: type={}", (int8_t)type);

        // if we have an associated session use it (typically in a transaction)
        if (get_associated_session() != nullptr) {
            ServerSessionPtr session =  std::static_pointer_cast<ServerSession>(get_associated_session());
            queue_msg(msg, session);
            SPDLOG_DEBUG("Using associated session: id={}", session->id());
            return session;
        }

        ServerSessionPtr session = nullptr;

        if (type == PRIMARY && !_primary_session.expired()) {
            // use primary session
            session = _primary_session.lock();
            queue_msg(msg, session);
            return session;
        }

        if (type == REPLICA && !_replica_session.expired()) {
            // use replica session
            session = _replica_session.lock();
            queue_msg(msg, session);
            return session;
        }

        //// Shouldn't get here in common case; only if we need to allocate a new session
        session = _create_server_session(type);
        assert (session != nullptr);
        queue_msg(msg, session);

        return session;
    }

    ServerSessionPtr
    ClientSession::_create_server_session(Session::Type type)
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
            SPDLOG_DEBUG("Allocating new server session: {}:{}", _database, _user->username());
            session = instance->allocate_session(_server, _user, _database);
        }
        SPDLOG_DEBUG("Got server session: id={}, is_ready={}", session->id(), session->is_ready());

        if (type == PRIMARY) {
            // store reference to primary session
            _primary_session = session;
        } else {
            // store reference to replica session
            _replica_session = session;
        }
        session->pin_client_session(shared_from_this());


        if (session->is_ready()) {
            // session is ready, we can use it
            return session;
        }

        // we need to do authentication and wait for session to become ready
        // register server session connection with server
        _server->register_session(session);

        // queue client startup message, response comes in _process_msg()
        SessionMsgPtr startup_msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_STARTUP);
        queue_msg(startup_msg, session);

        // at this point we'll return through process()
        // most likely with _waiting_on_session set, in which case this
        // session will be removed from the server poll list

        return session;
    }


    Session::Type
    ClientSession::_parse_query(const std::string_view query,
                                SessionMsgPtr msg)
    {
        // parse the query and determine if it is a read or write query
        bool is_read_safe = true;
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query);

        for (auto &context : parse_contexts) {
            // check for PREPARE
            if (context->type == Parser::StmtContext::PREPARE_STMT) {
                // handle prepared statement, find substring query from location/length
                // cache may decide not to cache query if query is too long, if so it will
                // revert entry to not readonly (execute on Primary)
                auto p_query = query.substr(context->stmt_location, context->stmt_length);
                QueryStmtPtr qs = _stmt_cache.add(QueryStmtCache::PREPARED, context->name, p_query,
                                                  context->is_read_safe);
                msg->add_provides(context->name);
                if (!qs->is_read_safe()) {
                    context->is_read_safe = false;
                }
            }

            // check for EXECUTE
            if (context->type == Parser::StmtContext::EXECUTE_STMT) {
                // handle execute statement, find prepared statement in cache
                QueryStmtPtr qs = _stmt_cache.get(QueryStmtCache::PREPARED, context->name);
                if (qs == nullptr) {
                    SPDLOG_ERROR("Prepared statement not found: {}", context->name);
                    throw ProxyMessagePreparedError();
                }
                msg->add_dependency(qs);
                if (!qs->is_read_safe()) {
                    context->is_read_safe = false;
                }
            }

            // check for DECLARE
            if (context->type == Parser::StmtContext::DECLARE_STMT) {
                // handle declare statement insert into cache
                auto p_query = query.substr(context->stmt_location, context->stmt_length);
                QueryStmtPtr qs = _stmt_cache.add(QueryStmtCache::PORTAL,
                                                  context->name, p_query,
                                                  context->is_read_safe);
                msg->add_dependency(qs);
                if (!qs->is_read_safe()) {
                    context->is_read_safe = false;
                }
            }

/*
            // check for DISCARD
            if (context->type == Parser::StmtContext::DISCARD_STMT) {
                // handle discard statement, remove prepared statement from cache
                _stmt_cache.remove(QueryStmtCache::PREPARED, context->name);
            }

            if (context->type == Parser::StmtContext::DISCARD_ALL) {
                // handle discard all statement, remove all prepared statements from cache
                _stmt_cache.remove(QueryStmtCache::PREPARED);
            }

            // check for DEALLOCATE
            if (context->type == Parser::StmtContext::DEALLOCATE_STMT) {
                // handle deallocate statement, remove prepared statement from cache
                if (context->name.empty()) {
                    // deallocate all prepared statements
                    _stmt_cache.remove(QueryStmtCache::PREPARED);
                } else {
                    // deallocate specific prepared statement
                    _stmt_cache.remove(QueryStmtCache::PREPARED, context->name);
                }
            }

            // check for CLOSE
            if (context->type == Parser::StmtContext::CLOSE) {
                // handle close statement, remove portal from cache
                if (context->portal_name.empty()) {
                    // close all portals
                    _stmt_cache.remove(QueryStmtCache::PORTAL);
                } else {
                    // close specific portal
                    _stmt_cache.remove(QueryStmtCache::PORTAL, context->portal_name);
                }
            }
*/
            // set readonly flag
            if (!context->is_read_safe) {
                is_read_safe = false;
            }
        }

        return (is_read_safe) ? REPLICA : PRIMARY;
    }

}