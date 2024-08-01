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
#include <proxy/buffer_pool.hh>
#include <proxy/logging.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail::pg_proxy {

    ClientSession::ClientSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server,
                                 bool shadow_mode)

        : Session(connection, server, CLIENT),
          _stmt_cache(STATEMENT_CACHE_SIZE),
          _shadow_mode(shadow_mode)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client connected: endpoint={}", _id, connection->endpoint());

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

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Releasing server session: id={}", _id, server_session->id());

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

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session got message from server session: {}", _id, msg->type_str());

        // entry point for messages from server session
        switch(msg->type()) {
            case SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session got auth done from server session", _id);
                if (_state == READY) {
                    // already ready, auth completed previously, this was new server auth completing
                    break;
                }
                assert(_state == AUTH);

                _send_auth_done(msg->seq_id());

                // at this point, the client session is established as is a server session.
                // the replica session will be created on-demand

                // if in shadow mode, then create the replica session now
                if (_shadow_mode && _replica_session.expired()) {
                    uint64_t seq_id = _gen_seq_id();
                    PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Creating replica session in shadow mode: seq_id={}", _id, seq_id);
                    _create_server_session(Session::Type::REPLICA, seq_id);
                }

                break;

            case SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session got fatal error from server session", _id);
                throw ProxyServerError();

            case SessionMsg::MSG_SERVER_CLIENT_MSG_SUCCESS:
                // message complete
                _stmt_cache.commit_statement(msg->data(), msg->completed());
                break;

            case SessionMsg::MSG_SERVER_CLIENT_MSG_ERROR:
                // message error
                _stmt_cache.commit_statement(msg->data(), msg->completed());
                break;

            case SessionMsg::MSG_SERVER_CLIENT_READY: {
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session got ready from server session: status={}",
                            _id, msg->status().transaction_status);

                // check if we are in/still in a transaction
                SessionMsg::MsgStatus status = msg->status();
                if (status.transaction_status == 'I') {
                    _in_transaction = false;
                } else {
                    assert(status.transaction_status == 'E' || status.transaction_status == 'T');

                    // either 'E' or 'T' -- error requiring rollback or in transaction.
                    // could track transaction error state and avoid server round trips
                    // until we get a rollback...
                    _in_transaction = true;

                    // no longer waiting on associated session, but
                    // we still want to keep the same session until the transaction
                    // is complete
                    if (is_msg_queue_empty()) {
                        set_waiting_on_session(false);
                    }
                }

                _stmt_cache.sync_transaction(status.transaction_status);

                break;
            }

            default:
                SPDLOG_WARN("Invalid message recevied by client session: {}", msg->type_str());
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
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Processing packet, client session: state={:d}", _id, (int8_t)_state);

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
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] SSL client handshake in progress, need more data", _id);
            return;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] SSL client handshake complete", _id);

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

        uint64_t seq_id = _gen_seq_id();

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
    ClientSession::_process_startup_msg(int32_t remaining, uint64_t seq_id)
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
        ssize_t n = _connection->read(buffer, remaining);
        assert(n == remaining);

        Buffer read_buffer(buffer, remaining, remaining);
        _log_buffer(true, '?', remaining, buffer, seq_id);

        // seems to be a trailing null byte on the end
        while (read_buffer.remaining() > 1) {
            key = read_buffer.get_string();
            value = read_buffer.get_string();

            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Parameter: {}={}", _id, key, value);

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
        _send_auth_req(seq_id);
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
    ClientSession::_send_auth_req(uint64_t seq_id)
    {
        _state = AUTH;

        BufferPtr buffer = BufferPool::get_instance()->get(128);

        switch(_login->_type) {
            case TRUST:
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] User {} authenticated with trust", _id, _user->username());
                _create_server_session(Session::Type::PRIMARY, seq_id);
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
        assert(n == buffer->size());

        // log the buffer
        _log_buffer(false, '\0', buffer->size(), buffer->data(), seq_id);
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

            uint64_t seq_id = _gen_seq_id();

            // log buffer
            _log_buffer(true, code, msg_length, buffer->data() + 5, seq_id);

            PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Auth continue: msg_length={}, seq_id={}", _id, msg_length, seq_id);

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

                        // log buffer
                        _log_buffer(false, '\0', write_buffer->size(), write_buffer->data(), seq_id);

                        throw ProxyAuthError();
                    }

                    PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] MD5 password match", _id);

                    // auth successful on client side
                    // see if we need to create a server session
                    _create_server_session(Session::Type::PRIMARY, seq_id);

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
    ClientSession::_handle_scram_auth(const std::string_view data, uint64_t seq_id)
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

        // log buffer
        _log_buffer(false, '\0', write_buffer->size(), write_buffer->data(), seq_id);
    }

    void
    ClientSession::_handle_scram_auth_continue(const std::string_view data, uint64_t seq_id)
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

        // log buffer
        _log_buffer(false, '\0', write_buffer->size(), write_buffer->data(), seq_id);

        // auth successful on client side; see if a primary server side session exists
        if (_primary_session.expired()) {
            // create a new server session for primary
            _create_server_session(Session::Type::PRIMARY, seq_id);
            // wait until this is done before auth done messages are sent
            // server session will notify client through _process_message
        } else {
            // auth is done, send auth done messages
            _send_auth_done(seq_id);
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
    ClientSession::_send_auth_done(uint64_t seq_id)
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

        // log buffer
        _log_buffer(false, '\0', buffer->size(), buffer->data(), seq_id);

        // free login info
        _login.reset();

        // set state to ready
        _state = READY;

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session auth done, ready for queries", _id);
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

        for (auto buffer: blist.buffers) {
            char code = buffer->get();
            int32_t len = buffer->get32();
            uint64_t seq_id = _gen_seq_id();

            PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client got request code: {}, seq_id: {}", _id, code, seq_id);

            _log_buffer(true, code, len, buffer->data(), seq_id);

            // handle request
            switch (code) {
            case 'P':
                // parse - save query as prepared stmt
                _handle_parse(buffer, seq_id);
                break;

            case 'B':
                // bind - binds a prepared statement to a portal
                _handle_bind(buffer, seq_id);
                break;

            case 'D': {
                // describe - describe row format of result when executed
                _handle_describe(buffer, seq_id);
                break;
            }

            case 'C': {
                // close portal or prepared stmt -- release it
                _handle_close(buffer, seq_id);
                break;
            }

            case 'E': {
                // execute portal
                _handle_execute(buffer, seq_id);
                break;
            }

            case 'Q':
                // query - handle simple query (semicolon separated)
                _handle_simple_query(buffer, seq_id);
                break;

            case 'X':
                // terminate
                SPDLOG_ERROR("Terminate request");
                _state = ERROR;
                return;

            case 'F': // function call
                _handle_function_call(buffer, seq_id);
                break;

            case 'S': // sync
                _handle_sync(buffer, seq_id);
                break;

            case 'H': // flush
            case 'f': // copy fail
            case 'c': // copy done
            case 'd': // copy data
                // forward to server, should have associated server session
                _forward_to_server(buffer, seq_id);
                break;

            default:
                SPDLOG_ERROR("Unsupported request code: {}", code);
                throw ProxyMessageError();
            }
        }
    }

    void
    ClientSession::_forward_to_server(BufferPtr buffer, uint64_t seq_id)
    {
        // forward the message to the server session
        if (get_associated_session() == nullptr) {
            SPDLOG_ERROR("No associated server session");
            assert(0); // doesn't make sense to forward without a server session
        }

        // create a message and queue it
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_FORWARD, buffer, seq_id);
        _send_msg(msg, true);
    }

    void
    ClientSession::_handle_function_call(BufferPtr buffer, uint64_t seq_id)
    {
        // doc's state that this should really be deprecated and not used
        // instead clients should use a prepared statement

        // send to primary server session
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_FORWARD, buffer, seq_id);
        _send_msg(msg, false);
    }

    void
    ClientSession::_handle_parse(BufferPtr buffer, uint64_t seq_id)
    {
        // PARSE packet request, create prepared statement
        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        // query string
        std::string_view query = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Parse: stmt={}, query={}", _id, stmt, query);

        // parse the query
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query);

        // Create a query statement object
        QueryStmt::Type qs_type = _remap_parse_type(parse_contexts[0]);
        bool is_read_safe = parse_contexts[0]->is_read_safe;
        QueryStmtPtr query_stmt = std::make_shared<QueryStmt>(QueryStmt::Type::PREPARE, buffer, is_read_safe, stmt.data());
        query_stmt->extended_type = qs_type;

        // cache the parse packet for the server session
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }
        _stmt_cache.add(query_stmt);

        // create the server message
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_PARSE, query_stmt, seq_id);

        // select a server session and queue message
        _send_msg(msg, is_read_safe);
    }

    void
    ClientSession::_handle_bind(BufferPtr buffer, uint64_t seq_id)
    {
        // BIND packet request, bind a prepared statement to a portal
        // portal string
        std::string_view portal = buffer->get_string();

        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Bind: prepared={}, portal={}", _id, stmt, portal);

        // get the prepared statement from the cache
        std::pair<QueryStmtPtr, bool> lookup_result = _stmt_cache.lookup_prepared(stmt);
        QueryStmtPtr prepared_stmt = lookup_result.first;
        if (prepared_stmt == nullptr) {
            SPDLOG_ERROR("Prepared statement not found: {}", stmt);
            throw ProxyMessagePreparedError();
        }

        // cache the bind packet for the server session
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }
        QueryStmtPtr qs = _stmt_cache.add(QueryStmt::DECLARE, buffer, prepared_stmt->is_read_safe, portal.data());
        qs->dependency = prepared_stmt;

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_BIND, qs, seq_id);
        if (!lookup_result.second) {
            // add dependency if not in current transaction
            msg->add_dependency(prepared_stmt);
        }

        // queue message to server session
        _send_msg(msg, prepared_stmt->is_read_safe);
    }

    void
    ClientSession::_handle_describe(BufferPtr buffer, uint64_t seq_id)
    {
        // DESCRIBE packet request, get row details for a prepared statement or portal
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement name
        std::string_view name = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Describe request: type={}, name={}", _id, stmt_type, name);

        // get the statement from the cache
        std::pair<QueryStmtPtr, bool> lookup_result;
        if (stmt_type == 'S') {
            lookup_result = _stmt_cache.lookup_prepared(name);
        } else {
            // get the statement from the cache
            lookup_result = _stmt_cache.lookup_portal(name);
        }

        QueryStmtPtr query_stmt = lookup_result.first;
        if (query_stmt == nullptr) {
            SPDLOG_ERROR("Statement not found: {}", name);
            throw ProxyMessagePreparedError();
        }

        // cache the describe packet for the transaction
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }

        QueryStmtPtr qs = _stmt_cache.add(QueryStmt::DESCRIBE, buffer, query_stmt->is_read_safe);
        qs->dependency = query_stmt;

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_DESCRIBE, qs, seq_id);
        if (!lookup_result.second) {
            // add dependency if not in current transaction
            msg->add_dependency(query_stmt);
        }

        // queue message to server session
        _send_msg(msg, query_stmt->is_read_safe);
    }

    void
    ClientSession::_handle_execute(BufferPtr buffer, uint64_t seq_id)
    {
        // EXECUTE packet request, execute portal
        // portal name
        std::string_view name = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Execute request: name={}", _id, name);

        // find the dependency
        QueryStmt::Type qs_type = QueryStmt::ANONYMOUS;

        std::pair<QueryStmtPtr, bool> lookup_result = _stmt_cache.lookup_portal(name);
        QueryStmtPtr query_stmt = lookup_result.first;
        if (query_stmt != nullptr) {
            // found the portal statement, trace it back looking
            // for a prepare (PARSE) statement to determine the
            // real type of the query
            QueryStmtPtr dep_stmt = query_stmt;
            while (dep_stmt->dependency != nullptr) {
                dep_stmt = dep_stmt->dependency;
            }
            if (dep_stmt != query_stmt &&
                dep_stmt->type == QueryStmt::PREPARE &&
                dep_stmt->extended_type != QueryStmt::NONE) {
                qs_type = dep_stmt->extended_type;
            }
        } else {
            SPDLOG_ERROR("Portal not found: {}", name);
        }

        // cache the execute packet for the transaction
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }

        QueryStmtPtr qs = _stmt_cache.add(qs_type, buffer, query_stmt->is_read_safe);

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_EXECUTE, qs, seq_id);

        // select a server session and notify it of this message
        _send_msg(msg, query_stmt->is_read_safe);
    }

    void
    ClientSession::_handle_close(BufferPtr buffer, uint64_t seq_id)
    {
        // CLOSE packet, close prepared statement or portal
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement
        std::string_view name = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Close request: type={}, name={}", _id, stmt_type, name);

        // cache the close packet for the transaction
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }

        QueryStmtPtr dep_stmt = nullptr;
        QueryStmtPtr qs;

        if (stmt_type == 'S') {
            std::tie(dep_stmt, std::ignore) = _stmt_cache.lookup_prepared(name);

            if (dep_stmt == nullptr) {
                SPDLOG_ERROR("Statement not found: {}", name);
                throw ProxyMessagePreparedError();
            }

            qs = _stmt_cache.add(QueryStmt::DEALLOCATE, buffer, dep_stmt->is_read_safe, name.data());
        } else {
            std::tie(dep_stmt, std::ignore) = _stmt_cache.lookup_portal(name);
            qs = _stmt_cache.add(QueryStmt::CLOSE, buffer, dep_stmt->is_read_safe, name.data());
        }

        // create message with dependencies/provides
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_CLOSE, qs, seq_id);
        if (dep_stmt != nullptr) {
            msg->add_dependency(dep_stmt);
        }

        // notify server session
        _send_msg(msg, qs->is_read_safe);
    }

    void
    ClientSession::_handle_sync(BufferPtr buffer, uint64_t seq_id)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Sync request", _id);
        if (get_associated_session() == nullptr) {
            // this is a weird case as it doesn't make sense to issue
            // a sync without a set of other extended queries preceeding it
            // but we'll handle it anyway, just issue a sync to the server
            _select_session(REPLICA, seq_id);
        }

        QueryStmtPtr qs = std::make_shared<QueryStmt>(QueryStmt::SYNC, buffer,
            associated_session_type() == REPLICA);
        queue_msg(SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SYNC, qs, seq_id));
    }

    void
    ClientSession::_handle_simple_query(BufferPtr buffer, uint64_t seq_id)
    {
        std::string_view query = buffer->get_string();

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Simple Query: {}", _id, query);

        // parse the query and determine if it is a read or write query
        if (!_in_transaction) {
            // not in a transaction, clear the cache
            _stmt_cache.clear_statement();
            _in_transaction = true; // implicit transaction
        }

        // select a server session and notify it of this message
        // if no server is available a new server session will be connected
        // and this message will be delayed until after the session is ready
        std::vector<QueryStmtPtr> dependencies;
        QueryStmtPtr qs = _parse_simple_query(buffer, query, dependencies);

        // create message for server for query
        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY, qs, seq_id);
        msg->set_dependencies(std::move(dependencies));

        // select session and queue msg
        _send_msg(msg, qs->is_read_safe);
    }

    void
    ClientSession::_send_msg(SessionMsgPtr msg, bool is_readonly, SessionPtr session)
    {
        // explicit session send, not usual
        if (session) {
            queue_msg(msg, session);
            return;
        }

        // not in shadow mode or not readonly, send to single server
        if (!_shadow_mode || !is_readonly) {
            // select a server session and notify it of this message
            session = _select_session(is_readonly ? REPLICA : PRIMARY, msg->seq_id());
            queue_msg(msg);
            return;
        }

        // both shadow mode and readonly; we send to both primary and replica
        assert(_shadow_mode && is_readonly);

        // make sure to send to primary first; so get PRIMARY session
        session = _select_session(PRIMARY, msg->seq_id());
        queue_msg(msg, session);

        // then clone msg and get a replica session
        // don't use _select_session() as it uses/sets the associated session
        msg = msg->clone();
        session = nullptr;
        if (!_replica_session.expired()) {
            // have a replica session use it
            session = _replica_session.lock();
        } else {
            // create a new replica session; shouldn't be common to get here
            session = _create_server_session(REPLICA, msg->seq_id());
        }

        assert (session != nullptr);
        // make sure queue_msg doesn't set associated session
        assert (get_associated_session() != nullptr);
        queue_msg(msg, session);

        return;
    }

    ServerSessionPtr
    ClientSession::_select_session(Session::Type type, uint64_t seq_id)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Selecting server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");

        // if we have an associated session use it (typically in a transaction)
        if (get_associated_session() != nullptr) {
            ServerSessionPtr session =  std::static_pointer_cast<ServerSession>(get_associated_session());
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Using associated session: id={}", _id, session->id());
            return session;
        }

        ServerSessionPtr session = nullptr;

        if (type == PRIMARY && !_primary_session.expired()) {
            // use primary session
            session = _primary_session.lock();
            set_associated_session(session);
            return session;
        }

        if (type == REPLICA && !_replica_session.expired()) {
            // use replica session
            session = _replica_session.lock();
            assert (!_shadow_mode);
            set_associated_session(session);
            return session;
        }

        //// Shouldn't get here in common case; only if we need to allocate a new session
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Creating new server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");
        session = _create_server_session(type, seq_id);
        assert (session != nullptr);
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Created new server session: id={}", _id, session->id());

        // set associated session
        assert(!_shadow_mode || type == PRIMARY);
        set_associated_session(session);

        return session;
    }

    ServerSessionPtr
    ClientSession::_create_server_session(Session::Type type, uint64_t seq_id)
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
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Allocating new server session: {}:{}", _id, _database, _user->username());
            session = instance->allocate_session(_server, _user, _database);
        }
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Got server session: id={}, is_ready={}", _id, session->id(), session->is_ready());

        if (type == PRIMARY) {
            // store reference to primary session
            _primary_session = session;
        } else {
            // store reference to replica session
            _replica_session = session;

            // if client is in shadow mode, then the replica session
            // becomes a shadow session, not returning results to the client
            if (_shadow_mode) {
                // set shadow mode on the session
                session->set_shadow_mode(true);
            }
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
        SessionMsgPtr startup_msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_STARTUP, seq_id);
        queue_msg(startup_msg, session);

        // at this point we'll return through process()
        // most likely with _waiting_on_session set, in which case this
        // session will be removed from the server poll list

        return session;
    }

    QueryStmt::Type
    ClientSession::_remap_parse_type(const Parser::StmtContextPtr context) const
    {
        switch(context->type) {
            // statements explicitly tracked in session history
            case Parser::StmtContext::Type::PREPARE_STMT:
                return QueryStmt::PREPARE;

            case Parser::StmtContext::Type::DECLARE_STMT:
                if (context->has_declare_hold) {
                    return QueryStmt::DECLARE_HOLD;
                } else {
                    return QueryStmt::DECLARE;
                }

            case Parser::StmtContext::Type::DISCARD_ALL_STMT:
                return QueryStmt::DISCARD_ALL;

            case Parser::StmtContext::Type::DISCARD_STMT:
                return QueryStmt::DISCARD;

            case Parser::StmtContext::Type::VAR_SET_STMT:
                if (context->has_is_local) {
                    return QueryStmt::SET_LOCAL;
                } else {
                    return QueryStmt::SET;
                }

            case Parser::StmtContext::Type::VAR_RESET_STMT:
                return QueryStmt::RESET;

            case Parser::StmtContext::Type::FETCH_STMT:
                return QueryStmt::FETCH;

            case Parser::StmtContext::Type::LISTEN_STMT:
                return QueryStmt::LISTEN;

            case Parser::StmtContext::Type::UNLISTEN_STMT:
                return QueryStmt::UNLISTEN;

            case Parser::StmtContext::SAVEPOINT_STMT:
                return QueryStmt::SAVEPOINT;

            case Parser::StmtContext::ROLLBACK_TO_SAVEPOINT_STMT:
                return QueryStmt::ROLLBACK_TO_SAVEPOINT;

            case Parser::StmtContext::RELEASE_SAVEPOINT_STMT:
                return QueryStmt::RELEASE_SAVEPOINT;

            case Parser::StmtContext::Type::TRANSACTION_BEGIN_STMT:
                return QueryStmt::BEGIN;

            case Parser::StmtContext::Type::TRANSACTION_COMMIT_STMT:
                return QueryStmt::COMMIT;

            case Parser::StmtContext::Type::TRANSACTION_ROLLBACK_STMT:
                return QueryStmt::ROLLBACK;

            // statements with dependencies
            case Parser::StmtContext::Type::CLOSE_STMT:
                if (context->name.empty()) {
                    return QueryStmt::CLOSE_ALL;
                } else {
                    return QueryStmt::CLOSE;
                }

            case Parser::StmtContext::Type::DEALLOCATE_STMT:
                if (context->name.empty()) {
                    // deallocate all prepared statements
                    return QueryStmt::DEALLOCATE_ALL;
                } else {
                    return QueryStmt::DEALLOCATE;
                }

            case Parser::StmtContext::Type::EXECUTE_STMT:
                return QueryStmt::EXECUTE;

            // those that have no affect on session history and no dependencies
            default:
                return QueryStmt::ANONYMOUS;
        }
    }

    QueryStmtPtr
    ClientSession::_parse_simple_query(const BufferPtr buffer,
                                       const std::string_view query,
                                       std::vector<QueryStmtPtr> &dependencies)
    {
        // create query statement for simple query (parent)
        QueryStmtPtr qs = std::make_shared<QueryStmt>(QueryStmt::Type::SIMPLE_QUERY, buffer, false);

        // parse the query and determine if it is a read or write query
        bool is_read_safe = true;
        // first parse the query to determine the type of statement(s)
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query);

        // iterate through the parse contexts (one per query within multi-statement block)
        for (auto &context : parse_contexts) {
            QueryStmt::Type stmt_type = _remap_parse_type(context);
            std::pair<QueryStmtPtr, bool> lookup_result = {nullptr, false};

            switch(stmt_type) {
                    case QueryStmt::DEALLOCATE:
                        // deallocate specific prepared statement
                        // XXX optimize this in future, since it is silly to execute
                        // a prepared statement to deallocate it, but deallocate will
                        // fail if the prepared statement is not found
                        lookup_result = _stmt_cache.lookup_prepared(context->name);
                    break;

                case QueryStmt::EXECUTE:
                    lookup_result = _stmt_cache.lookup_prepared(context->name);
                    break;

                // those that have no affect on session history and no dependencies
                default:
                    stmt_type = QueryStmt::ANONYMOUS;
                    break;
            }


            auto p_query = query.substr(context->stmt_location, context->stmt_length);
            QueryStmtPtr stmt = std::make_shared<QueryStmt>(stmt_type, p_query.data(), context->is_read_safe, context->name.data());

            // add to parent
            qs->children.push_back(stmt);

            // if there is a dependency add it; only prepared stmts; only add if not in current transaction
            if (lookup_result.first != nullptr) {
                if (lookup_result.second == false) {
                    dependencies.push_back(lookup_result.first);
                }
                // if dependency is not read safe, then this query is not read safe
                if (!lookup_result.first->is_read_safe) {
                    is_read_safe = false;
                }
            }

            // set readonly flag
            if (!context->is_read_safe) {
                is_read_safe = false;
            }
        }

        qs->is_read_safe = is_read_safe;
        return qs;
    }

} // namespace springtail::pg_proxy
