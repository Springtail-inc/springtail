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

namespace springtail::pg_proxy {

    ServerSession::ServerSession(ProxyConnectionPtr connection,
                                 ProxyServerPtr server,
                                 UserPtr user,
                                 std::string database,
                                 DatabaseInstancePtr instance,
                                 Session::Type type)
        : Session(instance, connection, server, user, database, type)
    {
        _state = STARTUP;
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Server connected: endpoint={}, id={}", connection->endpoint(), _id);
    }

    void
    ServerSession::_process_msg(SessionMsgPtr msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Server session processing message: type: {:d}, seq_id: {}", (int8_t)msg->type(), msg->seq_id());

        // entry point for message processing from client session
        switch(msg->type()) {
        case SessionMsg::MSG_CLIENT_SERVER_STARTUP:
            // this is the startup message from client session
            if (_server->is_ssl_enabled()) {
                // send ssl request to server
                _send_ssl_req();
            } else {
                // otherwise send the startup message
                _send_startup_msg();
            }

            // block more messages until we are ready
            block_messages();
            break;

        case SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY:
        case SessionMsg::MSG_CLIENT_SERVER_PARSE:
        case SessionMsg::MSG_CLIENT_SERVER_BIND:
        case SessionMsg::MSG_CLIENT_SERVER_DESCRIBE:
        case SessionMsg::MSG_CLIENT_SERVER_EXECUTE:
        case SessionMsg::MSG_CLIENT_SERVER_CLOSE:
        case SessionMsg::MSG_CLIENT_SERVER_SYNC:
            _handle_msg_to_server(msg);
            break;

        case SessionMsg::MSG_CLIENT_SERVER_FORWARD: {
            // forward the message to the server
            BufferPtr buffer = msg->buffer();
            ssize_t n = _connection->write(buffer->data(), buffer->size());
            assert(n == buffer->size());
            _log_buffer(false, '\0', buffer->size(), buffer->data(), msg->seq_id());
            break;
        }

        default:
            SPDLOG_WARN("Unknown message: {:d}", (int8_t)msg->type());
            break;
        }
    }

    void
    ServerSession::_process_connection()
    {
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Server session processing connection: state={:d}", (int8_t)_state);

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
        case QUERY:
        case DEPENDENCIES:
        case EXTENDED_ERROR:
            // ready for query, handle requests
            _handle_message_from_server();
            break;
        default:
            SPDLOG_ERROR("Unknown state: {:d}", (int8_t)_state);
            _state = ERROR;
            break;
        }

        if (is_ready()) {
            // if we are ready, then we can process more messages
            enable_messages();
        }
    }

    void
    ServerSession::_handle_message_from_server()
    {
        // Read messages from server session

        // May be in AUTH_DONE or READY state
        // If in AUTH_DONE state:
        // Completed authentication; first expecting auth ok 'R' message
        // followed by 'S' messages for parameter status (may be multiple),
        // followed by 'K' message for backend key data,
        // followed by 'Z' for ready for query

        // see: https://www.postgresql.org/docs/16/protocol-message-formats.html
        // for description of message formats

        // read just the header, the message length is the remaining bytes
        auto [code, msg_length] = _read_hdr();

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Server session message: code={}, length={}", code, msg_length);

        assert(_pending_queue.size() > 0);
        SessionMsgPtr msg = _pending_queue.front()->msg;

        // first handle messages where we just need to forward to client
        switch(code) {
            // responses to extended query protocol
            case '1': // Parse complete (parse)
            case '2': // Bind complete (bind)
            case '3': // Close complete (close)
            case 's': // Portal suspended (execute)
            case 'I': // Empty query response (execute)
            case 'C': // Command complete (execute, simple query)
            case 'n': // No data - response to (describe)
            case 'T': // Row description (describe)
            case 't': // Parameter description (describe)
                if (_state == QUERY) {
                    // we are in query state, continue with query responses
                    _handle_query_response();
                }
                if (_state == DEPENDENCIES) {
                    // we are in dependency checking state, continue with dependencies
                    _handle_dependency_response(false);
                }
                assert (_state != EXTENDED_ERROR);

                // fall through

            case 'f': // Copy fail
            case 'c': // Copy done
            case 'G': // Copy in response
            case 'H': // Copy out response
            case 'W': // Copy both response
            case 'D': // Data row
            case 'N': // Notice response
            case 'A': // Notification response (async from a listen)
                _stream_to_remote_session(code, msg_length, msg->seq_id());
                return;
        }

        // if not handled above then read in full message
        // get a bufffer from the buffer pool
        BufferPtr buffer = BufferPool::get_instance()->get(msg_length);
        ssize_t n = _connection->read(buffer->data(), msg_length);
        assert(n == msg_length);
        buffer->set_size(msg_length);

        // log the buffer
        _log_buffer(true, code, msg_length, buffer->data(), msg->seq_id());

        switch(code) {
            case 'R': {
                // authentication request
                if (_state == AUTH) {
                    // still in auth negotiation state
                    _handle_auth(buffer);
                    break;
                }

                // done with auth, this should be last message
                assert(_state == AUTH_DONE);

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
                // parameter status: either during authentication or as a result of a SET

                // Parameter status
                std::string_view key = buffer->get_string();
                std::string_view value = buffer->get_string();

                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Parameter status from server: {}={}", key, value);

                if (_state <= AUTH_DONE) {
                    // still in auth negotiation state
                    // XXX may want to send these to client
                    break;
                }
                _send_to_remote_session(code, msg_length, buffer->data(), msg->seq_id());

                break;
            }

            case 'K':
                // backend key data
                assert(_state == AUTH_DONE);
                // get the backend pid and key for cancel
                _pid = buffer->get32();
                _cancel_key = buffer->get32();
                break;

            case 'E':
                // Error response
                // handle the error code, this determines if error is fatal
                // it also sends the error response to the client
                _handle_error_code(buffer, msg->seq_id());
                if (_state == ERROR) {
                    return;
                }

                // non-fatal error
                if (_state == QUERY) {
                    // error during query
                    _handle_query_error();
                } else if (_state == DEPENDENCIES) {
                    // error during dependency checking, shouldn't happen
                    _handle_dependency_response(true);
                }

                break;

            case 'Z': {
                // Ready for query
                // I - Idle, T - Transaction, E - Error in transaction
                char status = buffer->get();
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Server session: Ready for query, status={}", status);

                if (_state == AUTH_DONE) {
                    assert (status == 'I');
                    _state = READY;
                    // at this point we should notify client session
                    // server authentication is done, and we can complete
                    // the client session authentication
                    SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_AUTH_DONE);
                    queue_msg(msg);
                    break;
                }

                if (_state != DEPENDENCIES) {
                    // send ready for query to client
                    _send_to_remote_session(code, 1, &status, msg->seq_id());
                }

                // handle the ready for query response
                // regardless of state
                _handle_ready_for_query_response(status);

                break;
            }
            default:
                SPDLOG_ERROR("Unknown message: {}", code);
                _state = ERROR;
                break;
        }

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Done msg handling");
    }

    void
    ServerSession::_send_ssl_req()
    {
        // Send ssl message
        char data[8];
        Buffer buffer(data, 8);
        buffer.put32(8); // length
        buffer.put32(MSG_SSLREQ); // SSL request code
        ssize_t n = _connection->write(buffer.data(), buffer.size());
        assert(n == buffer.size());
    }

    void
    ServerSession::_handle_ssl_response()
    {
        // Read startup ssl message from server in response to send_startup
        // Just one character: 'N' no ssl or 'S' yes ssl
        char ssl_response;
        ssize_t n = _connection->read(&ssl_response, 1);
        assert(n==1);

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "SSL response from server: {}", ssl_response);
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
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "SSL server handshake in progress, need more data");
            return;
        }

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "SSL server handshake complete");

        // send the startup message and then move to AUTH state
        _send_startup_msg();
    }

    void ServerSession::_send_startup_msg()
    {
        // Send startup message
        int msg_len = 8 + 5 + 9 + 17 + 11 + 16 + 5 + _user->username().size() + _database.size() + 3; // length
        BufferPtr buffer = BufferPool::get_instance()->get(msg_len + 4);
        buffer->put32(msg_len);
        buffer->put32(MSG_STARTUP_V3); // protocol version
        buffer->put_string("user");
        buffer->put_string(_user->username());
        buffer->put_string("database");
        buffer->put_string(_database);
        buffer->put_string("application_name");
        buffer->put_string("Springtail");
        buffer->put_string("client_encoding");
        buffer->put_string("UTF8");
        buffer->put(0); // null terminator

        ssize_t n = _connection->write(buffer->data(), buffer->size());
        assert(n == buffer->size());

        _state = AUTH;
    }

    void
    ServerSession::_handle_auth(BufferPtr buffer)
    {
        // Read auth response 'R', we are still in auth flow
        // for SASL, we may have multiple messages
        // for MD5, we have one message
        int32_t auth_type = buffer->get32();

        switch (auth_type) {
            case MSG_AUTH_OK:
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Auth type: OK");
                _state = AUTH_DONE;
                break;

            case MSG_AUTH_MD5:
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Auth type: MD5");
                _handle_auth_md5(buffer);
                // set state to auth done
                _state = AUTH_DONE;
                break;

            case MSG_AUTH_SASL:
                // first message in SASL flow (SCRAM-SHA-256)
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Auth type: SASL");
                // encode reply for first scram message to server
                _handle_auth_scram(buffer);
                break;

            case MSG_AUTH_SASL_CONTINUE:
                // continue SASL flow
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Auth type: SASL continue");
                // encode reply to continue message
                _handle_auth_scram_continue(buffer);
                break;

            case MSG_AUTH_SASL_COMPLETE:
                // complete SASL flow
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Auth type: SASL complete");
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
    ServerSession::_handle_auth_md5(BufferPtr buffer)
    {
        // read in the salt from the server
        int32_t salt;
        buffer->get_bytes(reinterpret_cast<char*>(&salt), 4);

        // get user login info
        _login = _get_user_login();
        if (_login == nullptr) {
            throw ProxyAuthError();
        }
        _login->_salt = salt;

        char md5[MD5_PASSWD_LEN+1];
        // calculate md5 hash; skip the 'md5' prefix on the password; add salt and compute
        assert(_login->_password.starts_with("md5"));
        if (!pg_md5_encrypt(_login->_password.c_str()+3, reinterpret_cast<char*>(&_login->_salt), 4, md5)) {
            SPDLOG_ERROR("Failed to calculate MD5 hash");
            throw ProxyAuthError();
        }
        md5[MD5_PASSWD_LEN] = '\0';

        // encode md5 auth response
        BufferPtr write_buffer = BufferPool::get_instance()->get(41);

        write_buffer->put('p');
        write_buffer->put32(40); // length
        write_buffer->put_string(md5);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());
    }

    void
    ServerSession::_handle_auth_scram(BufferPtr buffer)
    {
        // get user login info
        _login = _get_user_login();
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

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());
    }

    void
    ServerSession::_handle_auth_scram_continue(BufferPtr buffer)
    {
        std::string_view data = buffer->get_bytes(buffer->remaining());

        if (_login->scram_state.client_nonce == nullptr) {
            SPDLOG_ERROR("No client nonce set");
            throw ProxyAuthError();
        }

        if (_login->scram_state.server_first_message != nullptr) {
            SPDLOG_ERROR("Received second SCRAM-SHA-256 continue message");
            throw ProxyAuthError();
        }

        int salt_len;
        char *input = strdup(data.data());

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

        int msg_len = 4 + strlen(client_final_message); // length

        BufferPtr write_buffer = BufferPool::get_instance()->get(1 + msg_len);
        write_buffer->put('p');
        write_buffer->put32(msg_len);
        write_buffer->put_bytes(client_final_message, msg_len - 4);

        free(client_final_message);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());
    }

    void
    ServerSession::_handle_auth_scram_complete(BufferPtr buffer)
    {
        std::string_view data = buffer->get_bytes(buffer->remaining());

        // make sure we are in right flow
        if (_login->scram_state.server_first_message == nullptr) {
            SPDLOG_ERROR("No server first message set");
            throw ProxyAuthError();
        }

        char *input = strdup(data.data());
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
    ServerSession::_send_simple_query(const std::string &query, uint64_t seq_id)
    {
        // Send simple query to server
        BufferPtr write_buffer = BufferPool::get_instance()->get(4 + query.size() + 2);
        write_buffer->put('Q');
        write_buffer->put32(4 + query.size() + 1); // length
        write_buffer->put_string(query);

        ssize_t n = _connection->write(write_buffer->data(), write_buffer->size());
        assert(n == write_buffer->size());

        // log the buffer
        _log_buffer(false, '\0', write_buffer->size(), write_buffer->data(), seq_id);
    }

    void
    ServerSession::_handle_error_code(BufferPtr buffer, uint64_t seq_id)
    {
        // Error response
        SPDLOG_ERROR("Error response from server");

        std::string severity;
        std::string text;
        std::string code;
        std::string message;

        ProxyProtoError::decode_error(buffer, severity, text, code, message);

        // send error to client
        _send_to_remote_session('E', buffer->capacity(), buffer->data(), seq_id);

        // depending on error, behavior is different
        // if text is "FATAL" or "PANIC" we should stop, sever connection
        if (text == "FATAL" || text == "PANIC") {
            SPDLOG_ERROR("Got fatal error from server: {}", message);
            _state = ERROR;
        }

        // if not fatal then wait for ready for query from server
        return;
    }

    void
    ServerSession::_handle_dependency_response(bool error)
    {
        // response to dependency
        assert (_state == DEPENDENCIES);
        assert (!error);

        QueryStatusPtr query_status = _pending_queue.front();

        // add dependency to cache
        auto dep = query_status->msg->get_dependency(query_status->dependency_complete_count);
        if (dep->type == QueryStmt::Type::PREPARE) {
            // add prepared statement to cache
            _stmts.insert(dep->get_hashed_name());
        }

        // check if all dependencies are complete
        query_status->dependency_complete_count++;
        assert (query_status->dependency_complete_count <= query_status->dependency_count);

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Query dependency complete, count: {:d}/{:d}",
                     query_status->dependency_complete_count,
                     query_status->dependency_count);

        if (query_status->dependency_complete_count < query_status->dependency_count) {
            return;
        }

        // all dependencies are complete

        // we need to know if we are expecting a ready for query
        // message from the server (for simple query dependency)
        // if so we shouldn't set the _state to QUERY
        if (!query_status->simple_query_dependency) {
            _state = QUERY;
        }
    }

    void
    ServerSession::_handle_query_error()
    {
        QueryStatusPtr query_status = _pending_queue.front();
        // pop the query from the queue, and issue response
        _pending_queue.pop();
        query_status->msg->set_msg_response(false, query_status->query_complete_count);
        queue_msg(query_status->msg);

        // if we are in extended error state, we need to wait for
        // sync message and won't get any responses until then
        if (query_status->msg->data()->is_extended()) {
            _state = EXTENDED_ERROR;

            // iterate through all pending messages and set them to error
            while (!_pending_queue.empty()) {
                query_status = _pending_queue.front();
                if (query_status->msg->data()->type == QueryStmt::Type::SYNC) {
                    // wait for query ready
                    return;
                }

                // pop the query from the queue, and issue response
                _pending_queue.pop();
                query_status->msg->set_msg_response(false, query_status->query_complete_count);
                queue_msg(query_status->msg);
            }
        }
    }

    void
    ServerSession::_handle_ready_for_query_response(char xact_status)
    {
        QueryStatusPtr query_status = _pending_queue.front();

        if (_state == DEPENDENCIES) {
            // we are in dependency checking state,
            // this shouldn't generate message back to client
            // check if have more messages in queue;
            // if so, check next message for more dependencies
            assert (!_pending_queue.empty());
            assert (query_status != nullptr);

            if (query_status->dependency_count == 0) {
                // we have dependencies, set state to handle them
                _state = QUERY;
            }
            return;
        }

        SessionMsg::MsgStatus msg_status = {xact_status};

        // check if current message is a sync message
        if (query_status != nullptr && query_status->msg->data()->type == QueryStmt::Type::SYNC) {
            _pending_queue.pop();
            query_status->msg->set_status_ready(msg_status);
            queue_msg(query_status->msg);
        } else {
            // if previous message was a simple query then we would
            // have returned earlier when last simple query completed
            // create a new message to send to client
            SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_READY, msg_status);
            queue_msg(msg);
        }

        // if all queries complete, set state to ready
        if (_pending_queue.empty()) {
            _state = READY;
        }
    }

    void
    ServerSession::_handle_query_response()
    {
        assert (_state == QUERY);

        // no error, mark query as complete
        QueryStatusPtr query_status = _pending_queue.front();
        query_status->query_complete_count++;

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Query complete, count: {:d}/{:d}, query_stmt: {}",
                     query_status->query_complete_count,
                     query_status->query_count,
                     (int8_t)query_status->msg->data()->type);

        assert (query_status->query_complete_count <= query_status->query_count);

        // check if this was a prepare that completed
        QueryStmtPtr qs = query_status->msg->data();
        if (qs->type == QueryStmt::Type::PREPARE) {
            // add prepared statement to cache
            _stmts.insert(qs->get_hashed_name());
        } else if (qs->type == QueryStmt::Type::SIMPLE_QUERY) {
            assert (qs->children.size() >= query_status->query_complete_count);
            qs = qs->children[query_status->query_complete_count-1];
            if (qs->type == QueryStmt::Type::PREPARE) {
                // add prepared statement to cache
                _stmts.insert(qs->get_hashed_name());
            }
        }

        // check if not done with parent query, if not then return now
        if (query_status->query_complete_count < query_status->query_count) {
            return;
        }

        // this query is complete; send response to client session
        _pending_queue.pop();
        query_status->msg->set_msg_response(true, query_status->query_complete_count);
        queue_msg(query_status->msg);

        // if all queries complete, set state to ready
        if (_pending_queue.empty()) {
            _state = READY;
            return;
        }

        // have more messages in queue; look at next message
        // see if there are more dependencies
        query_status = _pending_queue.front();
        if (query_status->dependency_count > 0) {
            // we have dependencies, set state to handle them
            _state = DEPENDENCIES;
        }
    }

    void
    ServerSession::_handle_msg_to_server(SessionMsgPtr msg)
    {
        // Entry point for client session message to server
        // we send all dependencies and then the server msg
        // NOTE: this may be called multiple times before
        // receiving a response from the server (if client
        // is pipelining queries)

        // track the query status
        QueryStatusPtr query_status = std::make_shared<QueryStatus>(msg);
        // set query count
        if (msg->data()->children.size() > 0) {
            query_status->query_count = msg->data()->children.size();
        } else {
            query_status->query_count = 1;
        }

        _state = QUERY;
        _pending_queue.push(query_status);

        // get dependencies and issue them to server
        int num_dependencies = msg->num_dependencies();
        for (int i = 0; i < num_dependencies; i++) {
            auto dep = msg->get_dependency(i);
            assert (dep->type == QueryStmt::Type::PREPARE);
            std::string hashed_name = dep->get_hashed_name();
            if (_stmts.contains(hashed_name)) {
                // already prepared, no need to send to server
                continue;
            }
            query_status->dependency_count++;
            _send_dependency(dep, msg->seq_id());
        }

        // send the message to server
        _send_server_msg(query_status);
    }

    void
    ServerSession::_send_server_msg(QueryStatusPtr query_status)
    {
        SessionMsgPtr msg = query_status->msg;
        // queue server message
        if (_state == READY) {
            _state = QUERY;
        }

        QueryStmtPtr qs = msg->data();

        if (qs->data_type == QueryStmt::DataType::SIMPLE) {
            // send the simple query using the query string
            _send_simple_query(qs->query(), msg->seq_id());
        } else {
            // send the data buffer
            assert (qs->data_type == QueryStmt::DataType::PACKET);
            BufferPtr buffer = qs->buffer();
            ssize_t n = _connection->write(buffer->data(), buffer->size());
            assert(n == buffer->size());

            // log the buffer
            _log_buffer(false, '\0', buffer->size(), buffer->data(), msg->seq_id());
        }
    }

    void
    ServerSession::_send_dependency(const QueryStmtPtr query_stmt, uint64_t seq_id)
    {
        // check if we have a buffer to send or a simple query to send
        switch (query_stmt->data_type) {
            case QueryStmt::DataType::SIMPLE:
                // send the simple query
                _send_simple_query(query_stmt->query(), seq_id);
                return;

            case QueryStmt::DataType::PACKET: {
                // send the packet
                BufferPtr buffer = query_stmt->buffer();
                ssize_t n = _connection->write(buffer->data(), buffer->size());
                assert(n == buffer->size());


                // log the buffer
                _log_buffer(false, '\0', buffer->size(), buffer->data(), seq_id);
                return;
            }

            default:
                SPDLOG_WARN("Query not cached");
                assert(0); // shouldn't be set as a dependency it should reside on the primary
        }
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
            throw ProxyIOConnectionError();
        }

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Created connection for server session, to: db={}", database);

        return std::make_shared<ServerSession>(connection, server, user, database, instance, type);
    }
} // namespace springtail::pg_proxy
