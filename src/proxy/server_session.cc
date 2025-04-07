#include <regex>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <common/logging.hh>
#include <common/common.hh>

#include <proxy/session.hh>
#include <proxy/server_session.hh>
#include <proxy/server.hh>
#include <proxy/connection.hh>
#include <proxy/errors.hh>
#include <proxy/exception.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/logging.hh>
#include <proxy/database.hh>
#include <proxy/util.hh>

namespace springtail::pg_proxy {

    /** reset query string; similar to rollback, discard all, set search path, but discard all can't be run with other statements */
    constexpr char RESET_QUERY[] = "ROLLBACK;CLOSE ALL;SET SESSION AUTHORIZATION DEFAULT;RESET ALL;DEALLOCATE ALL;UNLISTEN *;SELECT pg_advisory_unlock_all();DISCARD PLANS;DISCARD TEMP;DISCARD SEQUENCES;SET SEARCH_PATH TO DEFAULT;";

    ServerSession::ServerSession(ProxyConnectionPtr connection,
                                 UserPtr user,
                                 std::string database,
                                 std::string prefix,
                                 DatabaseInstancePtr instance,
                                 const std::unordered_map<std::string, std::string> &parameters,
                                 Session::Type type)

        : Session(instance, connection, user, database, parameters, type), _db_prefix(prefix)
    {
        _state = STARTUP;
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server connected: endpoint={}", _id, connection->endpoint());
    }

    void
    ServerSession::run(std::set<int> &fds)
    {
        // should only be called when session is in reset state or deferred shutdown
        CHECK(_defer_shadow_shutdown || (_state == RESET_SESSION || _state == RESET_SESSION_READY));
        DCHECK_EQ(_client_session.lock(), nullptr);

        _wrap_error_handler([this] {
            do {
                if (_defer_shadow_shutdown) {
                    // process any pending messages
                    process_connection(_seq_id);
                } else {
                    CHECK(_state == RESET_SESSION || _state == RESET_SESSION_READY);
                    // process the reset query response
                    _handle_reset_session_message();
                }
                // check if we have any pending messages necessary for SSL
            } while (_state != ERROR && _connection->has_pending());
        });
    }

    void
    ServerSession::_release_session(bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Server type={}; releasing session", _id,
                    (_type == REPLICA ? "Replica" : "Primary"));
        if (_type == PRIMARY) {
            DatabaseMgr::get_instance()->primary_set()->release_session(shared_from_this(), deallocate);
        } else {
            DatabaseMgr::get_instance()->replica_set()->release_session(shared_from_this(), deallocate);
        }
    }

    bool
    ServerSession::check_startup_params(const std::unordered_map<std::string, std::string> &parameters)
    {
        _wrap_error_handler([this, parameters] {
            // check if the startup parameters match
            for (const auto &param : parameters) {
                // skip those that are fixed
                if (EXCLUDED_STARTUP_PARAMS.contains(param.first)) {
                    // XXX may still want to check these
                    continue;
                }

                auto it = _parameters.find(param.first);
                if (it == _parameters.end() || it->second != param.second) {
                    PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] Server startup parameter mismatch: {}={}; expected={}",
                                _id, param.first, it->second, param.second);
                    return false;
                }
            }

            return true;
        });

        return false;
    }

    void
    ServerSession::startup(uint64_t seq_id)
    {
        CHECK_EQ(_state, STARTUP);

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, seq_id] {
            _auth = std::make_shared<ServerAuthorization>(_connection, _id, _user, _database, _db_prefix, _type, _parameters);
            _state = AUTH_SERVER;
            _auth->send_startup_msg(seq_id);
        });
    }

    void
    ServerSession::startup_reset_session(uint64_t seq_id, const std::unordered_map<std::string, std::string> &parameters)
    {
        // reset the session with new startup parameters
        CHECK_EQ(_state, RESET_SESSION_READY);

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, seq_id, parameters]() {
            _seq_id = seq_id;

            // encode parameters as set statements in simple query
            if (!_send_status_query(parameters)) {
                // nothing to send set to ready state
                set_ready_reset_done();
                return;
            }

            _state = RESET_SESSION_PARAMS;
        });
    }

    void
    ServerSession::set_ready_reset_done()
    {
        CHECK(_state == RESET_SESSION_READY || _state == RESET_SESSION_PARAMS);

        // set state to ready
        _seq_id = 0;
        _state = READY;

        // check for any pending messages
        _process_next_batch();

        // XXX these params may have to be merged with the ones we just set

        // do callback to client session
        auto cs = get_client_session();
        CHECK_NE(cs, nullptr);
        cs->server_auth_done(shared_from_this(), _auth->server_parameters());
    }

    void
    ServerSession::queue_msg_batch(std::deque<SessionMsgPtr> msg_batch)
    {
        _wrap_error_handler([this, &msg_batch] {
            // queue the message batch
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] Server session queueing message batch: size={}, state={}",
                        _id, msg_batch.size(), (int8_t)_state);

            _batch_queue.push_batch(std::move(msg_batch));

            // if not processing anything, start processing this batch
            if (_pending_queue.empty() && _state == READY) {
                _process_next_batch();
            }
        });
    }

    void
    ServerSession::_process_next_batch()
    {
        // process the next queued message batch
        // typically we'll just go through this once, however if the batch is just
        // a forward message, we'll just send it to the server and then be ready
        // for the next batch if there is one
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] Server session processing next batch", _id);

        while (_pending_queue.empty() && _state == READY) {
            if (!_batch_queue.load_processing_batch()) {
                // batch queue is empty
                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] Server session batch queue is empty", _id);
                return;
            }
            while (!_batch_queue.processing_empty()) {
                // process the message
                auto msg = _batch_queue.pop_processing_msg();
                CHECK(msg.has_value());
                process_msg(msg.value());
            }
        }
    }

    void
    ServerSession::process_msg(SessionMsgPtr msg)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server {}{}; message: type: {}, seq_id: {}", _id,
                    (_is_shadow ? "Shadow " : ""), (_type == REPLICA ? "Replica" : "Primary"),
                    msg->type_str(), msg->seq_id());

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, msg] {
            // if not set, set seq_id, otherwise it is set when dequeuing next pending message
            if (_seq_id == 0) {
                _seq_id = msg->seq_id();
            }

            // process the message
            switch(msg->type()) {
                case SessionMsg::MSG_CLIENT_SERVER_SIMPLE_QUERY:
                case SessionMsg::MSG_CLIENT_SERVER_PARSE:
                case SessionMsg::MSG_CLIENT_SERVER_BIND:
                case SessionMsg::MSG_CLIENT_SERVER_DESCRIBE:
                case SessionMsg::MSG_CLIENT_SERVER_EXECUTE:
                case SessionMsg::MSG_CLIENT_SERVER_CLOSE:
                case SessionMsg::MSG_CLIENT_SERVER_SYNC:
                case SessionMsg::MSG_CLIENT_SERVER_FUNCTION:
                    _handle_msg_to_server(msg);
                    break;

                case SessionMsg::MSG_CLIENT_SERVER_FORWARD:
                    // forward the message to the server
                    // usually things like copy data, etc.
                    // write out the buffer
                    _send_buffer(msg->buffer(), msg->seq_id());
                    break;

                default:
                    LOG_WARN("Unknown message: {}", (int8_t)msg->type());
                    break;
            }
        });
    }

    void
    ServerSession::process_shutdown(uint64_t seq_id)
    {
        // try to shutdown the session cleanly so it can be released to the pool
        // called from _handle_error when the client is shutting down
        _seq_id = seq_id;

        _wrap_error_handler([this, seq_id] {

            if (_state == RESET_SESSION || is_shutdown()) {
                // if we are in reset session state, or shutdown return
                return;
            }

            // clear the client before going forward to avoid loops in shutdown handling
            unpin_client_session();

            if (_state == READY) {
                // if in ready state, we can reuse this session, and add back to pool
                // reset server_session and the private session state
                DCHECK(_batch_queue.empty() && _pending_queue.empty());

                reset_session();
                _state = RESET_SESSION;
                // register the session with the server
                ProxyServer::get_instance()->register_session(shared_from_this(), nullptr, _connection->get_socket(), true);
                // send the reset simple query to server
                _send_reset();

                return;
            }

            // NOT in a READY state
            if (is_shadow() && (_state == QUERY || _state == DEPENDENCIES)) {
                // if in shadow mode, we can defer the shutdown until we done
                // with all the messages in the message queue
                _defer_shadow_shutdown = true;
                // re-register the session with the server to receive updates
                ProxyServer::get_instance()->register_session(shared_from_this(), nullptr, _connection->get_socket(), true);

                return;
            }

            // if not in ready state, we do a hard shutdown and close the connection
            // this will return us through Session::_handle_error() and to shutdown_session()
            LOG_WARN("[S:{}] Server session shutting down, state not ready {}", _id, (int8_t)_state);
            _send_shutdown();
            _state = ERROR;

            return;
        });
    }

    void
    ServerSession::process_connection(uint64_t seq_id)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session processing packet: state={:d}", _id, (int8_t)_state);

        _wrap_error_handler([this, seq_id] {
            // entry point for connection message processing
            // called from run in client session
            switch(_state) {
                case AUTH_SERVER:
                    if (_auth->process_auth_data(seq_id)) {
                        // auth done, ready for queries
                        _state = READY;

                        // check for pending messages
                        _process_next_batch();

                        // notify client session that auth is done
                        auto cs = get_client_session();
                        CHECK_NE(cs, nullptr);
                        auto &&params = _auth->server_parameters();
                        cs->server_auth_done(shared_from_this(), params);
                    }

                    if (_state == ERROR) {
                        auto error = _auth->get_error();
                        auto cs = get_client_session();
                        CHECK_NE(cs, nullptr);
                        cs->server_auth_error(shared_from_this(), seq_id, error.first, error.second);
                    }

                    break;

                case READY:
                case QUERY:
                case DEPENDENCIES:
                case EXTENDED_ERROR:
                    // ready for query, handle requests
                    _handle_message_from_server();
                    break;

                case RESET_SESSION_READY:
                case RESET_SESSION_PARAMS:
                    // session is being or has just been reset
                    // mostly dropping messages or handling error waiting for client
                    _handle_reset_session_message();
                    break;

                case RESET_SESSION:
                    CHECK_NE(_state, RESET_SESSION);
                    break;

                default:
                    LOG_ERROR("Unknown state: {:d}", (int8_t)_state);
                    _state = ERROR;
                    break;
            }
        });
    }

    void
    ServerSession::_read_and_drop_message(int msg_length)
    {
        // read in the message from connection and drop it
        if (msg_length == 0) {
            // no message to read
            return;
        }

        char buffer[1024];
        while (msg_length > 0) {
            int to_read = std::min(msg_length, 1024);
            ssize_t n = _connection->read(buffer, to_read, to_read);
            CHECK_EQ(n, to_read);
            msg_length -= n;
        }
    }

    void
    ServerSession::_handle_reset_session_message()
    {
        // read just the header, the message length is the remaining bytes
        auto [code, msg_length] = read_hdr(_connection);

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session message: code={}, length={}", _id, code, msg_length);
        if (msg_length == 0) {
            // no message to read
            return;
        }

        DCHECK_LE(msg_length, 10000); // sanity check

        switch (code) {
            case 'Z': {                 // Ready for query
                // read buffer and log it
                BufferPtr buffer = _read_message(code, msg_length, _seq_id);

                // I - Idle, T - Transaction, E - Error in transaction
                char status = buffer->get();
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session: Ready for query, status={}", _id, status);

                if (status == 'E') {
                    // error in transaction, need to reset session
                    LOG_WARN("[S:{}] Error in transaction while resetting session", _id);
                    _state = ERROR;
                    return;
                }

                if (_state == RESET_SESSION) {
                    // sent reset query, now ready to be added to pool
                    DCHECK_EQ(status, 'I');
                    _seq_id = 0;
                    _state = RESET_SESSION_READY;
                    _release_session(false);
                    return;
                }

                if (_state == RESET_SESSION_PARAMS) {
                    DCHECK_EQ(status, 'I');
                    // reset is complete move to ready state
                    set_ready_reset_done();
                    return;
                }

                break;
            }
            case 'E': {                // Error response
                // read buffer and log it
                BufferPtr buffer = _read_message(code, msg_length, _seq_id);

                // decode the error, may set the state to ERROR, if not fatal we ignore for now
                std::string err_code = _decode_error_buffer(buffer, _seq_id);

                DCHECK(code != 'E' || err_code == "57P01");

                break;
            }

            default:
                // ignore all other messages
                _read_and_drop_message(msg_length);
                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[S:{}] Reset session, dropping message: code={}, length={}", _id, code, msg_length);
                break;
        }
    }

    void
    ServerSession::_send_reset()
    {
        // send reset message to server
        std::string reset_query{RESET_QUERY};

        // buffer length; add cmd length + null terminator + code byte
        int length = reset_query.size() + 4 + 1 + 1;

        BufferPtr buffer = BufferPool::get_instance()->get(length);
        buffer->put('Q');
        buffer->put32(length-1); // don't include code byte
        buffer->put_string(reset_query);

        _send_buffer(buffer, _seq_id);
    }

    void
    ServerSession::_handle_message_from_server()
    {
        // Read messages from server session
        // see: https://www.postgresql.org/docs/16/protocol-message-formats.html
        // for description of message formats

        // read just the header, the message length is the remaining bytes
        auto [code, msg_length] = Session::read_hdr(_connection);

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session message: code={}, length={}, state={}", _id, code, msg_length, (int8)_state);

        DCHECK_LE(msg_length, 1000000); // sanity check

        // first handle messages where we just need to forward to client
        switch(code) {
            // these are messages that are a direct responses to queries
            case 'T': // Row description (describe)
                // 'T' can be a response to a simple query for a select or for a describe
                if (_get_pending_query_type() == QueryStmt::Type::SIMPLE_QUERY) {
                    // simple query, not a completion
                    CHECK_EQ(_state, QUERY);
                    // forward to client
                    _stream_to_remote_session(code, msg_length, _seq_id);
                    return;
                }

                // fall through if not a simple query

            case '1': // Parse complete (parse)
            case '2': // Bind complete (bind)
            case '3': // Close complete (close)
            case 's': // Portal suspended (execute)
            case 'I': // Empty query response (execute, simple query)
            case 'C': // Command complete (execute, simple query)
            case 'n': // No data - response to (describe)
            case 't': // Parameter description (describe)
            case 'V': // Function call response
                CHECK_NE(_state, EXTENDED_ERROR);

                if (_state == DEPENDENCIES) {
                    // we are in dependency checking state, continue with dependencies
                    _handle_dependency_response(false);

                    // drop data
                    _read_and_drop_message(msg_length);

                    return;
                }

                if (_state == QUERY) {
                    // we are in query state, continue with query responses
                    // this may send a message to the client
                    _handle_query_response();
                }

                // fall through, forward message to client

            // these messages are not direct responses to queries are either
            // purely async or are intermediate messages
            case 'G': // Copy in response
            case 'f': // Copy fail
            case 'c': // Copy done
            case 'H': // Copy out response
            case 'W': // Copy both response -- streaming repl only
            case 'D': // Data row -- this may be large
            case 'N': // Notice response
            case 'A': // Notification response (async from a listen)
            case 'd': // Copy data
                // forward message to client
                _stream_to_remote_session(code, msg_length, _seq_id);
                return;
        }

        // if not handled above then read in full message
        // buffer contains the header (code, msg_length)
        // current_data() points past the header
        BufferPtr buffer = _read_message(code, msg_length, _seq_id);

        switch(code) {

            case 'S': {
                // parameter status: either during authentication or as a result of a SET

                // Parameter status
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Parameter status from server: {}={}",
                        _id, buffer->get_string(), buffer->get_string());

                // this is a result of a SET operation
                _send_to_remote_session(code, buffer, _seq_id);

                break;
            }

            case 'K':
                // backend key data
                CHECK_EQ(_state, AUTH_DONE);
                // get the backend pid and key for cancel
                _pid = buffer->get32();
                _cancel_key = buffer->get32();
                break;

            case 'E':
                // Error response
                // handle the error code, this determines if error is fatal
                // it also sends the error response to the client
                _decode_error_buffer(buffer, _seq_id);
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] error", _id);
                if (_state == ERROR) {
                    // TODO: possible this is a dependency error, which for now will be fatal
                    // fatal error, send error to client
                    _send_to_remote_session(code, buffer, _seq_id);
                    return;
                }

                // non-fatal error -- check which state we are in
                if (_state == QUERY) {
                    PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] error in query state, forwarding", _id);
                    // error during query
                    _handle_query_error();
                    // forward to client
                    _send_to_remote_session(code, buffer, _seq_id);
                }

                if (_state == DEPENDENCIES) {
                    // error during dependency checking, shouldn't happen
                    _handle_dependency_response(true);
                    // drop the buffer
                }

                break;

            case 'Z': {
                // Ready for query
                // I - Idle, T - Transaction, E - Error in transaction
                char status = buffer->get();
                uint64_t seq_id = _seq_id; // it may get reset in the handle functions

                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session: Ready for query, status={}", _id, status);

                // handle the ready for query response
                // regardless of state
                _handle_ready_for_query_response(status);

                if (_state == DEPENDENCIES) {
                    return;
                }

                // forward to client
                _send_to_remote_session(code, buffer, seq_id);

                break;
            }
            default:
                LOG_ERROR("Unknown message: {}", code);
                _state = ERROR;
                break;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[S:{}] Done msg handling", _id);
    }

    void
    ServerSession::_send_simple_query(const std::string &query, uint64_t seq_id)
    {
        // Send simple query to server
        BufferPtr write_buffer = BufferPool::get_instance()->get(4 + query.size() + 2);
        write_buffer->put('Q');
        write_buffer->put32(4 + query.size() + 1); // length
        write_buffer->put_string(query);

        _send_buffer(write_buffer, seq_id);
    }

    std::string
    ServerSession::_decode_error_buffer(BufferPtr buffer, uint64_t seq_id)
    {
        // Error response
        std::string severity;
        std::string text;
        std::string code;
        std::string message;

        ProxyProtoError::decode_error(buffer, severity, text, code, message);

        LOG_ERROR("Error response from server: seq_id: {}, {}, {}, {}", seq_id, text, code, message);

        // depending on error, behavior is different
        // if text is "FATAL" or "PANIC" we should stop, sever connection
        if (text == "FATAL" || text == "PANIC") {
            LOG_ERROR("Got fatal error from server: {}", message);
            _state = ERROR;
        }

        // if not fatal then wait for ready for query from server
        return code;
    }

    void
    ServerSession::_handle_dependency_response(bool error)
    {
        // response to dependency
        CHECK_EQ(_state, DEPENDENCIES);
        DCHECK(!error);

        CHECK(!_pending_queue.empty());
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

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Query dependency complete, count: {:d}/{:d}",
                    _id, query_status->dependency_complete_count,
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
        assert (!_pending_queue.empty());
        QueryStatusPtr query_status = _pending_queue.front();

        // pop the query from the queue, and issue response
        _pending_queue.pop();
        query_status->msg->set_msg_response(query_status->query_complete_count);
        _client_msg_response(query_status->msg, false);

        // if we are in extended error state, we need to wait for
        // sync message and won't get any responses until then
        if (!query_status->msg->data()->is_extended()) {
            return;
        }

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
            _client_msg_response(query_status->msg, false);
        }
    }

    void
    ServerSession::_handle_ready_for_query_response(char xact_status)
    {
        QueryStatusPtr query_status = nullptr;
        if (!_pending_queue.empty()) {
            query_status = _pending_queue.front();
        }

        if (_state == DEPENDENCIES) {
            // we are in dependency checking state,
            // this shouldn't generate message back to client
            // check if have more messages in queue;
            // if so, check next message for more dependencies
            assert (!_pending_queue.empty());
            assert (query_status != nullptr);

            if (query_status->dependency_count == 0) {
                // we had dependencies, set state to query
                _state = QUERY;
            }
            return;
        }

        DCHECK(_state == QUERY || _state == EXTENDED_ERROR);

        // check if current message is a sync message
        if (query_status != nullptr && query_status->msg->data()->type == QueryStmt::Type::SYNC) {
            _pending_queue.pop();
        }

        // send ready for query message to client
        if (!_is_shadow) {
            auto cs = get_client_session();
            CHECK_NE(cs, nullptr);
            cs->server_ready_msg(xact_status);
        }

        // if pending queue is not empty need to wait for them to complete
        if (!_pending_queue.empty()) {
            return;
        }

        // no pending message, set state to ready and process next batch
        _state = READY;
        _seq_id = 0;

        if (_defer_shadow_shutdown && _batch_queue.empty()) {
            // we are in shadow mode with a deferred shutdown and no more messages to process
            // we can reset the session and release it to the pool
            CHECK(_is_shadow);
            reset_session();
            _state = RESET_SESSION;
            _send_reset();
            return;
        }

        _process_next_batch();
    }

    QueryStmt::Type
    ServerSession::_get_pending_query_type()
    {
        if (_pending_queue.empty()) {
            return QueryStmt::Type::NONE;
        }
        QueryStatusPtr query_status = _pending_queue.front();
        return query_status->msg->data()->type;
    }

    void
    ServerSession::_handle_query_response()
    {
        CHECK_EQ(_state, QUERY);
        CHECK(!_pending_queue.empty());

        // no error, mark query as complete
        QueryStatusPtr query_status = _pending_queue.front();
        query_status->query_complete_count++;

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Query complete, count: {:d}/{:d}, query_stmt: {}",
                    _id, query_status->query_complete_count,
                    query_status->query_count,
                    (int8_t)query_status->msg->data()->type);

        CHECK_LE(query_status->query_complete_count, query_status->query_count);

        // check if this was a prepare that completed
        QueryStmtPtr qs = query_status->msg->data();
        switch (qs->type) {
            case QueryStmt::Type::PREPARE:
                // add prepared statement to cache
                _stmts.insert(qs->get_hashed_name());
                break;

            case QueryStmt::Type::SIMPLE_QUERY:
                // it is possible for children.size() == 0 when there is an empty query
                if (qs->children.size() > 0) {
                    assert (qs->children.size() >= query_status->query_complete_count);
                    qs = qs->children[query_status->query_complete_count-1];
                    if (qs->type == QueryStmt::Type::PREPARE) {
                        // add prepared statement to cache
                        _stmts.insert(qs->get_hashed_name());
                    }
                }
                break;
            default:
                break;
        }

        // check if not done with parent query, if not then return now
        if (query_status->query_complete_count < query_status->query_count) {
            return;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] All queries complete for this msg, seq_id: {}", _id, query_status->msg->seq_id());

        // this query is complete; send response to client session
        _pending_queue.pop();

        // send response notification to client
        query_status->msg->set_msg_response(query_status->query_complete_count);
        _client_msg_response(query_status->msg, true);

        // if all queries complete
        if (_pending_queue.empty()) {
            return;
        }

        // have more messages in queue; look at next message
        query_status = _pending_queue.front();
        _seq_id = query_status->msg->seq_id();

        // see if there are more dependencies
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

        // set seq_id if this is a new message that is current
        if (_pending_queue.empty()) {
            _seq_id = msg->seq_id();
        }

        _pending_queue.push(query_status);
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session: msg to server, msg: {}, seq_id: {}, query_count: {}",
                    _id, msg->type_str(), msg->seq_id(), query_status->query_count);

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
    ServerSession::_client_msg_response(SessionMsgPtr msg, bool success)
    {
        if (_is_shadow) {
            // shadow session, don't send response to client
            return;
        }
        // otherwise send message and status to client session
        auto cs = get_client_session();
        CHECK_NE(cs, nullptr);
        cs->server_msg_response(msg, success);
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
            DCHECK_EQ(qs->data_type, QueryStmt::DataType::PACKET);
            _send_buffer(qs->buffer(), msg->seq_id());
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
                _send_buffer(query_stmt->buffer(), seq_id);
                return;
            }

            default:
                LOG_WARN("Query not cached");
                assert(0); // shouldn't be set as a dependency it should reside on the primary
        }
    }

    void
    ServerSession::_send_shutdown()
    {
        // send the shutdown message to the server
        BufferPtr buffer = BufferPool::get_instance()->get(5);
        buffer->put('X');
        buffer->put32(4); // length

        _send_buffer(buffer, _seq_id);
    }

    bool
    ServerSession::_send_status_query(const std::unordered_map<std::string, std::string> &parameters)
    {
        std::ostringstream query;

        if (parameters.empty()) {
            // no parameters to set
            return false;
        }

        std::map<std::string, std::string> params;

        // this function goes through the client session startup params and tries to
        // apply them on the server session as a set of SET statements
        // some startup params may be encoded as options: options= -c key=value -c key=value
        // these have to be split out and processed separately.
        // No escaping is needed as startup params so the values are quoted if needed

        // go through parameters to find valid ones; these maps should be very small
        for (const auto& [key, value] : parameters) {
            if (EXCLUDED_STARTUP_PARAMS.contains(key) || !util::is_valid_postgres_key(key)) {
                continue;
            }

            if (key == "options") {
                // if the key is options, parse the options, values are quoted
                auto option_map = util::parse_options(value);

                // go through the options and set the valid ones
                for (const auto& [opt_key, opt_value] : option_map) {
                    PROXY_DEBUG(LOG_LEVEL_DEBUG4, "[S:{}] Server options key: {}, value: {}", _id, opt_key, opt_value);
                    if (util::is_valid_postgres_key(opt_key) &&
                        !EXCLUDED_STARTUP_PARAMS.contains(opt_key) &&
                        util::is_valid_postgres_value(opt_value)) {
                        query << "SET " << opt_key << "=" << opt_value << ";";
                    }
                }
            } else {
                // regular key value pair
                std::string quoted_value = util::quote_postgres_value(value);
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "[S:{}] Server key: {}, value: {}", _id, key, quoted_value);
                if (util::is_valid_postgres_value(quoted_value)) {
                    query << "SET " << key << "=" << quoted_value << ";";
                }
            }
        }

        // get the query string
        std::string query_str = query.str();

        if (query_str.empty()) {
            // no parameters to set
            return false;
        }

        // create a buffer for the query
        // length = 4B len + query size + null terminator + code byte
        int length = query_str.size() + 4 + 1 + 1;
        BufferPtr buffer = BufferPool::get_instance()->get(length);

        // create a query to set the session parameters
        buffer->put('Q');
        buffer->put32(length - 1); // subtract 1 for the code byte
        buffer->put_string(query.str());

        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "[S:{}] Server session: encode status query: {}", _id, query_str);

        _send_buffer(buffer, _seq_id);

        return true;
    }

    void
    ServerSession::shutdown_session()
    {
        // callback from Session::_handle_error()
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Server session is shutting down, socket={}", _id, _connection->get_socket());

        // grab a shared pointer to self, to avoid losing the reference during cleanup
        ServerSessionPtr self = shared_from_this();

        // notify client session of error
        auto client = get_client_session();
        if (client != nullptr) {
            client->server_shutdown(self);
        }

        // on error, the server shuts down the connection and releases the session
        ProxyServer::get_instance()->shutdown_session(self);

        // do the disconnect
        ProxyServer::get_instance()->log_disconnect(self);
        _connection->close();

        // clear all internal data structures
        reset_session();

        // release the session, and deallocate it
        _release_session(true);
    }

    /** factory to create session */
    ServerSessionPtr
    ServerSession::create(UserPtr user,
                          const std::string &database,
                          const std::string &prefix,
                          DatabaseInstancePtr instance,
                          Session::Type type,
                          const std::unordered_map<std::string, std::string> &params)
    {
        assert (instance != nullptr);

        auto connection = instance->create_connection();
        if (connection == nullptr) {
            LOG_ERROR("Failed to create connection for server db");
            throw ProxyIOConnectionError();
        }

        ServerSessionPtr session = std::make_shared<ServerSession>(connection, user, database, prefix, instance, params, type);
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[S:{}] Created connection for server session, to: db={}", session->id(), database);

        ProxyServer::get_instance()->log_connect(session);

        return session;
    }
} // namespace springtail::pg_proxy
