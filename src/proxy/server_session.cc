#include <sys/socket.h>

#include <proxy/server_session.hh>
#include <proxy/server.hh>
#include <proxy/errors.hh>
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
        _state = State::STARTUP;
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server connected: endpoint={}", _id, connection->endpoint());
    }

    ServerSession::~ServerSession()
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session being deallocated", _id);
        try {
            if (_instance != nullptr) {
                _instance->remove_session(_id);
            }
        } catch (const std::exception &e) {
            LOG_ERROR("Error deallocating server session: {}", e.what());
        }
    }

    void
    ServerSession::run(std::set<int> &fds)
    {
        // should only be called when session is in reset state or deferred shutdown
        CHECK(_defer_shadow_shutdown || (_state == State::RESET_SESSION || _state == State::RESET_SESSION_READY));
        DCHECK_EQ(_client_session.lock(), nullptr);

        _wrap_error_handler([this] {
            do {
                if (_defer_shadow_shutdown) {
                    // process any pending messages
                    process_connection(_seq_id);
                } else {
                    CHECK(_state == State::RESET_SESSION || _state == State::RESET_SESSION_READY);
                    // process the reset query response
                    _handle_reset_session_message();
                }
                // check if we have any pending messages necessary for SSL
            } while (_state != State::ERROR && _connection->has_pending());
        });
    }

    bool
    ServerSession::_release_session(bool deallocate)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Server type={}; releasing session", _id,
                    (_type == Type::REPLICA ? "Replica" : "Primary"));

        if (_type == Type::PRIMARY) {
            return DatabaseMgr::get_instance()->primary_set()->release_session(shared_from_this(), deallocate);
        } else {
            return DatabaseMgr::get_instance()->replica_set()->release_session(shared_from_this(), deallocate);
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
                    LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[S:{}] Server startup parameter mismatch: {}={}; expected={}",
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
        CHECK_EQ(_state, State::STARTUP);

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, seq_id] {
            _auth = std::make_shared<ServerAuthorization>(_connection, _id, _user, _database, _db_prefix, _type, _parameters);
            _state = State::AUTH_SERVER;
            _auth->send_startup_msg(seq_id);
        });
    }

    void
    ServerSession::startup_reset_session(uint64_t seq_id, const std::unordered_map<std::string, std::string> &parameters)
    {
        // reset the session with new startup parameters
        CHECK_EQ(_state, State::RESET_SESSION_READY);

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, seq_id, parameters]() {
            _seq_id = seq_id;

            // encode parameters as set statements in simple query
            if (!_send_status_query(parameters)) {
                // nothing to send set to ready state
                set_ready_reset_done();
                return;
            }

            _state = State::RESET_SESSION_PARAMS;
        });
    }

    void
    ServerSession::set_ready_reset_done()
    {
        CHECK(_state == State::RESET_SESSION_READY || _state == State::RESET_SESSION_PARAMS);

        LOG_INFO("[S:{}] Server session reset complete; ready for queries", _id);

        // set state to ready
        _seq_id = 0;
        _state = State::READY;

        // XXX these auth params may have to be merged with the ones we just set

        // do callback to client session
        auto cs = get_client_session();
        CHECK_NE(cs, nullptr);
        cs->server_auth_done(shared_from_this(), _auth->server_parameters());

        // check for any pending messages
        _process_next_batch();
    }

    void
    ServerSession::queue_msg_batch(std::deque<SessionMsgPtr> msg_batch)
    {
        // Called from client session to queue a batch of messages
        _wrap_error_handler([this, &msg_batch] {
            // queue the message batch
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[S:{}] Server session queueing message batch: size={}, state={}",
                        _id, msg_batch.size(), (int8_t)_state);

            _batch_queue.push_batch(std::move(msg_batch));

            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[S:{}] Server session batch queue size: {}",
                        _id, _batch_queue.size());

            // if not processing anything, start processing this batch
            if (_current_batch == nullptr && _state == State::READY) {
                _process_next_batch();
            }
        });
    }

    void
    ServerSession::forward_msg(SessionMsgPtr msg)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server {}{}; message: type: {}, seq_id: {}", _id,
                    (_is_shadow ? "Shadow " : ""), (_type == Type::REPLICA ? "Replica" : "Primary"),
                    msg->type_string(), msg->seq_id());

        // wrap in error handler to catch any exceptions
        _wrap_error_handler([this, msg] {
            // if not set, set seq_id, otherwise it is set when dequeuing next pending message
            if (_seq_id == 0) {
                _seq_id = msg->seq_id();
            }

            DCHECK(msg->type() == SessionMsg::MSG_CLIENT_SERVER_FORWARD);

            // forward the message to the server
            // usually things like copy data, etc.
            // write out the buffer
            _send_buffer(msg->buffer(), msg->seq_id());

        });
    }

    void
    ServerSession::process_shutdown(uint64_t seq_id)
    {
        // try to shutdown the session cleanly so it can be released to the pool
        // called from _handle_error when the client is shutting down
        _seq_id = seq_id;

        _wrap_error_handler([this] {

            if (_state == State::RESET_SESSION || is_shutdown()) {
                // if we are in reset session state, or shutdown return
                return;
            }

            // clear the client before going forward to avoid loops in shutdown handling
            unpin_client_session();

            if (_state == State::READY && _db_id != constant::INVALID_DB_ID) {
                // if in ready state, we can reuse this session, and add back to pool
                // reset server_session and the private session state
                DCHECK(_batch_queue.empty() && _current_batch == nullptr);

                reset_session();
                _state = State::RESET_SESSION;
                // register the session with the server
                ProxyServer::get_instance()->register_session(shared_from_this(), nullptr, _connection->get_socket(), true);
                // send the reset simple query to server
                _send_reset();

                return;
            }

            // NOT in a READY state
            if (is_shadow() && (_state == State::QUERY || _state == State::DEPENDENCIES)) {
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
            _state = State::ERROR;

            return;
        });
    }

    void
    ServerSession::process_connection(uint64_t seq_id)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session processing packet: state={:d}", _id, (int8_t)_state);

        _wrap_error_handler([this, seq_id] {
            // entry point for connection message processing
            // called from run in client session
            switch(_state) {
                case State::AUTH_SERVER:
                    if (_auth->process_auth_data(seq_id)) {
                        // auth done, ready for queries
                        _state = State::READY;

                        // check for pending messages
                        _process_next_batch();

                        // notify client session that auth is done
                        auto cs = get_client_session();
                        CHECK_NE(cs, nullptr);
                        auto &&params = _auth->server_parameters();
                        cs->server_auth_done(shared_from_this(), params);
                    }

                    if (_state == State::ERROR) {
                        auto error = _auth->get_error();
                        auto cs = get_client_session();
                        CHECK_NE(cs, nullptr);
                        cs->server_auth_error(shared_from_this(), seq_id, error.first, error.second);
                    }

                    break;

                case State::READY:
                case State::QUERY:
                case State::DEPENDENCIES:
                case State::EXTENDED_ERROR:
                    // ready for query, handle requests
                    _handle_message_from_server();
                    break;

                case State::RESET_SESSION_READY:
                case State::RESET_SESSION_PARAMS:
                    // session is being or has just been reset
                    // mostly dropping messages or handling error waiting for client
                    _handle_reset_session_message();
                    break;

                case State::RESET_SESSION:
                    CHECK_NE(_state, State::RESET_SESSION);
                    break;

                default:
                    LOG_ERROR("Unknown state: {:d}", (int8_t)_state);
                    _state = State::ERROR;
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

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session message: code={}, length={}", _id, code, msg_length);
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
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session: Ready for query, status={}", _id, status);

                if (status == 'E') {
                    // error in transaction, need to reset session
                    LOG_WARN("[S:{}] Error in transaction while resetting session", _id);
                    _state = State::ERROR;
                    return;
                }

                if (_state == State::RESET_SESSION) {
                    // sent reset query, now ready to be added to pool
                    DCHECK_EQ(status, 'I');
                    _seq_id = 0;
                    _state = State::RESET_SESSION_READY;
                    if (_release_session(false)) {
                        _session_cleanup();
                    }
                    return;
                }

                if (_state == State::RESET_SESSION_PARAMS) {
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
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[S:{}] Reset session, dropping message: code={}, length={}", _id, code, msg_length);
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

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session message: code={}, length={}, state={}", _id, code, msg_length, (int8)_state);

        DCHECK_LE(msg_length, 1000000); // sanity check

        // first handle messages where we just need to forward to client
        switch(code) {
            // these are messages that are a direct responses to queries
            case 'T': { // Row description (describe)
                // 'T' can be a response to a simple query for a select or for a describe
                auto query_status = _current_batch->get_current_query_status();
                CHECK_NE(query_status, nullptr);

                if (query_status->msg->is_simple_query_message()) {
                    // simple query, not a completion
                    CHECK_EQ(_state, State::QUERY);
                    // forward to client
                    _stream_to_remote_session(code, msg_length, _seq_id);
                    return;
                }
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
                CHECK_NE(_state, State::EXTENDED_ERROR);

                if (_state == State::DEPENDENCIES) {
                    // we are in dependency checking state, continue with dependencies
                    _handle_msg_response();

                    // drop data
                    _read_and_drop_message(msg_length);

                    return;
                }

                if (_state == State::QUERY) {
                    // we are in query state, continue with query responses
                    // this may send a message to the client
                    _handle_msg_response();
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
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Parameter status from server: {}={}",
                        _id, buffer->get_string(), buffer->get_string());

                // this is a result of a SET operation
                _send_to_remote_session(code, buffer, _seq_id);

                break;
            }

            case 'K':
                // backend key data
                CHECK_EQ(_state, State::AUTH_DONE);
                // get the backend pid and key for cancel
                _pid = buffer->get32();
                _cancel_key.resize(buffer->remaining());
                buffer->get_bytes(reinterpret_cast<char *>(_cancel_key.data()), buffer->remaining());
                break;

            case 'E':
                // Error response
                // handle the error code, this determines if error is fatal
                // it also sends the error response to the client
                _decode_error_buffer(buffer, _seq_id);
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] error", _id);
                if (_state == State::ERROR) {
                    // TODO: possible this is a dependency error, which for now will be fatal
                    // fatal error, send error to client
                    _send_to_remote_session(code, buffer, _seq_id);
                    return;
                }

                // non-fatal error -- check which state we are in
                if (_state == State::QUERY) {
                    LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] error in query state, forwarding", _id);
                    // error during query
                    _handle_query_error();
                    // forward to client
                    _send_to_remote_session(code, buffer, _seq_id);
                }

                if (_state == State::DEPENDENCIES) {
                    // error during dependency checking, shouldn't happen
                    _handle_msg_response();
                    DCHECK(false) << "Error during dependency checking shouldn't happen";
                    // drop the buffer
                }

                break;

            case 'Z': {
                // Ready for query
                // I - Idle, T - Transaction, E - Error in transaction
                char status = buffer->get();
                uint64_t seq_id = _seq_id; // it may get reset in the handle functions

                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session: Ready for query, status={}", _id, status);

                // save current state as handling the ready for query may change it
                auto state = _state;

                // handle the ready for query response
                // regardless of state
                _handle_ready_for_query_response(status);

                if (state == State::DEPENDENCIES) {
                    return;
                }

                // forward to client
                _send_to_remote_session(code, buffer, seq_id);

                break;
            }
            default:
                LOG_ERROR("Unknown message: {}", code);
                _state = State::ERROR;
                break;
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Done msg handling", _id);
    }

    void
    ServerSession::_send_simple_query(const std::string &query, uint64_t seq_id)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Sending simple query MESSAGE: {}", _id, query);

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
            _state = State::ERROR;
        }

        // if not fatal then wait for ready for query from server
        return code;
    }

    void
    ServerSession::_handle_query_error()
    {
        if (_state != State::QUERY && _state != State::DEPENDENCIES) {
            LOG_ERROR("[S:{}] Received query error in unexpected state: {}",
                    _id, static_cast<int>(_state));
            return;
        }

        if (!_current_batch || _current_batch->query_status_queue.empty()) {
            LOG_ERROR("[S:{}] Received query error but no current batch or empty queue", _id);
            return;
        }

        // Get the current QueryStatus being processed
        // This is always at the front of the queue due to in-order processing
        QueryStatusPtr query_status = _current_batch->get_current_query_status();
        if (!query_status) {
            LOG_ERROR("[S:{}] No current query status for error handling", _id);
            return;
        }

        SessionMsgPtr msg = query_status->msg;

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] Error response MESSAGE from {}: seq_id: {}, type: {}, is_dependency: {}",
                _id,
                _type == Type::REPLICA ? "REPLICA" : "PRIMARY",
                query_status->msg->seq_id(),
                query_status->msg->type_string(),
                query_status->is_dependency() ? "true" : "false");

        // Check if this is a dependency error
        if (query_status->is_dependency()) {
            // Dependency error - log but don't send to client
            LOG_WARN("[S:{}] Dependency message failed, seq_id: {}, type: {}",
                    _id,
                    msg->seq_id(),
                    static_cast<int8_t>(msg->data()->type));

            // XXX need to decide what to do here, should we fail the entire session?
            // it is possible the failure is due to a non-existent table on a replica
            // or something similar, which should not be fatal, but could cause this
            // to be rerun on the primary.

            // for now fail the session (if primary) / replica
            _state = State::ERROR;

            return;
        }

        // Send error notification to client
        msg->set_msg_response(query_status->query_count);
        _client_msg_response(msg, false); // false = not successful

        // Mark all statements as complete (failed)
        query_status->query_complete_count = query_status->query_count;

        // Remove failed query from queue
        _current_batch->remove_completed_query_status();

        // Check protocol type to determine error handling strategy
        if (msg->is_simple_query_message()) {
            // Simple query error
            // Server will send ReadyForQuery after ErrorResponse
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1,
                    "[S:{}] Simple query error, waiting for ReadyForQuery", _id);

            // Wait for ReadyForQuery - it will handle state transition
            return;
        }

        // Extended protocol error

        // Enter extended error state
        // Server will remain in error state until it receives Sync
        _state = State::EXTENDED_ERROR;

        // all queued queries are now invalid until Sync
        bool sync_found = false;
        while (!_current_batch->query_status_queue.empty()) {
            QueryStatusPtr qs = _current_batch->get_current_query_status();
            if (qs->msg->is_sync_message()) {
                // Stop at Sync message
                sync_found = true;
                break;
            }
            _current_batch->remove_completed_query_status();
        }

        if (!sync_found) {
            LOG_ERROR("[S:{}] No Sync message found after extended protocol error", _id);
            // protocol error, set to ERROR state
            DCHECK(false) << "No Sync message found after extended protocol error";
        }

        // Server requires Sync message to recover from error state
        // Enter EXTENDED_ERROR state and wait for Sync
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1,
                "[S:{}] Extended protocol error, entering EXTENDED_ERROR state", _id);

        // Note: We continue processing remaining messages in batch
        // The next message should be a Sync (or we have a protocol error)
        // When Sync's ReadyForQuery arrives, we'll handle recovery
    }

    void
    ServerSession::_handle_extended_error_ready_for_query(char transaction_status)
    {
        CHECK_EQ(_state, State::EXTENDED_ERROR);

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1,
                "[S:{}] Extended error recovery complete, transaction_status: {}",
                _id, transaction_status);

        if (!_current_batch) {
            LOG_ERROR("[S:{}] No current batch during extended error recovery", _id);
            // Transition back to READY state anyway to prevent getting stuck
            _state = State::READY;
            _process_next_batch();
            return;
        }

        // Send ReadyForQuery to client
        // Client needs to know that error state is cleared
        if (!_is_shadow) {
            auto client_session = get_client_session();
            if (client_session != nullptr) {
                client_session->server_ready_msg(transaction_status);
            }
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3,
                "[S:{}] Extended error ReadyForQuery",
                _id);

        // Transition back to QUERY state
        // Server is now ready to process normal queries again
        _state = State::QUERY;

        // Check if batch is complete
        if (_current_batch->is_complete()) {
            _complete_batch();
        }
    }

    void
    ServerSession::_process_next_batch()
    {
        // process the next queued message batch
        // typically we'll just go through this once, however if the batch is just
        // a forward message, we'll just send it to the server and then be ready
        // for the next batch if there is one
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session processing next batch, batch size: {}",
                  _id, _batch_queue.size());

        std::vector<SessionMsgPtr> messages;

        if (_state != State::READY || _current_batch != nullptr || _batch_queue.empty()) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Server session not ready, cannot process next batch",
                      _id);
            return;
        }

        // pop the next batch from the queue
        auto batch = _batch_queue.pop_batch();
        while (!batch.empty()) {
            auto msg = batch.front();
            batch.pop_front();
            messages.push_back(msg);
        }

        if (messages.empty()) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[S:{}] No messages to process in next batch", _id);
            return;
        }

        // Create batch structure from messages
        _current_batch = std::make_shared<Batch>(messages);
        DCHECK_EQ(_current_batch->is_empty(), false);

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] Created batch: {} total messages",
                _id,
                _current_batch->get_total_message_count());

        // send all messages in the batch
        _send_all_messages();
    }


    void
    ServerSession::_send_all_messages()
    {
        DCHECK_NE(_current_batch, nullptr);

        auto front_query_status = _current_batch->get_current_query_status();
        if (!front_query_status) {
            // no messages to send, can complete batch
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Cannot send messages: current batch has no messages", _id);
            _complete_batch();
            return;
        }

        size_t messages_sent = 0;
        size_t flush_messages = 0;

        // Determine initial state based on first message type
        if (front_query_status->is_dependency()) {
            // Start in DEPENDENCIES state
            _state = State::DEPENDENCIES;
        } else {
            // Start in QUERY state
            _state = State::QUERY;
        }

        // set seq_id to first message
        _seq_id = front_query_status->msg->seq_id();

        // Iterate through query_status_queue and send all dependencies
        // Dependencies should always be at the front of the queue in a batch
        for (auto it = _current_batch->query_status_queue.begin();
                  it != _current_batch->query_status_queue.end();) {

            auto query_status = *it;
            auto msg = query_status->msg;
            auto is_dependency = query_status->is_dependency();

            DCHECK(msg);

            if (_state == State::DEPENDENCIES && !is_dependency) {
                // Reached end of dependencies, stop
                break;
            }

            if (_state == State::QUERY && is_dependency) {
                // Should not happen, dependencies should be first
                LOG_WARN("[S:{}] Unexpected dependency message in QUERY state", _id);
                break;
            }

            // no reply for flush message so remove from queue here, prior to sending
            if (msg->is_flush_message()) {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                        "[S:{}] Removing flush message from queue, seq_id: {}",
                        _id, msg->seq_id());
                it = _current_batch->query_status_queue.erase(it);
                flush_messages++;
            } else {
                ++it;
            }

            // Send message to server
            _send_msg(query_status);
            messages_sent++;
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] Sent {} messages",
                _id, messages_sent);


        if (flush_messages == messages_sent) {
            // All messages were flush messages, can complete batch
            // no responses expected, transition to READY state
            _complete_batch();
        }
    }

    void
    ServerSession::_send_msg(QueryStatusPtr query_status)
    {
        if (!query_status || !query_status->msg) {
            LOG_ERROR("[S:{}] Cannot send message: invalid QueryStatus or message", _id);
            return;
        }

        SessionMsgPtr msg = query_status->msg;
        DCHECK(_state == State::QUERY || _state == State::DEPENDENCIES);

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] Sending MESSAGE to {}: seq_id: {}, type: {}, is_dependency: {}",
                _id,
                _type == Type::REPLICA ? "REPLICA" : "PRIMARY",
                msg->seq_id(),
                msg->type_string(),
                query_status->is_dependency() ? "true" : "false");

        QueryStmtPtr qs = msg->data();
        if (qs->data_type == QueryStmt::DataType::SIMPLE) {
            // send the simple query using the query string
            _send_simple_query(qs->query(), msg->seq_id());
        } else {
            // send the data buffer
            DCHECK(qs->data_type == QueryStmt::DataType::PACKET);
            _send_buffer(qs->buffer(), msg->seq_id());
        }
    }

    void
    ServerSession::_handle_msg_response()
    {
        CHECK(_state == State::QUERY || _state == State::DEPENDENCIES);

        if (!_current_batch || _current_batch->query_status_queue.empty()) {
            LOG_ERROR("[S:{}] Received query response but no current batch or empty queue", _id);
            return;
        }

        // Get the current QueryStatus being processed
        // This is always at the front of the queue due to in-order processing
        QueryStatusPtr query_status = _current_batch->get_current_query_status();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] Response MESSAGE from {}: seq_id: {}, type: {}, is_dependency: {}",
                _id,
                _type == Type::REPLICA ? "REPLICA" : "PRIMARY",
                query_status->msg->seq_id(),
                query_status->msg->type_string(),
                query_status->is_dependency() ? "true" : "false");

        bool query_complete = _current_batch->mark_message_complete();
        if (!query_complete) {
            // More messages to process in current state
            return;
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1,
                "[S:{}] All statements complete for this message, seq_id: {}",
                _id, query_status->msg->seq_id());

        // Send completion notification to client session
        // This is called BEFORE ReadyForQuery is received
        if (_state == State::QUERY) {
            query_status->msg->set_msg_response(query_status->query_complete_count);
            _client_msg_response(query_status->msg, true);
        }

        // State transition happens in _handle_ready_for_query when ReadyForQuery arrives
    }

    void
    ServerSession::_handle_ready_for_query_response(char transaction_status)
    {
        if (!_current_batch) {
            LOG_ERROR("[S:{}] Cannot handle query ReadyForQuery: no current batch", _id);
            DCHECK(false) << "No current batch in ReadyForQuery handling";
            return;
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] ReadyForQuery MESSAGE from {}, transaction_status: {}",
                _id,
                _type == Type::REPLICA ? "REPLICA" : "PRIMARY",
                transaction_status);

        // Check if current message is a Sync message
        // Sync messages only generate ReadyForQuery, so mark it as complete
        auto query_status = _current_batch->get_current_query_status();
        if (query_status && query_status->msg->is_sync_message()) {
            query_status->mark_message_complete();
            _current_batch->mark_message_complete();

            // notify client of sync completion to add to session history
            if (!_is_shadow && !query_status->is_dependency()) {
                auto client_session = get_client_session();
                CHECK_NE(client_session, nullptr);
                client_session->server_msg_response(query_status->msg, _id, true);
            }
        }

        // Handle ready for query based on state
        switch (_state) {
            case State::DEPENDENCIES:
                _handle_dependency_ready_for_query(transaction_status);
                break;

            case State::QUERY:
                _handle_query_ready_for_query(transaction_status);
                break;

            case State::EXTENDED_ERROR:
                // Extended error state handling for extended protocol recovery
                // Requires special handling to wait for Sync message
                _handle_extended_error_ready_for_query(transaction_status);
                break;

            default:
                LOG_ERROR("[S:{}] Received ReadyForQuery in unexpected state: {}",
                    _id, static_cast<int>(_state));
                DCHECK(false) << "Unexpected state in ReadyForQuery handling";
                break;
        }
    }

    void
    ServerSession::_handle_query_ready_for_query(char transaction_status)
    {
        // State must be QUERY here
        DCHECK_EQ(_state, State::QUERY);

        // Send ReadyForQuery to client session
        if (!_is_shadow) {
            auto client_session = get_client_session();
            CHECK_NE(client_session, nullptr);
            client_session->server_ready_msg(transaction_status);
        }

        // Check if all queries are complete
        if (!_current_batch->is_complete()) {
            return;
        }

        _complete_batch();
    }

    void
    ServerSession::_complete_batch()
    {
        // All queries in this batch are complete
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1,
                  "[S:{}] All queries complete, batch finished", _id);

        DCHECK_NE(_current_batch, nullptr);
        DCHECK_EQ(_current_batch->is_complete(), true);

        // Reset state
        _current_batch = nullptr;
        _state = State::READY;
        _seq_id = 0;

        // Check for deferred shadow shutdown
        // Shadow sessions may defer shutdown until all batches complete
        if (_defer_shadow_shutdown && _batch_queue.empty()) {
            CHECK(_is_shadow);
            reset_session();
            _state = State::RESET_SESSION;
            _send_reset();
            return;
        }

        // Process next batch if available
        _process_next_batch();
    }

    void
    ServerSession::_handle_dependency_ready_for_query(char transaction_status)
    {
        // State must be DEPENDENCIES here
        DCHECK_EQ(_state, State::DEPENDENCIES);

        // If all dependencies are complete, transition to query phase
        if (!_current_batch->are_dependencies_complete()) {
            return;
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2,
                "[S:{}] All dependency ReadyForQuery responses received, transitioning to query phase",
                _id);

        _send_all_messages();
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
        cs->server_msg_response(msg, _id, success);
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
                    LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "[S:{}] Server options key: {}, value: {}", _id, opt_key, opt_value);
                    if (util::is_valid_postgres_key(opt_key) &&
                        !EXCLUDED_STARTUP_PARAMS.contains(opt_key) &&
                        util::is_valid_postgres_value(opt_value)) {
                        query << "SET " << opt_key << "=" << opt_value << ";";
                    }
                }
            } else {
                // regular key value pair
                std::string quoted_value = util::quote_postgres_value(value);
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "[S:{}] Server key: {}, value: {}", _id, key, quoted_value);
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

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "[S:{}] Server session: encode status query: {}", _id, query_str);

        _send_buffer(buffer, _seq_id);

        return true;
    }

    void
    ServerSession::_session_cleanup()
    {
        // grab a shared pointer to self, to avoid losing the reference during cleanup
        ServerSessionPtr self = shared_from_this();

        // on error, the server shuts down the connection and releases the session
        ProxyServer::get_instance()->shutdown_session(self);

        // do the disconnect
        ProxyServer::get_instance()->log_disconnect(self);
        _connection->close();

        // clear all internal data structures
        reset_session();
    }

    void
    ServerSession::shutdown_session()
    {
        // callback from Session::_handle_error()
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Server session is shutting down, socket={}", _id, _connection->get_socket());

        _session_cleanup();

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
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[S:{}] Created connection for server session, to: db={}{}", session->id(), prefix, database);

        ProxyServer::get_instance()->log_connect(session);

        return session;
    }

    void
    ServerSession::send_cancel()
    {
        struct sockaddr_storage addr;
        socklen_t len = sizeof(addr);

        // determine destination address from the existing connection
        _connection->get_peer_address(&addr, &len);
        DCHECK(len != 0) << "Failed to get peer addresss of the server connection";
        if (len == 0) {
            LOG_ERROR( "Failed to get peer addresss of the server connection");
            return;
        }

        // create socket
        int socket_fd = ::socket(addr.ss_family, SOCK_STREAM, 0);
        DCHECK(socket_fd > -1) << "Failed to create a socket";
        if (socket_fd == -1) {
            LOG_ERROR("Failed to creat a socket: error code: {}, error string {}", errno, strerror(errno));
            return;
        }

        // connect to the same destination address
        const struct sockaddr *sock_addr = reinterpret_cast<struct sockaddr *>(&addr);
        int ret = ::connect(socket_fd, sock_addr, len);
        DCHECK(ret != -1) << "Failed to connect to the peer address";
        if (ret == -1) {
            LOG_ERROR("Failed to connect to the peer address: error code: {}, error string {}", errno, strerror(errno));
            ::close(socket_fd);
            return;
        }

        // prepare cancel message
        auto [pid, cancel_key] = _auth->get_pid_cancel_key_pair();
        size_t msg_len = sizeof(uint32_t) * 2 + sizeof(pid) + sizeof(uint8_t) * cancel_key.size();
        char buffer[msg_len];

        sendint32(msg_len, buffer);
        sendint32(MSG_CANCEL, buffer + sizeof(uint32_t));
        sendint32(pid, buffer + 2 * sizeof(uint32_t));
        std::memcpy(buffer + sizeof(uint32_t) * 2 + sizeof(pid), cancel_key.data(), sizeof(uint8_t) * cancel_key.size());

        // send cancel message
        ret = write(socket_fd, buffer, msg_len);
        DCHECK(ret == msg_len) << "Failed to write " << msg_len << " bytes";
        if (ret != msg_len) {
            LOG_ERROR("Failed to write {} bytes: bytes written {}", msg_len, ret);
            if (ret == -1) {
                LOG_ERROR("Failed to write to the socker: error code: {}, error string {}", errno, strerror(errno));
            }
        }

        // close connection
        ::close(socket_fd);
    }
} // namespace springtail::pg_proxy
