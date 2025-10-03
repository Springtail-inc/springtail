#include <cassert>

#include <openssl/err.h>

#include <common/logging.hh>
#include <common/common.hh>
#include <common/constants.hh>

#include <pg_repl/pg_types.hh>

#include <proxy/server_session.hh>
#include <proxy/client_session.hh>
#include <proxy/server_session.hh>
#include <proxy/database.hh>
#include <proxy/user_mgr.hh>
#include <proxy/errors.hh>
#include <proxy/server.hh>
#include <proxy/exception.hh>
#include <proxy/parser.hh>
#include <proxy/buffer_pool.hh>

namespace springtail::pg_proxy {

    /** unique session id counter */
    static std::atomic<uint32_t> process_id(1);

    std::unordered_map<
        int32_t,
        std::pair<std::vector<uint8_t>, std::shared_ptr<ClientSession>>
    > ClientSession::_cancel_map{};

    ClientSession::ClientSession(ProxyConnectionPtr connection)
        : Session(connection),
          _stmt_cache(STATEMENT_CACHE_SIZE),
          _shadow_mode(ProxyServer::get_instance()->mode() == ProxyServer::MODE::SHADOW),
          _primary_mode(ProxyServer::get_instance()->mode() == ProxyServer::MODE::PRIMARY)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client connected: endpoint={}", _id, connection->endpoint());

        // NOTE: the assumption here is that by the time we wrap around,
        //  the earliest sessions would be long gone
        _pid = process_id;
        process_id = (process_id + 1) & 0x7FFFFFFF;

        // generate random cancel key
        _cancel_key.resize(sizeof(uint32_t));
        get_random_bytes(_cancel_key.data(), sizeof(uint32_t));

        // create the client auth object
        _auth = std::make_shared<ClientAuthorization>(connection, _id, _pid, _cancel_key);
        _start_time = common::get_time_in_millis();
    }

    ClientSession::~ClientSession()
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Client session being deallocated");
    }

    void
    ClientSession::run(std::set<int> &fds)
    {
        // main entry point for client session
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client session running", _id);

        // wrap with error handler to catch any exceptions
        _wrap_error_handler([this, &fds] {
            do {
                // go through fds and check if we have any pending data
                // first check client session
                if (fds.contains(_connection->get_socket())) {
                    _process_connection();
                }

                // check if we have any server sessions
                if (_state != ERROR && _primary_session && fds.contains(_primary_session->get_connection()->get_socket())) {
                    _primary_session->process_connection(_gen_seq_id());
                }

                if (_state != ERROR && _replica_session && fds.contains(_replica_session->get_connection()->get_socket())) {
                    _replica_session->process_connection(_gen_seq_id());
                }

                fds.clear();

            } while ((_state != ERROR) && !is_shutdown() && _has_pending_data(fds));
        });

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "[C:{}] Client session done", _id);
    }

    bool
    ClientSession::_has_pending_data(std::set<int> &fds) const
    {
        std::vector<ProxyConnectionPtr> connections;
        connections.push_back(_connection);

        if (_primary_session) {
            connections.push_back(_primary_session->get_connection());
        }

        if (_replica_session) {
            connections.push_back(_replica_session->get_connection());

        }

        return ProxyConnection::has_pending(connections, fds);
    }

    void
    ClientSession::_process_connection()
    {
        // entry point for network connection message
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Processing packet, client session: state={:d}", _id, (int8_t)_state);

        // main entry point for thread processing
        // resume from where we left off
        switch(_state) {
            case STARTUP:
                // startup messages, no auth done yet
                _handle_auth();
                break;

            case READY:
                // completed auth ready for queries
                _handle_request();
                break;

            case AUTH_SERVER:
                // waiting for server auth to complete,
                // completion comes through server_auth_done() call
                break;

            default:
                LOG_ERROR("Invalid state: {}", (int8_t)_state);
                _state = ERROR;
                break;
        }
    }

    void
    ClientSession::_handle_auth()
    {
        // startup messages, no auth done yet
        uint64_t seq_id = _gen_seq_id();
        bool auth_done = false;

        try {
            // process the auth data, it may throw an exception, or just set _state to ERROR
            auth_done = _auth->process_auth_data(seq_id);

            // check for timeout if auth not done
            if (!auth_done && common::get_time_in_millis() - _start_time > AUTH_TIMEOUT_MS) {
                // auth timed out
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client session auth timeout", _id);
                throw ProxyAuthError();
            }

        } catch (ProxyAuthError &e) {
            // print backtrace
            LOG_ERROR("[C:{}] Client session auth error: {}", _id, e.what());
            e.log_backtrace();
            _state = ERROR;
        }

        if (_auth->is_cancel()) {
            auto pid_cancel_key_pair = _auth->get_pid_cancel_key_pair();

            // 1. Find ClientSession with the given pid and cancel_key
            auto it = _cancel_map.find(pid_cancel_key_pair.first);
            if (it != _cancel_map.end()) {

                // 2. Request cancel on this client session
                const std::vector<uint8_t>& cancel_key = pid_cancel_key_pair.second;
                it->second.second->_handle_cancel(pid_cancel_key_pair.first, cancel_key);
            } else {
                LOG_ERROR("[C:{}] Client session for process id {} is not found", _id, pid_cancel_key_pair.first);
            }

            // 3. Terminate current connection
            _connection->close();
            return;
        }

        if (_state == ERROR) {
            // auth failed, handle the error
            std::string error_code = _auth->get_error_code();
            if (error_code.empty()) {
                error_code = ProxyProtoError::INVALID_PASSWORD;
            }

            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client session auth failed: {}", _id, error_code);

            // encode and send error message
            BufferPtr buffer = BufferPool::get_instance()->get(128);
            ProxyProtoError::encode_error(buffer, error_code, "authentication failed", "FATAL");
            _send_buffer(buffer, seq_id);

            // throw the error
            throw ProxyAuthError();
        }

        if (auth_done) {
            _cancel_map.emplace(_pid, std::make_pair(_cancel_key, shared_from_this()));

            // auth done, haven't sent ready for query yet
            // need to finish server authentication
            _state = AUTH_SERVER;
            _db_id = _auth->db_id();
            _database = _auth->database();
            _user = _auth->user();
            _parameters = _auth->parameters();

            if (_db_id == constant::INVALID_DB_ID) {
                // not connecting to a replicated database, force primary only mode
                LOG_INFO("[C:{}] Client session forcing primary mode", _id);
                _primary_mode = true;
            }

            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client session auth done, db={}, user={}", _id, _database, _user->username());
            _primary_session = _create_server_session(Session::Type::PRIMARY, seq_id);
        }
    }


    void
    ClientSession::_handle_cancel(int pid, const std::vector<uint8_t> &cancel_key)
    {
        DCHECK(pid == _pid) << "Process id does not match";
        if (cancel_key != _cancel_key) {
            LOG_WARN("Invalid cancel key for cancel request");
            return;
        }

        if (_primary_session != nullptr) {
            // send cancel
            _primary_session->send_cancel();
        }
        if (_replica_session != nullptr) {
            // send cancel
            _replica_session->send_cancel();
        }
    }

    void
    ClientSession::server_auth_error(ServerSessionPtr session,
                                     uint64_t seq_id,
                                     const std::string &error_code,
                                     const std::string &error_message)
    {
        // if primary create login error
        if (session->type() != Session::Type::PRIMARY) {
            return;
        }

        // client session only waits for the primary auth to be done
        // if we get an error here, we should send it to the client
        BufferPtr buffer = BufferPool::get_instance()->get(1024);
        ProxyProtoError::encode_error(buffer, error_code, error_message, "FATAL");
        _send_buffer(buffer, seq_id);
    }

    void
    ClientSession::server_auth_done(ServerSessionPtr session,
                                    const std::unordered_map<std::string, std::string> &parameters)
    {
        if (_state != AUTH_SERVER) {
            DCHECK_EQ(_state, READY);
            return;
        }

        // this is the primary server session
        CHECK_EQ(session->type(), Session::Type::PRIMARY);
        _state = READY;
        _auth->send_auth_done(_gen_seq_id(), parameters);
    }

    void
    ClientSession::server_msg_response(SessionMsgPtr msg, bool success)
    {
        // update statement cache with msg completion
        _stmt_cache.commit_statement(msg->data(), msg->completed(), success);
    }

    void
    ClientSession::server_ready_msg(char xact_status)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client session got ready from server session: status={}",
                    _id, xact_status);

        // check if we are in/still in a transaction
        if (xact_status == 'I') {
            _in_transaction = false;

            // clear associated session
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Clearing associate server session", _id);
            clear_associated_session();
        } else {
            CHECK(xact_status == 'E' || xact_status == 'T');
            // either 'E' or 'T' -- error requiring rollback or in transaction.
            // could track transaction error state and avoid server round trips
            // until we get a rollback...
            _in_transaction = true;
        }

        _stmt_cache.sync_transaction(xact_status);
    }

    void
    ClientSession::server_shutdown(ServerSessionPtr session)
    {
        // server session is shutting down
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Server session shutting down", _id);

        if (session->type() == Session::Type::PRIMARY) {
            // primary session is shutting down
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Primary session shutting down", _id);
            _primary_session = nullptr;
        } else {
            // replica session is shutting down
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Replica session shutting down", _id);
            _replica_session = nullptr;

            // XXX need to failover to new replica
            if (_shadow_mode) {
                // for now ignore the replica failure, we will get a new one
                // XXX we should really replay any session state on startup...
                return;
            }
        }

        // XXX right now can't handle this
        if (get_associated_session()) {
            clear_associated_session();
        }

        _state = ERROR;
    }


    void
    ClientSession::shutdown_session(void)
    {
        // grab a shared pointer to self, to avoid losing the reference during cleanup
        ClientSessionPtr self = shared_from_this();

        // Callback from Session::_handle_error()
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Client session shutting down", _id);

        // first close connection and remove from server poll list
        // this removes all associated sockets from this session
        ProxyServer::get_instance()->log_disconnect(self);
        _connection->close();
        ProxyServer::get_instance()->shutdown_session(self);

        // notify server replica/primary sessions via shutdown_server_sessions()
        uint64_t seq_id = _gen_seq_id();
        if (_primary_session != nullptr) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Client primary use count: {}", _id, _primary_session.use_count());
            _primary_session->process_shutdown(seq_id);
            _primary_session = nullptr;
        }

        if (_replica_session != nullptr) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Client replica use count: {}", _id, _primary_session.use_count());
            _replica_session->process_shutdown(seq_id);
            _replica_session = nullptr;
        }

        // clean up cancel map
        _cancel_map.erase(_pid);

        // clear all internal data structures; clears associated session
        reset_session();
    }


    void
    ClientSession::_handle_request()
    {
        BufferList blist;
        Session::read_msg(_connection, blist);

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client handle request: buffers={}", _id, blist.buffers.size());

        _msg_queue.clear();

        // iterate through message buffers
        [[maybe_unused]] int i = 0;

        for (auto &buffer: blist.buffers) {
            char code = buffer->get();
            int32_t len = buffer->get32();
            uint64_t seq_id = _gen_seq_id();

            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Client, buf={}, got request code: {}, seq_id: {}", _id, i++, code, seq_id);

            // log buffer, skipping the header
            _log_buffer(true, code, len, buffer->current_data(), seq_id);

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
                LOG_INFO("Terminate request");
                _state = ERROR;
                return;

            case 'F': // function call
                _handle_function_call(buffer, seq_id);
                break;

            case 'S': // sync
                _handle_sync(buffer, seq_id);
                break;

            case 'c':   // copy done
            case 'd':   // copy data
            case 'f': { // copy fail
                if (get_associated_session() == nullptr) {
                    LOG_WARN("[C:{}] No associated server session", _id);
                    // a c/d could come after an error message after ready for query
                    // in this case we drop it.
                    break;
                }

                // forward to server, should have associated server session
                // and associated session should be primary since this is copy data
                ServerSessionPtr session = std::static_pointer_cast<ServerSession>(get_associated_session());
                CHECK_EQ(session->type(), Session::Type::PRIMARY);

                // NOTE: normally we'd queue the message and send it as a batch, however
                // this data is expected out of band and it will never be sent to a replica
                // since it is copy in data.  It can be sent directly to the primary
                // bypassing the batch queue.  These messages should only be sent by the
                // client after the server has sent a copy in response 'G'.

                // forward message bypassing the batch queue
                DCHECK(_msg_queue.empty());
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Forwarding to server: code={}, len={}", _id, code, len);
                session->process_msg(SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_FORWARD, buffer, seq_id));
                break;
            }

            case 'H':   // flush (extended protocol)
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Forwarding to server: code={}, len={}", _id, code, len);
                _queue_msg(SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_FORWARD, buffer, seq_id));
                break;

            default:
                LOG_ERROR("Unsupported request code: {}", code);
                throw ProxyMessageError();
            }
        }

        // go through msg queue and send batch to server
        _send_msg_queue();
    }

    void
    ClientSession::_send_msg_queue()
    {
        // send batch msg queue to server
        if (_msg_queue.empty()) {
            return;
        }

        bool is_read_safe = _primary_mode ? false : true;

        // go through all messages and check that they are all read-safe
        if (!_primary_mode) {
            for (auto &msg: _msg_queue) {
                if (!msg->is_read_safe()) {
                    is_read_safe = false;
                    break;
                }
            }
        }

        ServerSessionPtr server_session;
        uint64_t seq_id = _msg_queue.front()->seq_id();

        // not in shadow mode or not readonly, send to single server
        if (!_shadow_mode || !is_read_safe) {
            // select a server session and notify it of this message
            server_session = _select_session(is_read_safe ? REPLICA : PRIMARY, seq_id);
            server_session->queue_msg_batch(std::move(_msg_queue));
            _msg_queue.clear();
            return;
        }

        // both shadow mode and readonly; we send to both primary and replica
        CHECK(_shadow_mode && is_read_safe);

        // make sure to send to primary first; so get PRIMARY session
        server_session = _select_session(PRIMARY, seq_id);

        // clone the message queue
        std::deque<SessionMsgPtr> clone_queue;
        for (auto &msg: _msg_queue) {
            clone_queue.push_back(msg->clone());
        }

        server_session->queue_msg_batch(std::move(_msg_queue));
        _msg_queue.clear();

        // get a replica session
        // don't use _select_session() as it uses/sets the associated session
        server_session = nullptr;
        if (_replica_session != nullptr) {
            // have a replica session use it
            server_session = _replica_session;
        } else {
            // create a new replica session; shouldn't be common to get here
            server_session = _create_server_session(REPLICA, seq_id);
        }

        DCHECK(server_session != nullptr);
        server_session->queue_msg_batch(std::move(clone_queue));

        return;
    }

    void
    ClientSession::_handle_function_call(BufferPtr buffer, uint64_t seq_id)
    {
        // doc's state that this should really be deprecated and not used
        // instead clients should use a prepared statement

        // send to primary server session
        QueryStmtPtr qs = std::make_shared<QueryStmt>(QueryStmt::FUNCTION, buffer, false);

        SessionMsgPtr msg = SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_FUNCTION, qs, seq_id);

        // select a server session and queue message
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_parse(BufferPtr buffer, uint64_t seq_id)
    {
        // PARSE packet request, create prepared statement
        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        // query string
        std::string_view query = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Parse: stmt={}, query={}", _id, stmt, query);

        // parse the query
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query, [this](const std::string &schema, const std::string &table) {
            return DatabaseMgr::get_instance()->is_table_replicated(this->_db_id, schema, table);
        });

        // Create a query statement object
        QueryStmt::Type qs_type = parse_contexts.empty() ? QueryStmt::Type::PREPARE : _remap_parse_type(parse_contexts[0]);
        bool is_read_safe = parse_contexts.empty() ? true : parse_contexts[0]->is_read_safe;
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
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_bind(BufferPtr buffer, uint64_t seq_id)
    {
        // BIND packet request, bind a prepared statement to a portal
        // portal string
        std::string_view portal = buffer->get_string();

        // statement string -- prepared name
        std::string_view stmt = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Bind: prepared={}, portal={}", _id, stmt, portal);

        // get the prepared statement from the cache
        std::pair<QueryStmtPtr, bool> lookup_result = _stmt_cache.lookup_prepared(stmt);
        QueryStmtPtr prepared_stmt = lookup_result.first;
        if (prepared_stmt == nullptr) {
            LOG_ERROR("Prepared statement not found: {}", stmt);
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
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_describe(BufferPtr buffer, uint64_t seq_id)
    {
        // DESCRIBE packet request, get row details for a prepared statement or portal
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement name
        std::string_view name = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Describe request: type={}, name={}", _id, stmt_type, name);

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
            LOG_ERROR("Statement not found: {}", name);
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
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_execute(BufferPtr buffer, uint64_t seq_id)
    {
        // EXECUTE packet request, execute portal
        // portal name
        std::string_view name = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Execute request: name={}", _id, name);

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
            LOG_ERROR("Portal not found: {}", name);
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
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_close(BufferPtr buffer, uint64_t seq_id)
    {
        // CLOSE packet, close prepared statement or portal
        // type: S - statement, P - portal
        char stmt_type = buffer->get();

        // portal or statement
        std::string_view name = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Close request: type={}, name={}", _id, stmt_type, name);

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
                LOG_ERROR("Statement not found: {}", name);
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
        _queue_msg(msg);
    }

    void
    ClientSession::_handle_sync(BufferPtr buffer, uint64_t seq_id)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Sync request", _id);
        QueryStmtPtr qs = std::make_shared<QueryStmt>(QueryStmt::SYNC, buffer, true);

        _queue_msg(SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SYNC, qs, seq_id));
    }

    void
    ClientSession::_handle_simple_query(BufferPtr buffer, uint64_t seq_id)
    {
        std::string_view query = buffer->get_string();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Simple Query: {}", _id, query);

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
        _queue_msg(msg);
    }


    ServerSessionPtr
    ClientSession::_select_session(Session::Type type, uint64_t seq_id)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Selecting server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");

        if (_primary_mode) {
            // force primary mode
            type = PRIMARY;
        }

        if (type == REPLICA && !DatabaseMgr::get_instance()->is_database_ready(_db_id)) {
            type = PRIMARY;
        }

        // if we have an associated session use it (typically in a transaction)
        if (get_associated_session() != nullptr) {
            if (type == PRIMARY && type != associated_session_type()) {
                // TODO: handle change of associated session type
            }
            ServerSessionPtr session =  std::static_pointer_cast<ServerSession>(get_associated_session());
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Using associated session: id={}", _id, session->id());
            return session;
        }

        ServerSessionPtr session = nullptr;

        if (type == PRIMARY && _primary_session != nullptr) {
            // use primary session
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Using primary session; setting associated session", _id);
            session = _primary_session;
            set_associated_session(session);
            return session;
        }

        if (type == REPLICA && _replica_session != nullptr) {
            // use replica session
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Using replica session; setting associated session", _id);
            session = _replica_session;
            CHECK(!_shadow_mode);
            set_associated_session(session);
            return session;
        }

        CHECK(!_shadow_mode || type == PRIMARY);

        //// Shouldn't get here in common case; only if we need to allocate a new session
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Creating new server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");
        session = _create_server_session(type, seq_id);
        DCHECK_NE(session, nullptr);
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Created new server session: id={}", _id, session->id());

        // set associated session
        set_associated_session(session);

        return session;
    }

    ServerSessionPtr
    ClientSession::_create_server_session(Session::Type type, uint64_t seq_id)
    {
        DatabaseMgr *db_mgr = DatabaseMgr::get_instance();

        // get a session from the pool or allocate a new one
        bool from_pool = true;
        ServerSessionPtr session = nullptr;

        if (_db_id != constant::INVALID_DB_ID) {
            // currently don't use pooling for non-replicated databases
            session = db_mgr->get_pooled_session(type, _db_id, _user->username());
        } else {
            // with no db_id we must be in primary mode
            CHECK_EQ(type, PRIMARY);
        }

        if (session == nullptr) {
            // need to allocate a new session
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[C:{}] Allocating new server session: {}:{}", _id, _database, _user->username());

            from_pool = false;

            if ((session = db_mgr->allocate_session(type, _db_id, _user, _parameters, _database)) == nullptr) {
                LOG_ERROR("Failed to allocate server session for user {}, database {}", _user->username(), _database);
                assert(0);
                return nullptr;
            }
        }

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "[C:{}] Got server session: id={}, is_ready={}", _id, session->id(), session->is_ready());

        if (type == PRIMARY) {
            // store reference to primary session
            _primary_session = session;
        } else {
            // store reference to replica session
            _replica_session = session;

            // if client is in shadow mode, then the replica session
            // becomes a shadow session, not returning results to the client
            session->set_shadow_mode(_shadow_mode);
        }
        session->pin_client_session(shared_from_this());

        // TODO: it is possible that the session got into an error state
        // during allocation or just after prior to being pinned
        // we should check for this and handle it

        // first unregister server session and then register connection with server
        ProxyServer::get_instance()->register_session(shared_from_this(), session, session->get_connection()->get_socket());

        if (from_pool) {
            // if session is ready and clean, we can use it
            auto &&parameters = _auth->parameters();
            if (session->check_startup_params(parameters)) {
                session->set_ready_reset_done();
            } else {
                // apply parameters to session if they don't match
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[C:{}] Applying session parameters to server session: id={}", _id, session->id());
                session->startup_reset_session(seq_id, parameters);
            }

            return session;
        }

        // this is a newly allocated session, we need to start it up
        // we need to do authentication and wait for session to become ready

        // startup the server session
        session->startup(seq_id);

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

            case Parser::StmtContext::Type::SAVEPOINT_STMT:
                return QueryStmt::SAVEPOINT;

            case Parser::StmtContext::Type::ROLLBACK_TO_SAVEPOINT_STMT:
                return QueryStmt::ROLLBACK_TO_SAVEPOINT;

            case Parser::StmtContext::Type::RELEASE_SAVEPOINT_STMT:
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
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query, [this](const std::string &schema, const std::string &table) {
            return DatabaseMgr::get_instance()->is_table_replicated(this->_db_id, schema, table);
        });

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
