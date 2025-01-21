#include <iostream>
#include <sstream>
#include <cassert>

#include <openssl/err.h>

#include <common/logging.hh>

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
#include <proxy/logging.hh>

#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail::pg_proxy {

    ClientSession::ClientSession(ProxyConnectionPtr connection)
        : Session(connection),
          _stmt_cache(STATEMENT_CACHE_SIZE),
          _shadow_mode(ProxyServer::get_instance()->mode() == ProxyServer::MODE::SHADOW),
          _primary_mode(ProxyServer::get_instance()->mode() == ProxyServer::MODE::PRIMARY)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client connected: endpoint={}", _id, connection->endpoint());

        // initialize pid and key for cancellation
        get_random_bytes(reinterpret_cast<uint8_t*>(&_pid), 4);
        // clear top bit to make pid not signed, some historic issue
        _pid &= 0x7FFFFFFF;
        get_random_bytes(reinterpret_cast<uint8_t*>(&_cancel_key), 4);

        // create the client auth object
        _auth = std::make_shared<ClientAuthorization>(connection, _id, _pid, _cancel_key);
    }

    ClientSession::~ClientSession()
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Client session being deallocated");
    }

    void
    ClientSession::run(const std::set<int> &fds)
    {
        // main entry point for client session
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session running", _id);

        // first go through fds and check if we have any pending data
        // first check client session
        if (fds.contains(_connection->get_socket())) {
            // wrap with error handler to catch any exceptions
            _wrap_error_handler([this] {
                _process_connection();
            });
        }

        // check if we have any server sessions
        if (_primary_session && fds.contains(_primary_session->get_connection()->get_socket())) {
            _primary_session->process_connection(_gen_seq_id());
        }

        if (_replica_session && fds.contains(_replica_session->get_connection()->get_socket())) {
            _replica_session->process_connection(_gen_seq_id());
        }

        // re-enable processing
        ProxyServer::get_instance()->signal(shared_from_this());
    }

    void
    ClientSession::_process_connection()
    {
        // entry point for network connection message
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Processing packet, client session: state={:d}", _id, (int8_t)_state);

        // main entry point for thread processing
        // resume from where we left off
        switch(_state) {
            case STARTUP: {
                // startup messages, no auth done yet
                uint64_t seq_id = _gen_seq_id();
                if (_auth->process_auth_data(seq_id)) {
                    // auth done, haven't sent ready for query yet
                    _state = AUTH_SERVER;
                    _db_id = _auth->db_id();
                    _database = _auth->database();
                    _user = _auth->user();
                    _parameters = _auth->parameters();

                    PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session auth done, db={}, user={}", _id, _database, _user->username());
                    _primary_session = _create_server_session(Session::Type::PRIMARY, seq_id);
                }
                break;
            }
            case READY:
                // completed auth ready for queries
                _handle_request();
                break;

            case AUTH_SERVER:
                // waiting for server auth to complete,
                // completion comes through server_auth_done() call
                break;

            default:
                SPDLOG_ERROR("Invalid state: {}", (int8_t)_state);
                _state = ERROR;
                break;
        }
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
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client session got ready from server session: status={}",
                    _id, xact_status);

        // check if we are in/still in a transaction
        if (xact_status == 'I') {
            _in_transaction = false;

            // clear associated session
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Clearing associate server session", _id);
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
    ClientSession::_handle_request()
    {
        BufferList blist;
        Session::read_msg(_connection, blist);

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client handle request: buffers={}", _id, blist.buffers.size());
        int i = 0;
        for (auto buffer: blist.buffers) {
            char code = buffer->get();
            int32_t len = buffer->get32();
            uint64_t seq_id = _gen_seq_id();

            PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Client, buf={}, got request code: {}, seq_id: {}", _id, i++, code, seq_id);

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
                SPDLOG_ERROR("Terminate request");
                _state = ERROR;
                return;

            case 'F': // function call
                _handle_function_call(buffer, seq_id);
                break;

            case 'S': // sync
                _handle_sync(buffer, seq_id);
                break;

            case 'c': // copy done
            case 'd': // copy data
                if (get_associated_session() == nullptr) {
                    SPDLOG_WARN("[C:{}] No associated server session", _id);
                    // a c/d could come after an error message after ready for query
                    // in this case we drop it.
                    break;
                }
                // fall through if we have an associated session
                // to forward the message
            case 'H':   // flush
            case 'f': { // copy fail
                // forward to server, should have associated server session
                PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Forwarding to server: code={}, len={}", _id, code, len);
                auto session = get_associated_session();
                CHECK_NE(session, nullptr);
                _send_to_remote_session(code, buffer, seq_id);
                break;
            }
            default:
                SPDLOG_ERROR("Unsupported request code: {}", code);
                throw ProxyMessageError();
            }
        }
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
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query, [this](const std::string &schema, const std::string &table) {
            return DatabaseMgr::get_instance()->is_table_replicated(this->_db_id, this->_default_schema, schema, table);
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
        ServerSessionPtr session = _get_associated_session();
        if (session == nullptr) {
            // this is a weird case as it doesn't make sense to issue
            // a sync without a set of other extended queries preceeding it
            // but we'll handle it anyway, just issue a sync to the server
            session = _select_session(REPLICA, seq_id);
        }

        session->process_msg(SessionMsg::create(SessionMsg::MSG_CLIENT_SERVER_SYNC, buffer, seq_id));
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
        ServerSessionPtr server_session = std::static_pointer_cast<ServerSession>(session);

        // explicit session send, not usual
        if (server_session) {
            server_session->process_msg(msg);
            return;
        }

        // session == nullptr, select a session based on the message type

        if (_primary_mode) {
            // send to primary session force to !readonly
            is_readonly = false;
        }

        // not in shadow mode or not readonly, send to single server
        if (!_shadow_mode || !is_readonly) {
            // select a server session and notify it of this message
            server_session = _select_session(is_readonly ? REPLICA : PRIMARY, msg->seq_id());
            server_session->process_msg(msg);
            return;
        }

        // both shadow mode and readonly; we send to both primary and replica
        assert(_shadow_mode && is_readonly);

        // make sure to send to primary first; so get PRIMARY session
        server_session = _select_session(PRIMARY, msg->seq_id());
        server_session->process_msg(msg);

        // then clone msg and get a replica session
        // don't use _select_session() as it uses/sets the associated session
        msg = msg->clone();
        server_session = nullptr;
        if (_replica_session != nullptr) {
            // have a replica session use it
            server_session = _replica_session;
        } else {
            // create a new replica session; shouldn't be common to get here
            server_session = _create_server_session(REPLICA, msg->seq_id());
        }

        assert (server_session != nullptr);
        server_session->process_msg(msg);

        return;
    }

    ServerSessionPtr
    ClientSession::_select_session(Session::Type type, uint64_t seq_id)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Selecting server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");

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
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Using associated session: id={}", _id, session->id());
            return session;
        }

        ServerSessionPtr session = nullptr;

        if (type == PRIMARY && _primary_session != nullptr) {
            // use primary session
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Using primary session; setting associated session", _id);
            session = _primary_session;
            set_associated_session(session);
            return session;
        }

        if (type == REPLICA && _replica_session != nullptr) {
            // use replica session
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Using replica session; setting associated session", _id);
            session = _replica_session;
            assert (!_shadow_mode);
            set_associated_session(session);
            return session;
        }

        assert(!_shadow_mode || type == PRIMARY);

        //// Shouldn't get here in common case; only if we need to allocate a new session
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Creating new server session: type={}", _id, type == PRIMARY ? "PRIMARY" : "REPLICA");
        session = _create_server_session(type, seq_id);
        assert (session != nullptr);
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[C:{}] Created new server session: id={}", _id, session->id());

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
        ServerSessionPtr session = db_mgr->get_pooled_session(type, _db_id, _user->username());

        if (session == nullptr) {
            // need to allocate a new session
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[C:{}] Allocating new server session: {}:{}", _id, _database, _user->username());

            from_pool = false;

            if ((session = db_mgr->allocate_session(type, _db_id, _user, _parameters)) == nullptr) {
                SPDLOG_ERROR("Failed to allocate server session for user {}, database {}", _user->username(), _database);
                assert(0);
                return nullptr;
            }
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
            session->set_shadow_mode(_shadow_mode);
        }
        session->pin_client_session(shared_from_this());

        // TODO: it is possible that the session got into an error state
        // during allocation or just after prior to being pinned
        // we should check for this and handle it

        // register server session connection with server
        ProxyServer::get_instance()->register_session(shared_from_this(), session->get_connection()->get_socket());

        if (from_pool) {
            // if session is ready and clean, we can use it
            auto &&parameters = _auth->parameters();
            if (session->check_startup_params(parameters)) {
                session->set_ready();
            } else {
                // apply parameters to session if they don't match
                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[C:{}] Applying session parameters to server session: id={}", _id, session->id());
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
        std::vector<Parser::StmtContextPtr> &&parse_contexts = Parser::parse_query(query, [this](const std::string &schema, const std::string &table) {
            return DatabaseMgr::get_instance()->is_table_replicated(this->_db_id, this->_default_schema, schema, table);
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

    void
    ClientSession::shutdown_server_sessions()
    {
        uint64_t seq_id = _gen_seq_id();
        if (_primary_session != nullptr) {
            _primary_session->process_shutdown(seq_id);
        }

        if (_replica_session != nullptr) {
            _replica_session->process_shutdown(seq_id);
        }
    }

} // namespace springtail::pg_proxy
