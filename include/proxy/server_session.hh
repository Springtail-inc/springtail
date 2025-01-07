#pragma once

#include <memory>
#include <queue>
#include <set>
#include <unordered_map>

#include <proxy/session.hh>
#include <proxy/client_session.hh>
#include <proxy/session_msg.hh>
#include <proxy/user_mgr.hh>
#include <proxy/buffer_pool.hh>

namespace springtail::pg_proxy {

    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class DatabaseInstance;
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;

    /**
     * @brief Internal state for a query that is being processed.
     * A query may have multiple dependencies, and may have multiple
     * simple queries. This object tracks the completion state of the query.
     */
    struct QueryStatus {
        int query_count = 0;               ///< number of (simple) queries
        int query_complete_count = 0;      ///< number of (simple) queries completed
        int dependency_count = 0;          ///< number of dependencies
        int dependency_complete_count = 0; ///< number of dependencies completed
        bool simple_query_dependency = false; ///< at least one simple query dependency
        SessionMsgPtr msg;                 ///< parent message being processed

        QueryStatus(SessionMsgPtr msg)
            : msg(msg)
        {}
    };
    using QueryStatusPtr = std::shared_ptr<QueryStatus>;

    /**
     * @brief Server session object.
     * This object represents a session with a remote database.
     * The database may be either the primary or a replica
     */
    class ServerSession : public Session {
    public:
        ServerSession(const ServerSession&) = delete;
        ServerSession& operator=(const ServerSession&) = delete;

        ServerSession(ProxyConnectionPtr connection,
                      ProxyServerPtr server,
                      UserPtr user,
                      std::string database,
                      std::string prefix,
                      DatabaseInstancePtr instance,
                      const std::unordered_map<std::string, std::string> &parameters,
                      Session::Type type=PRIMARY);

        /** For test purposes */
        ServerSession(Session::Type type,
                      uint64_t id,
                      uint64_t db_id,
                      const std::string &database,
                      const std::string &username)
            : Session(type, id, db_id, database, username)
        {}

        ~ServerSession() {};

        /**
         * @brief Pin this session to a client session
         * @return true if pinned
         */
        bool is_pinned() const {
            return _is_pinned;
        }

        /**
         * @brief Pin this session to a client session
         * @param client_session client session to pin
         */
        void pin_client_session(std::weak_ptr<ClientSession> client_session) {
            _client_session = client_session;
            _is_pinned = true;
        }

        /**
         * @brief Get the client session object
         * @return SessionPtr
         */
        SessionPtr get_client_session() {
            SessionPtr client = _client_session.lock();
            if (client == nullptr) {
                client = get_associated_session();
            }
            return client;
        }

        /**
         * @brief Shutdown the client session
         */
        void shutdown_client_session()
        {
            SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR);
            SessionPtr client = _client_session.lock();
            if (client == nullptr) {
                client = get_associated_session();
                if (client == nullptr) {
                    return;
                }
            }

            client->queue_shutdown_msg(msg);
        }

        /**
         * @brief Get shared pointer to this server session
         * @return std::shared_ptr<ServerSession> shared pointer to this server session
         */
        std::shared_ptr<ServerSession> shared_from_this() {
            return std::static_pointer_cast<ServerSession>(Session::shared_from_this());
        }

        /**
         * @brief Helper to release the session
         * @param deallocate if true it will deallocate the session and not add to the pool
         */
        void release_session(bool deallocate);

        /**
         * @brief Reset the session, override base session (and call it)
         */
        void reset_session() override {
            _is_pinned = false;
            _client_session.reset();
            _stmts.clear();
            while (!_pending_queue.empty()) {
                _pending_queue.pop();
            }
            // reset base session
            Session::reset_session();
        }

        /**
         * @brief Check if the session's startup params match the client's
         * @param parameters client parameters
         * @return true if match
         * @return false if not a match
         */
        bool check_startup_params(const std::unordered_map<std::string, std::string> &parameters);

        /**
         * @brief Set the session to ready state
         */
        void set_ready() {
            assert (_state == RESET_SESSION_READY);
            _state = READY;
        }

        /** factory to create session */
        static std::shared_ptr<ServerSession>
        create(ProxyServerPtr server, UserPtr user, const std::string &database,
               const std::string &prefix,
               DatabaseInstancePtr instance,
               Session::Type type,
               const std::unordered_map<std::string, std::string> &parameters);

    protected:
        void _process_connection() override;

        void _process_msg(SessionMsgPtr msg) override;

        bool _is_pinned = false;

        std::weak_ptr<ClientSession> _client_session; ///< client session

        // message state for current client query (for state=QUERY)
        std::queue<QueryStatusPtr> _pending_queue; ///< queue of pending messages

        std::set<std::string> _stmts;              ///< completed prepared statement ids
        std::string _db_prefix;                    ///< database name prefix to be used for this server session

        /** Send startup message */
        void _send_startup_msg(uint64_t seq_id);

        /** Initial setup, SSL negotiation */
        void _send_ssl_req(uint64_t seq_id);

        /** Send SSL handshake */
        void _send_ssl_handshake(uint64_t seq_id);

        /** Wrapper around sending a buffer to the server */
        void _send_buffer(BufferPtr buffer, uint64_t seq_id, char code='\0');

        /** Send shutdown to server */
        void _send_shutdown();

        /** Send reset query cmds to reset session */
        void _send_reset();

        /**
         * Send required statements to fulfil dependency
         * @param dependency dependency to fulfil
         */
        void _send_dependency(const QueryStmtPtr dependency, uint64_t seq_id);

        void _send_server_msg(QueryStatusPtr query_status);

        /** Send simple query */
        void _send_simple_query(const std::string &query, uint64_t seq_id);

        /** Handle SSL handshake */
        void _handle_ssl_handshake(uint64_t seq_id);

        /** Handle ssl response */
        void _handle_ssl_response();

        /** Authentication */
        void _handle_auth(BufferPtr buffer);

        /** Handle replies from server */
        void _handle_message_from_server();

        /** Handle error code 'E' */
        void _handle_error_code(BufferPtr buffer, uint64_t seq_id);

        /** Handle md5 auth send response */
        void _handle_auth_md5(BufferPtr buffer);

        /** Handle scram-sha-256 auth send response */
        void _handle_auth_scram(BufferPtr buffer);

        /** Handle scram continue auth send response */
        void _handle_auth_scram_continue(BufferPtr buffer);

        /** Handle scram complete, update client key */
        void _handle_auth_scram_complete(BufferPtr buffer);

        /** Handle forwarded message, first replay dependencies */
        void _handle_msg_to_server(SessionMsgPtr msg);

        /** Handle dependency response; true if error */
        void _handle_dependency_response(bool error);

        /** Handle query response */
        void _handle_query_response();

        /** Handle query error */
        void _handle_query_error();

        /** Handle ready for query message from server */
        void _handle_ready_for_query_response(char xact_status);

        /** Handle shutdown message from client */
        void _handle_shutdown();

        /** Process response from server in reset session state */
        void _handle_reset_session_message();

        /** Helper to read in message */
        BufferPtr _read_message(int msg_length);
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
    using ServerSessionWeakPtr = std::weak_ptr<ServerSession>;
} // namespace springtail::pg_proxy
