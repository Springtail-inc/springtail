#pragma once

#include <proxy/session.hh>
#include <proxy/session_msg.hh>
#include <proxy/parser.hh>
#include <proxy/authorization.hh>

namespace springtail::pg_proxy {

    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class ServerSession;
    using ServerSessionPtr = std::shared_ptr<ServerSession>;

    class ClientSession : public Session
    {
    public:
        /** Prepared statement cache size */
        constexpr static int STATEMENT_CACHE_SIZE = 100;

        /** Default timeout for authentication */
        constexpr static int AUTH_TIMEOUT_MS = 60*1000; // 60 seconds

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /** Construct a connection with the given socket. */
        explicit ClientSession(ProxyConnectionPtr connection);

        ~ClientSession();

        /** Entry point from runnable */
        void run(std::set<int> &fds) override;

        /** Runnable name */
        std::string name() const override {
            return fmt::format("Client[{}]", _id);
        }

        /** Callback from Session::_handle_error() to shutdown the session */
        void shutdown_session() override;

        /**
         * @brief Is client session in shadow mode: sending to primary and replica,
         * but not only logging replies from replica
         * @return true if in shadow mode
         * @return false if not in shadow mode
         */
        bool is_shadow_mode() const {
            return _shadow_mode;
        }

        /**
         * @brief Is client session in primary mode: sending to primary only
         * @return true if in primary mode
         * @return false if not in primary mode
         */
        bool is_primary_mode() const {
            return _primary_mode;
        }

        /**
         * @brief Get the shadow session
         * @return ServerSessionPtr shadow session
         */
        ServerSessionPtr get_shadow_session() const {
            if (_shadow_mode && _replica_session != nullptr) {
                return _replica_session;
            }
            return nullptr;
        }

        /**
         * @brief Get the primary session
         * @return ServerSessionPtr primary session
         */
        ServerSessionPtr get_primary_session() const {
            return _primary_session;
        }

        /**
         * @brief Get the replica session
         * @return ServerSessionPtr replica session
         */
        ServerSessionPtr get_replica_session() const {
            return _replica_session;
        }

        ServerSessionPtr get_pending_replica_session() const {
            return _pending_replica_session;
        }

        /**
         * @brief Get a shared pointer to this client session
         * @return std::shared_ptr<ClientSession> shared pointer to this client session
         */
        std::shared_ptr<ClientSession> shared_from_this() {
            return std::static_pointer_cast<ClientSession>(Session::shared_from_this());
        }

        /**
         * @brief Callback from Server indicating that authentication is done
         * @param session server session
         * @param parameters parameters from server
         */
        void server_auth_done(ServerSessionPtr session, const std::unordered_map<std::string, std::string> &parameters);

        /**
         * @brief Callback from Server indicating that a message is ready
         * @param session server session
         * @param seq_id sequence id
         * @param error_code error code
         * @param error_message error message
         */
        void server_auth_error(ServerSessionPtr session, uint64_t seq_id, const std::string &error_code, const std::string &error_message);

        /**
         * @brief Callback from Server indicating that a message is ready
         * @param msg message containing query statement
         * @param success true if successful, false if error
         */
        void server_msg_response(SessionMsgPtr msg, bool success);

        /**
         * @brief Callback from Server indicating reception of ready for query message
         * @param xact_status transaction status: I - Idle, T - Transaction, E - Error in transaction
         */
        void server_ready_msg(char xact_status);

        /**
         * @brief Callback from Server indicating that it is shutting down
         * @param session session that is shutting down
         */
        void server_shutdown(ServerSessionPtr session);

        /**
         * @brief Enqueue a failover notification message to this session
         */
        void queue_failover_notification();

        /**
         * @brief Parse a simple query and return type of server session that can handle it
         * static, public for testing
         * @param db_id database id
         * @param buffer buffer holding original query
         * @param query query to parse (multiple queries separated by ';')
         * @return query statement that holds the parsed query stmts as children
         */
        static QueryStmtPtr parse_simple_query(uint64_t db_id, const BufferPtr buffer, const std::string_view query);

    private:

        /** map of pid amd cancel key pair to the corresponding ClientSession */
        static std::unordered_map<
            int32_t,
            std::pair<std::vector<uint8_t>, std::shared_ptr<ClientSession>>
        > _cancel_map;

        /** cache of statements, transaction history and session history */
        StatementCache _stmt_cache;

        ServerSessionPtr _primary_session; ///< primary server session
        ServerSessionPtr _replica_session; ///< replica server session
        ServerSessionPtr _pending_replica_session; ///< pending replica server session during failover

        std::string _default_schema = "public"; ///< default schema to be used for query parsing

        bool _shadow_mode = false;  ///< shadow mode flag; if true, send to primary and replica
        bool _primary_mode = false; ///< primary mode flag; if true, send to primary only

        ClientAuthorizationPtr _auth; ///< client authorization

        std::deque<SessionMsgPtr> _msg_queue; ///< queue of messages to be processed

        uint64_t _start_time; ///< start time of session in epoch ms

        /** Helper to queue message internally, for batch push to server session */
        void _queue_msg(SessionMsgPtr msg) {
            _msg_queue.push_back(msg);
        }

        /** Helper to send message queue to server */
        void _send_msg_queue();

        /**
         * @brief Check for pending data on any associated connections
         * @param fds set of fds; set socket if socket has data
         * @return true if any socket has data
         * @return false if no socket has data
         */
        bool _has_pending_data(std::set<int> &fds) const;

        /**
         * @brief Entry point for data from the connection, called from _run()
         */
        void _process_connection();

        /**
         * @brief Process notifications from the server
         * Currently handles failover notification indicating that the replica
         * is to be failed over to a new replica.
         */
        void _process_notifications();

        /**
         * @brief Handle failover notification from server session
         * Allocate a new replica session and associate it with this client session
         */
        void _handle_failover_notification();

        /**
         * @brief Handle completion of authentication for failover replica session
         */
        void _handle_failover_auth_done();

        /**
         * @brief Read in data from client, parse queries and dispatch to server session
         */
        void _handle_request();

        /** Handle authentication request */
        void _handle_auth();

        /** Handle cancel request */
        void _handle_cancel(int pid, const std::vector<uint8_t> &cancel_key);

        /** Handle simple query request */
        void _handle_simple_query(BufferPtr buffer, uint64_t seq_id);

        /** Handle parse request */
        void _handle_parse(BufferPtr buffer, uint64_t seq_id);

        /** Handle bind request */
        void _handle_bind(BufferPtr buffer, uint64_t seq_id);

        /** Handle describe request */
        void _handle_describe(BufferPtr buffer, uint64_t seq_id);

        /** Handle execute request */
        void _handle_execute(BufferPtr buffer, uint64_t seq_id);

        /** Handle close request */
        void _handle_close(BufferPtr buffer, uint64_t seq_id);

        /** Handle sync request */
        void _handle_sync(BufferPtr buffer, uint64_t seq_id);

        /** Handle function call request */
        void _handle_function_call(BufferPtr buffer, uint64_t seq_id);

        /**
         * @brief Create a server session of a certain type: primary or replica
         * @param type type of server session to create (PRIMARY or REPLICA)
         * @param seq_id sequence id
         * @param failover_session true if this is being created as part of a failover
         * @return ServerSessionPtr server session
         */
        ServerSessionPtr _create_server_session(Session::Type type, uint64_t seq_id, bool failover_session=false);

        /**
         * @brief Select a server session based on type; tries to use
         * associated session or _primary, _replica before calling create.
         * @param type type of server session to select
         * @param seq_id sequence id
         * @return ServerSessionPtr server session
         */
        ServerSessionPtr _select_session(Type type, uint64_t seq_id);

        /** Helper associated session as a server session ptr */
        ServerSessionPtr _get_associated_session() {
            return std::static_pointer_cast<ServerSession>(get_associated_session());
        }

        /**
         * @brief Helper to switch failover replica session with replica session
         * @return true if the switch was successful, false otherwise (requeue needed)
         */
        bool _switch_failover_replica();

        /**
         * @brief Remap a parse type from the parser context to a QueryStmt::Type
         * @param context parser context
         * @return QueryStmt::Type remapped type
         */
        static QueryStmt::Type _remap_parse_type(const Parser::StmtContextPtr context);
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
} // namespace springtail::pg_proxy
