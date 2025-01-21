#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <unordered_map>

#include <proxy/runnable.hh>
#include <proxy/session.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/connection.hh>
#include <proxy/history_cache.hh>
#include <proxy/parser.hh>
#include <proxy/authorization.hh>

namespace springtail::pg_proxy {

    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class ServerSession;
    using ServerSessionPtr = std::shared_ptr<ServerSession>;

    class ClientSession : public Session, public Runnable
    {
    public:
        constexpr static int STATEMENT_CACHE_SIZE = 100;

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /** Construct a connection with the given socket. */
        ClientSession(ProxyConnectionPtr connection);

        ~ClientSession();

        void run(const std::set<int> &fds) override;

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
        ServerSessionPtr get_shadow_session() {
            if (_shadow_mode && _replica_session != nullptr) {
                return _replica_session;
            }
            return nullptr;
        }

        /**
         * @brief Get the primary session
         * @return ServerSessionPtr primary session
         */
        ServerSessionPtr get_primary_session() {
            return _primary_session;
        }

        /**
         * @brief Get the replica session
         * @return ServerSessionPtr replica session
         */
        ServerSessionPtr get_replica_session() {
            return _replica_session;
        }


        /**
         * @brief Get a shared pointer to this client session
         * @return std::shared_ptr<ClientSession> shared pointer to this client session
         */
        std::shared_ptr<ClientSession> shared_from_this() {
            return std::static_pointer_cast<ClientSession>(Session::shared_from_this());
        }

        /**
         * @brief Shutdown the client session; send shutdown message to server sessions
         */
        void shutdown_server_sessions();

        /**
         * @brief Callback from Server indicating that authentication is done
         */
        void server_auth_done(ServerSessionPtr session, const std::unordered_map<std::string, std::string> &parameters);

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

    private:

        /** cache of statements, transaction history and session history */
        StatementCache _stmt_cache;

        ServerSessionPtr _primary_session; ///< primary server session
        ServerSessionPtr _replica_session; ///< replica server session

        std::string _default_schema = "public"; ///< default schema to be used for query parsing

        bool _shadow_mode = false;  ///< shadow mode flag; if true, send to primary and replica
        bool _primary_mode = false; ///< primary mode flag; if true, send to primary only

        ClientAuthorizationPtr _auth; ///< client authorization

        void _run(const std::set<int> &fds);

        /**
         * @brief Entry point for data from the connection, called from _run()
         */
        void _process_connection();

        /**
         * @brief Read in data from client, parse queries and dispatch to server session
         */
        void _handle_request();

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
         * @return ServerSessionPtr server session
         */
        ServerSessionPtr _create_server_session(Session::Type type, uint64_t seq_id);

        /**
         * @brief Parse a simple query and return type of server session that can handle it
         * @param buffer buffer holding original query
         * @param query query to parse (multiple queries separated by ';')
         * @param dependencies vector of query statements that need to be fulfilled
         * @return query statement that holds the parsed query stmts as children
         */
        QueryStmtPtr _parse_simple_query(const BufferPtr buffer, const std::string_view query, std::vector<QueryStmtPtr> &dependencies);

        /**
         * @brief Remap a parse type from the parser context to a QueryStmt::Type
         * @param context parser context
         * @return QueryStmt::Type remapped type
         */
        QueryStmt::Type _remap_parse_type(const Parser::StmtContextPtr context) const;

        /**
         * @brief Select a server session based on type; tries to use
         * associated session or _primary, _replica before calling create.
         * @param type type of server session to select
         * @param seq_id sequence id
         * @return ServerSessionPtr server session
         */
        ServerSessionPtr _select_session(Type type, uint64_t seq_id);

        /** Helper to send a message to session, if session is null, select session first */
        void _send_msg(SessionMsgPtr msg, bool is_readonly, SessionPtr session=nullptr);

        /** Helper associated session as a server session ptr */
        ServerSessionPtr _get_associated_session() {
            return std::static_pointer_cast<ServerSession>(get_associated_session());
        }
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
} // namespace springtail::pg_proxy
