#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include <proxy/session.hh>
#include <proxy/server_session.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/connection.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>
#include <proxy/history_cache.hh>
#include <proxy/parser.hh>

namespace springtail::pg_proxy {

    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class ClientSession : public Session
    {
    public:
        constexpr static char SERVER_VERSION[] = "16.0 (Springtail)";
        constexpr static int STATEMENT_CACHE_SIZE = 100;

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /** Construct a connection with the given socket. */
        ClientSession(ProxyConnectionPtr connection,
                      ProxyServerPtr server);

        ~ClientSession();

        /** notification from pool indicating server is free to use */
        void notify_server_available(SessionPtr server);

        bool is_shadow_mode() const {
            return _shadow_mode;
        }

        bool is_primary_mode() const {
            return _primary_mode;
        }

        ServerSessionPtr get_shadow_session() {
            if (_shadow_mode && _replica_session != nullptr) {
                return _replica_session;
            }
            return nullptr;
        }

        std::shared_ptr<ClientSession> shared_from_this() {
            return std::static_pointer_cast<ClientSession>(Session::shared_from_this());
        }

    protected:

        /**
         * @brief Entry point for data from the connection
         */
        void _process_connection() override;

        /**
         * @brief Entry point for a message from the server (normally a reply))
         * @param msg message from the server
         */
        void _process_msg(SessionMsgPtr msg) override;

    private:
        /** cache of statements, transaction history and session history */
        StatementCache _stmt_cache;

        std::shared_ptr<ServerSession> _primary_session; ///< primary server session
        std::shared_ptr<ServerSession> _replica_session; ///< replica server session

        std::string _default_schema = "public"; ///< default schema to be used for query parsing

        bool _shadow_mode = false;  ///< shadow mode flag; if true, send to primary and replica
        bool _primary_mode = false; ///< primary mode flag; if true, send to primary only

        void _process_startup_msg(int32_t msg_length, uint64_t seq_id);
        void _process_ssl_request();

        void _encode_parameter_status(BufferPtr buffer, const std::string &key, const std::string &value);
        void _encode_auth_md5(BufferPtr buffer);
        void _encode_auth_ok(BufferPtr buffer);
        void _encode_auth_scram(BufferPtr buffer);

        void _handle_request();
        void _handle_startup();
        void _handle_ssl_handshake();
        void _handle_auth();
        void _handle_scram_auth(const std::string_view data, uint64_t seq_id);
        void _handle_scram_auth_continue(const std::string_view data, uint64_t seq_id);
        void _handle_server_error(const std::string_view msg);

        void _handle_simple_query(BufferPtr buffer, uint64_t seq_id);
        void _handle_parse(BufferPtr buffer, uint64_t seq_id);
        void _handle_bind(BufferPtr buffer, uint64_t seq_id);
        void _handle_describe(BufferPtr buffer, uint64_t seq_id);
        void _handle_execute(BufferPtr buffer, uint64_t seq_id);
        void _handle_close(BufferPtr buffer, uint64_t seq_id);
        void _handle_sync(BufferPtr buffer, uint64_t seq_id);
        void _handle_function_call(BufferPtr buffer, uint64_t seq_id);

        void _forward_to_server(BufferPtr buffer, uint64_t seq_id);

        void _send_auth_req(uint64_t seq_id);
        void _send_auth_done(uint64_t seq_id);

        ServerSessionPtr _create_server_session(Session::Type type, uint64_t seq_id);

        /** Does primary server pool exist */
        bool _primary_pool_exists();

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

        /** Select a server session based on type */
        ServerSessionPtr _select_session(Type type, uint64_t seq_id);

        /** Send message to session, if session is null, select session first */
        void _send_msg(SessionMsgPtr msg, bool is_readonly, SessionPtr session=nullptr);
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
} // namespace springtail::pg_proxy
