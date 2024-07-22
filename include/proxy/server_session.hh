#pragma once

#include <proxy/session.hh>
#include <proxy/session_msg.hh>
#include <proxy/user_mgr.hh>
#include <proxy/buffer_pool.hh>

namespace springtail {
namespace pg_proxy {

    class ClientSession; ///< forward declaration

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
                      DatabaseInstancePtr instance,
                      Session::Type type=PRIMARY);

        ~ServerSession() {};

        bool is_pinned() const {
            return _is_pinned;
        }

        void pin_client_session(std::weak_ptr<ClientSession> client_session) {
            _client_session = client_session;
            _is_pinned = true;
        }

        std::shared_ptr<ServerSession> shared_from_this() {
            return std::static_pointer_cast<ServerSession>(Session::shared_from_this());
        }

        /** factory to create session */
        static std::shared_ptr<ServerSession>
        create(ProxyServerPtr server, UserPtr user, const std::string &database,
               DatabaseInstancePtr instance, Session::Type type);

    protected:
        void _process_connection() override;

        void _process_msg(SessionMsgPtr msg) override;

        bool _is_pinned = false;
        std::weak_ptr<ClientSession> _client_session; ///< client session

        DatabaseInstancePtr _instance;             ///< database instance

        // message state for current client query
        SessionMsgPtr _current_msg;                ///< current message being processed
        std::queue<QueryStatusPtr> _pending_queue; ///< queue of pending messages

        std::set<std::string> _stmts;              ///< completed prepared statement ids

        /** Send startup message */
        void _send_startup_msg();

        /** Initial setup, SSL negotiation */
        void _send_ssl_req();

        /** Send SSL handshake */
        void _send_ssl_handshake();

        /**
         * Send required statements to fulfil dependency
         * @param dependency dependency to fulfil
         */
        void _send_dependency(const QueryStmtPtr dependency);

        void _send_server_msg(QueryStatusPtr query_status);

        /** Handle SSL handshake */
        void _handle_ssl_handshake();

        /** Handle ssl response */
        void _handle_ssl_response();

        /** Authentication */
        void _handle_auth(BufferPtr buffer);

        /** Handle replies from server */
        void _handle_message_from_server();

        /** Handle simple query */
        void _handle_simple_query(const std::string &query);

        /** Handle error code 'E' */
        void _handle_error_code(BufferPtr buffer);

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

        /** Discard message data */
        void _discard_msg(int32_t msg_length);
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
} // namespace pg_proxy
} // namespace springtail
