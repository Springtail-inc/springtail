#pragma once

#include <proxy/session.hh>
#include <proxy/session_msg.hh>
#include <proxy/user_mgr.hh>
#include <proxy/buffer_pool.hh>

namespace springtail {
    class ClientSession; ///< forward declaration

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

        DatabaseInstancePtr _instance;       ///< database instance

        SessionMsgPtr _current_msg;          ///< current message being processed

        std::set<std::string> _stmts;        ///< issued prepared statement ids

        /** Send startup message */
        void _send_startup_msg();

        /** Initial setup, SSL negotiation */
        void _send_ssl_req();

        /** Send SSL handshake */
        void _send_ssl_handshake();

        /**
         * Send required statements to fulfil dependency
         * @param dependency dependency to fulfil
         * @return true if dependency was sent; false if not sent
         */
        bool _send_dependency(const QueryStmtPtr dependency);

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

        /** Handle response to dependency */
        void _handle_dependency_response();
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
}
