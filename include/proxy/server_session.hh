#pragma once

#include <proxy/session.hh>
#include <proxy/user_mgr.hh>
#include <proxy/buffer_pool.hh>

namespace springtail {
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

    private:
        //bool _is_pinned = false;

        DatabaseInstancePtr _instance;

        /** Send startup message */
        void _send_startup_msg();

        /** Initial setup, SSL negotiation */
        void _send_ssl_req();

        /** Send SSL handshake */
        void _send_ssl_handshake();

        /** Handle SSL handshake */
        void _handle_ssl_handshake();

        /** Handle ssl response */
        void _handle_ssl_response();

        /** Authentication */
        void _handle_auth(BufferPtr buffer);

        /** Handle replies from server */
        void _handle_message();

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
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
}
