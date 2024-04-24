#pragma once

#include <proxy/session.hh>

namespace springtail {
    class ServerSession : public Session {
    public:
        ServerSession(const ServerSession&) = delete;
        ServerSession& operator=(const ServerSession&) = delete;

        explicit ServerSession(ProxyConnectionPtr connection,
                               ProxyServerPtr server,
                               UserPtr user);

        ~ServerSession() {};

        std::shared_ptr<ServerSession> shared_from_this() {
            return std::static_pointer_cast<ServerSession>(Session::shared_from_this());
        }

        /** factory to create session */
        static std::shared_ptr<ServerSession> create(ProxyServerPtr server, UserPtr user);

    protected:
        void _process_connection() override;

        void _process_msg(SessionMsg &msg) override;

    private:
        //bool _is_pinned = false;

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
        void _handle_auth(int32_t msg_length);

        /** Auth done, final setup and params, waiting for Ready to Query */
        void _handle_auth_done();

        /** Handle replies from server */
        void _handle_message();

        /** Handle simple query */
        void _handle_simple_query(const std::string &query);

        /** Handle error code 'E' */
        void _handle_error_code(const char *data, int32_t size);

        /** Encode md5 auth response */
        void _encode_auth_md5();

        /** Encode scram-sha-256 auth response */
        void _encode_auth_scram();

        /** Encode scram continue auth response */
        void _encode_auth_scram_continue(const std::string &data);

        void _handle_auth_scram_complete(const std::string &data);
    };
    using ServerSessionPtr = std::shared_ptr<ServerSession>;
}
