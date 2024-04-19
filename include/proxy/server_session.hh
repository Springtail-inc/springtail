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

        /** factory to create session */
        static std::shared_ptr<ServerSession> create(ProxyServerPtr server, UserPtr user);

    protected:
        void _process_connection() override;

        void _process_msg(SessionMsg msg) override;

    private:
        //bool _is_pinned = false;

        /** Initial setup, SSL negotiation */
        void _handle_startup();

        /** Authentication */
        void _handle_auth();

        /** Auth done, final setup and params, waiting for Ready to Query */
        void _handle_auth_done();

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
