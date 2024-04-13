#pragma once

#include <proxy/session.hh>

namespace springtail {
    class ServerSession : public Session {
    public:
        ServerSession(const ServerSession&) = delete;
        ServerSession& operator=(const ServerSession&) = delete;

        explicit ServerSession(ProxyConnectionPtr connection,
                               ProxyServerPtr server,
                               const std::string &username,
                               const std::string &database);

        ~ServerSession() override {};

        void _process() override;

    private:
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
}
