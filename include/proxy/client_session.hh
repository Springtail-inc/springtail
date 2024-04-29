#pragma once

#include <memory>
#include <string>
#include <utility>

#include <proxy/session.hh>
#include <proxy/request_handler.hh>
#include <proxy/buffer.hh>
#include <proxy/connection.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {
    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class ClientSession : public Session
    {
    public:
        constexpr static char SERVER_VERSION[] = "16.0 (Springtail)";

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /// Construct a connection with the given socket.
        explicit ClientSession(ProxyConnectionPtr connection,
                               ProxyServerPtr server);

        ~ClientSession();

        /** notification from pool indicating server is free to use */
        void notify_server_available(SessionPtr server);

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
        void _process_startup_msg(int32_t code, int32_t msg_length);
        void _process_ssl_request();

        void _encode_parameter_status(const std::string &key, const std::string &value);

        void _encode_auth_md5();
        void _encode_auth_ok();
        void _encode_auth_scram();

        void _create_primary_server_session();

        void _handle_request();
        void _handle_startup();
        void _handle_ssl_handshake();
        void _handle_auth();
        void _handle_scram_auth(const std::string &data);
        void _handle_scram_auth_continue(const std::string &data);
        void _handle_server_error(const std::string_view msg);
        void _handle_simple_query(const std::string &query);

        void _send_auth_req();
        void _send_auth_done();

        ServerSessionPtr _select_session_and_notify(Type type, SessionMsgPtr msg);

        /** Release server session back to session pool */
        void _release_server_session();
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
}