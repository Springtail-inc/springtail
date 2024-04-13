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

    private:
        int _id;

        void _process() override;

        void _process_startup_msg(int32_t code, int32_t msg_length);

        void _encode_parameter_status(const std::string &key, const std::string &value);

        void _encode_auth_md5();
        void _encode_auth_ok();
        void _encode_auth_scram();

        void _handle_request();
        void _handle_startup();
        void _handle_auth();
        void _handle_scram_auth(const std::string &data);
        void _handle_scram_auth_continue(const std::string &data);

        void _send_auth_req();
        void _send_auth_done(bool reset=true);
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
}