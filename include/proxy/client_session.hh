#pragma once

#include <memory>
#include <vector>
#include <string>
#include <utility>

#include <proxy/request_handler.hh>
#include <proxy/buffer.hh>
#include <proxy/connection.hh>

namespace springtail {
    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class ClientSession : public std::enable_shared_from_this<ClientSession>
    {
    public:
        constexpr static int32_t MSG_STARTUP_V2 =0x20000;
        constexpr static int32_t MSG_STARTUP_V3 = 0x30000;
        constexpr static int32_t MSG_SSLREQ = 80877103;
        constexpr static int32_t MSG_CANCEL = 80877102;

        constexpr static char SERVER_VERSION[] = "16.0 (Springtail)";

        constexpr static int32_t MD5_PASSWD_LEN = 35;

        enum State : int8_t {
            STARTUP=0,
            AUTH=1,
            READY=2,
            ERROR=3
        };

        ClientSession(const ClientSession&) = delete;
        ClientSession& operator=(const ClientSession&) = delete;

        /// Construct a connection with the given socket.
        explicit ClientSession(ProxyConnectionPtr connection,
                               ProxyServerPtr server);

        ~ClientSession();

        // entry point for handling messages
        void operator()() {
            _process();
        }

        ProxyConnectionPtr get_connection() {
            return _connection;
        }

    private:
        struct UserLogin {
            enum Type {
                MD5,
                TRUST,
                SCRAM,
                UNKNOWN_USER
            };

            Type _type;

            std::string _password;
            uint32_t    _salt;

            UserLogin(Type type=TRUST)
                : _type(type)
            {}

            UserLogin(Type type, const std::string &password, uint32_t salt)
                : _type(type),
                  _password(password),
                  _salt(salt)
            {}
        };
        using UserLoginPtr = std::shared_ptr<UserLogin>;

        /// Object containing socket for the connection.
        ProxyConnectionPtr _connection;

        /// @brief Pointer to the server object.
        ProxyServerPtr _server;

        ProxyBuffer _read_buffer{1024};
        ProxyBuffer _write_buffer{1024};

        int _id;

        int32_t _pid = 1123;
        int32_t _key = 793746;

        State _state = STARTUP;

        std::string _username;
        std::string _database;
        UserLoginPtr _login;

        void _process();

        void _process_startup_msg(int32_t code, int32_t msg_length);

        void _encode_parameter_status(const std::string &key, const std::string &value);

        void _encode_auth_md5();
        void _encode_auth_ok();
        void _encode_auth_scram();

        void _handle_request();
        void _handle_startup();
        void _handle_auth();

        void _send_auth_req();
        void _send_auth_done();
        std::pair<char,int32_t> _read_msg();

        UserLoginPtr _get_user_login();
    };
    using ClientSessionPtr = std::shared_ptr<ClientSession>;
}