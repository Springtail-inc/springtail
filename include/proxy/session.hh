#pragma once

#include <memory>
#include <utility>

#include <common/logging.hh>

#include <proxy/connection.hh>
#include <proxy/buffer.hh>
#include <proxy/user_mgr.hh>
#include <proxy/auth/scram.hh>

namespace springtail {
    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    /**
     * @brief Session base class.  Derived classes  include:
     * - ClientSession -- client session client connected to us
     * - ServerSession -- we connect to the server
     */
    class Session {
    public:
        /** Type of session */
        enum Type : int8_t {
            CLIENT=0,
            PRIMARY=1,
            SPRINGTAIL=2
        };

        /** State of session */
        enum State : int8_t {
            STARTUP=0,
            AUTH=1,
            AUTH_DONE=2,
            READY=3,
            ERROR=4
        };

        constexpr static int32_t MSG_STARTUP_V2 =0x20000;
        constexpr static int32_t MSG_STARTUP_V3 = 0x30000;
        constexpr static int32_t MSG_SSLREQ = 80877103;
        constexpr static int32_t MSG_CANCEL = 80877102;

        constexpr static int8_t MSG_AUTH_OK = 0;
        constexpr static int8_t MSG_AUTH_MD5 = 5;
        constexpr static int8_t MSG_AUTH_SASL = 10;
        constexpr static int8_t MSG_AUTH_SASL_CONTINUE = 11;
        constexpr static int8_t MSG_AUTH_SASL_COMPLETE = 12;

        // max number of iterations to read packets on single socket
        // before giving thread up
        constexpr static int    PKT_ITER_MAX_COUNT = 5;

        /** Construct a session with the given socket. */
        explicit Session(ProxyConnectionPtr connection,
                         ProxyServerPtr server,
                         Type type=CLIENT)
            : _connection(connection),
              _server(server),
              _type(type) {}

        explicit Session(ProxyConnectionPtr connection,
                         ProxyServerPtr server,
                         const std::string &username,
                         const std::string &database,
                         Type type=CLIENT)
            : _connection(connection),
              _server(server), _type(type),
              _username(username), _database(database) {}

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        /** Destruct a connection. */
        virtual ~Session() {};

        /** Process messages for session connection;
         * thread entry, calls _process() */
        void operator()();

        /** Less than operator for std::set */
        bool operator<(const Session &rhs) {
            return _connection->get_socket() < rhs._connection->get_socket();
        }

        /** Process messages for session connection,
         * must be implemented by derived class */
        virtual void _process() = 0;

        ProxyConnectionPtr get_connection() {
            return _connection;
        }

    protected:
        ProxyConnectionPtr _connection;
        ProxyServerPtr _server;

        State _state = STARTUP;
        Type _type;

        ProxyBuffer _read_buffer{1024};
        ProxyBuffer _write_buffer{1024};

        std::string _username;
        std::string _database;
        UserLoginPtr _login;

        int32_t _pid;
        int32_t _cancel_key;

        UserLoginPtr _get_user_login();

        std::pair<char,int32_t> _read_msg();
    };
    using SessionPtr = std::shared_ptr<Session>;
}