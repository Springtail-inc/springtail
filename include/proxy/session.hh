#pragma once

#include <memory>
#include <utility>
#include <functional>
#include <string>
#include <variant>
#include <atomic>

#include <common/logging.hh>

#include <proxy/connection.hh>
#include <proxy/buffer.hh>
#include <proxy/auth/scram.hh>

namespace springtail {
    // forward declarations to avoid circular dependencies
    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    struct UserLogin;
    using UserLoginPtr = std::shared_ptr<UserLogin>;

    class User;
    using UserPtr = std::shared_ptr<User>;

    /**
     * @brief Session base class.  Derived classes  include:
     * - ClientSession -- client session client connected to us
     * - ServerSession -- we connect to the server
     */
    class Session : public std::enable_shared_from_this<Session> {
    public:
        using SessionPtr = std::shared_ptr<Session>;

        /** Type of session */
        enum Type : int8_t {
            CLIENT=0,
            PRIMARY=1,
            REPLICA=2
        };

        /** State of session */
        enum State : int8_t {
            STARTUP=0,
            SSL_HANDSHAKE=1,
            AUTH=2,
            AUTH_SERVER=3,
            AUTH_DONE=4,
            READY=5,
            ERROR=99
        };

        /** Messages between sessions */
        struct SessionMsg {
            enum SessionMsgType : int8_t {
                // client to server messages
                MSG_CLIENT_SERVER_STARTUP=0,
                MSG_CLIENT_SERVER_SIMPLE_QUERY=1,

                // server to client messages
                MSG_SERVER_CLIENT_AUTH_DONE=10,
                MSG_SERVER_CLIENT_READY=11,
                MSG_SERVER_CLIENT_FATAL_ERROR=99
            } type;

            // union of message types based on SessionMsg type
            std::variant<std::string> data;

            SessionMsg(SessionMsgType type, std::string data)
                : type(type), data(data)
            {}

            SessionMsg(SessionMsgType type)
                : type(type)
            {}
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
              _type(type)
        {
            _read_buffer.reset();
            _write_buffer.reset();
        }

        explicit Session(ProxyConnectionPtr connection,
                         ProxyServerPtr server,
                         UserPtr user,
                         Type type=CLIENT)
            : _connection(connection),
              _server(server), _type(type),
              _user(user)
        {
            _read_buffer.reset();
            _write_buffer.reset();
        }

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        /** Destruct a connection. */
        virtual ~Session() { SPDLOG_DEBUG("Session destructor"); };

        /** Process messages for session connection;
         * thread entry, calls _process() */
        void operator()();

        /** Less than operator for std::set */
        bool operator<(const Session &rhs) const {
            return _connection->get_socket() < rhs._connection->get_socket();
        }

        ProxyConnectionPtr get_connection() const {
            return _connection;
        }

        /** set associated session */
        void set_associated_session(std::shared_ptr<Session> remote_session) {
            _associated_session = remote_session;
            remote_session->_associated_session = shared_from_this();
            _waiting_on_session = true;
        }

        /** clear waiting on session flag */
        void clear_waiting_on_session() {
            _waiting_on_session = false;
        }

        /** clear associated session from this and remote session */
        void clear_associated_session() {
            _associated_session->_associated_session = nullptr;
            _associated_session = nullptr;
        }

        /** notify server session of message */
        void notify_server(SessionMsg &msg, SessionPtr remote_session) {
            assert(remote_session->_type == PRIMARY || remote_session->_type == REPLICA);
            SPDLOG_DEBUG("Notifying server session of message: {:d}", (int8_t)msg.type);
            set_associated_session(remote_session);
            remote_session->_internal_process_msg(msg);
        }

        /** notify client session of message */
        void notify_client(SessionMsg &msg) {
            assert(_associated_session != nullptr);
            assert(_associated_session->_type == CLIENT);
            SPDLOG_DEBUG("Notifying client of message: {:d}", (int8_t)msg.type);
            _associated_session->clear_waiting_on_session();
            _associated_session->_internal_process_msg(msg);
        }

        /** get error message */
        const std::string_view get_err_msg() const {
            return _err_msg;
        }

        std::shared_ptr<Session> get_associated_session() const {
            return _associated_session;
        }

        bool is_waiting_on_session() const {
            return _waiting_on_session;
        }

    protected:
        ProxyConnectionPtr _connection;   ///< connection associated with this session
        ProxyServerPtr     _server;       ///< server associated with this session

        State        _state = STARTUP;    ///< state of session, governs process()
        Type         _type;               ///< type of session

        ProxyBuffer  _read_buffer{1024};
        ProxyBuffer  _write_buffer{1024};

        UserPtr      _user;        ///< user associated with this session
        UserLoginPtr _login;       ///< user login creds, temporary

        int32_t      _pid;         ///< pid for cancel request
        int32_t      _cancel_key;  ///< cancel key for cancel request

        std::string_view _err_msg;  ///< error message

        /** Process messages for session connection,
         * must be implemented by derived class */
        virtual void _process_connection() = 0;

        virtual void _process_msg(SessionMsg &msg) = 0;

        /** Get user creds */
        UserLoginPtr _get_user_login();

        /** Read full message from data connection, returns header: 1B code, 4B length */
        std::pair<char,int32_t> _read_msg();

        /** Read header: 1B code, 4B length from data connection */
        std::pair<char,int32_t> _read_hdr();

        /** If we've just read the header, read the actual data into the local write buffer */
        void _read_remaining(int32_t msg_length);

        /** Stream data from one connection directly to the other */
        void _stream_to_remote_session(char code, int32_t msg_length);

        /** Send data to remote session */
        void _send_to_remote_session(char code, int32_t msg_length, const char *data);

    private:
        /** client/server session associated with this one */
        std::shared_ptr<Session> _associated_session = nullptr;
        /** waiting on associated session for data -- _associated_session should be set */
        bool _waiting_on_session = false;
        std::atomic_flag _shut_down_flag = ATOMIC_FLAG_INIT;

        /** Single place to do error handling on messages */
        void _internal_process_msg(SessionMsg &msg);

        /** enable processing of msgs via server poll loop */
        void _enable_processing();

        void _handle_error();

    };
    using SessionPtr = std::shared_ptr<Session>;
}