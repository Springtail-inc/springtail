#pragma once

#include <memory>
#include <utility>
#include <functional>
#include <string>
#include <variant>
#include <atomic>

#include <common/logging.hh>
#include <common/concurrent_queue.hh>

#include <proxy/buffer_pool.hh>
#include <proxy/connection.hh>
#include <proxy/auth/scram.hh>
#include <proxy/user_mgr.hh>
#include <proxy/session_msg.hh>

namespace springtail {
    // forward declarations to avoid circular dependencies
    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    class DatabaseInstance;
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;

    /**
     * @brief Session base class.  Derived classes  include:
     * - ClientSession -- client session client connects to proxy
     * - ServerSession -- proxy connects to the server; either a replica or primary
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
            STARTUP=0,        ///< initial state
            SSL_HANDSHAKE=1,  ///< SSL handshake
            AUTH=2,           ///< authentication
            AUTH_SERVER=3,    ///< server auth
            AUTH_DONE=4,      ///< auth complete
            READY=5,          ///< ready for query
            DEPENDENCIES=6,   ///< waiting on dependencies
            QUERY=7,          ///< query in progress
            EXTENDED_ERROR=8, ///< extended message error state
            ERROR=99          ///< fatal error state
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

        /**
         * @brief Construct a session with a connection and server ptr.
         * For client sessions.
         * @param connection connection
         * @param server     main server
         * @param type       type of session (default=CLIENT)
         * @return Session object
         */
        Session(ProxyConnectionPtr connection,
                ProxyServerPtr server,
                Type type=CLIENT);

        /**
         * Construct a session with a database instance and user.
         * For server/replica sessions
         * @param instance   database instance
         * @param connection connection
         * @param server     main server
         * @param user       user
         * @param database   database name
         * @param type       type of session (default=PRIMARY)
         * @return Session object
         */
        Session(DatabaseInstancePtr instance,
                ProxyConnectionPtr connection,
                ProxyServerPtr server,
                UserPtr user,
                const std::string &database,
                Type type=PRIMARY);

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        /** Destruct a connection. */
        virtual ~Session() { SPDLOG_DEBUG("Session destructor"); };

        /** Process messages for session connection;
         * thread entry, calls _process() */
        void operator()();

        /** Less than operator for std::set */
        bool operator<(const Session &rhs) const {
            return _id < rhs._id;
        }

        /** Get connection associated with this session */
        ProxyConnectionPtr get_connection() const {
            return _connection;
        }

        /** Set session to be associated with this session */
        void set_associated_session(std::shared_ptr<Session> remote_session) {
            _associated_session = remote_session;
            remote_session->_associated_session = shared_from_this();
            _waiting_on_session = true;
        }

        /** Clear associated session from this and remote session */
        void clear_associated_session() {
            assert(_associated_session != nullptr);
            assert(_waiting_on_session == true);
            _waiting_on_session = false;
            _associated_session->_associated_session = nullptr;
            _associated_session = nullptr;
        }

        /** Get remote session associated with this session */
        std::shared_ptr<Session> get_associated_session() const {
            return _associated_session;
        }

        Session::Type associated_session_type() const {
            return _associated_session->_type;
        }

        void set_waiting_on_session(bool waiting) {
            _waiting_on_session = waiting;
        }

        /** Is this session blocked waiting for another session to complete */
        bool is_waiting_on_session() const {
            return _waiting_on_session;
        }

        /** Set database name for this session */
        void set_database(const std::string &database) {
            _database = database;
        }

        /** Get database name for this session */
        const std::string &database() const {
            return _database;
        }

        /** Get user name for this session */
        const std::string &username() const {
            return _user->username();
        }

        /** Get db instance */
        DatabaseInstancePtr get_instance() const {
            return _instance;
        }

        /** Shutdown the session, close connection, etc */
        void shutdown() {
            SPDLOG_DEBUG("Shutting down session: type={}, socket={}",
                         _type == Type::PRIMARY ? "PRIMARY" : "CLIENT",
                         _connection->get_socket());
            assert(_associated_session == nullptr);
            _state = ERROR;
            _handle_error();
        }

        /** Check if session is in ready state or not */
        bool is_ready() const {
            return _state == READY;
        }

        /**
         * Add a msg to the queue; to be executed after current message completes
         * @param msg SessionMsgPtr message to enqueue
         */
        void queue_msg(SessionMsgPtr msg, SessionPtr remote_session) {
            if (_associated_session == nullptr) {
                set_associated_session(remote_session);
            }
            remote_session->_msg_queue.push(msg);
        }

        /**
         * @brief Queue message on associated session
         * @param msg Message to queue
         */
        void queue_msg(SessionMsgPtr msg) {
            assert (_associated_session != nullptr);
            _associated_session->_msg_queue.push(msg);
            if (_type == Type::CLIENT) {
                assert (_waiting_on_session == true);
            }
        }

        /**
         * @brief Check if message queue is empty
         * @return true if empty
         */
        bool is_msg_queue_empty() {
            return _msg_queue.empty();
        }

        /**
         * @brief Get a queued msg if one exists
         * @return SessionMsgPtr or nullptr if no queued msg
         */
        SessionMsgPtr get_msg() {
            SessionMsgPtr msg = _msg_queue.try_pop();
            return msg;
        }

        void block_messages() {
            _ready_for_message = false;
        }

        void enable_messages() {
            _ready_for_message = true;
        }

        /** Get session id */
        int id() const {
            return _id;
        }

    protected:
        ProxyConnectionPtr _connection;   ///< connection associated with this session
        ProxyServerPtr     _server;       ///< server associated with this session

        State        _state = STARTUP;    ///< state of session, governs process()
        Type         _type;               ///< type of session

        UserPtr      _user;        ///< user associated with this session
        UserLoginPtr _login;       ///< user login creds, temporary

        int32_t      _pid;         ///< pid for cancel request
        int32_t      _cancel_key;  ///< cancel key for cancel request

        std::string  _database;    ///< database name associated with this session

        DatabaseInstancePtr _instance; ///< database instance associated with this session

        uint64_t _id;                   ///< unique id for session

        bool _in_transaction = false;   ///< is this session in a transaction

        /** Process messages for session connection,
         * must be implemented by derived class */
        virtual void _process_connection() = 0;

        virtual void _process_msg(SessionMsgPtr msg) = 0;

        /** Get user creds */
        UserLoginPtr _get_user_login();

        /** Read full message from data connection, returns header: 1B code, 4B length */
        void _read_msg(BufferList &buffer_list);

        /** Read header: 1B code, 4B length from data connection */
        std::pair<char,int32_t> _read_hdr();

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

        bool _ready_for_message = true;  ///< ready to process messages

        /** queue of messages to process */
        ConcurrentQueue<SessionMsg> _msg_queue;

        /**
         * Single place to do error handling on messages.  Called to check for queued messages.
         * @param is_remote true if this called on an associated session
         */
        void _internal_process_msgs(bool is_remote);

        /** enable processing of msgs via server poll loop */
        void _enable_processing();

        /** handle fatal error, by shutting down */
        void _handle_error();
    };
    using SessionPtr = std::shared_ptr<Session>;
}