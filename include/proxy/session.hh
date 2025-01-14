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

namespace springtail::pg_proxy {

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

        /** Startup parameters that can't be 'SET' after session startup */
        const std::set<std::string> EXCLUDED_STARTUP_PARAMS = {
            "user",
            "database",
            "application_name",
            "client_encoding",  // maybe this can be sometimes...
            "is_superuser",
        };

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

            // reset session states; states after this session is reset
            // and released back to the session free pool
            RESET_SESSION=9,         ///< reset session state, e.g. after error
            RESET_SESSION_READY=10,  ///< reset session ready for allocation
            RESET_SESSION_PARAMS=11, ///< reset session, sending startup parameters

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
         * @brief Construct a session with a connection and server ptr.  Type forced to client.
         * For client sessions.
         * @param connection connection
         * @param server     main server
         * @return Session object
         */
        Session(ProxyConnectionPtr connection,
                ProxyServerPtr server);

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
                const std::unordered_map<std::string, std::string> &parameters,
                Type type=PRIMARY);

        /** For test purposes */
        Session(Type type,
                uint64_t id,
                uint64_t db_id,
                const std::string &database,
                const std::string &username)
            : _type(type), _user(std::make_shared<User>(username)), _db_id(db_id), _database(database), _id(id)
        {}

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        /** Destruct a connection. */
        virtual ~Session() { SPDLOG_DEBUG_MODULE(LOG_PROXY, "Session destructor"); };

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
            assert(remote_session != nullptr);
            _associated_session = remote_session;
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Setting associated session", (_type == CLIENT ? 'C': 'S'), _id);
        }

        /** Clear associated session from this session, leaves any association on remote session */
        void clear_associated_session() {
            assert(_associated_session != nullptr);
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Clearing associated session", (_type == CLIENT ? 'C': 'S'), _id);
            _associated_session = nullptr;
        }

        /** Get remote session associated with this session */
        std::shared_ptr<Session> get_associated_session() const {
            return _associated_session;
        }

        Type associated_session_type() const {
            return _associated_session->_type;
        }

        /** Set database name for this session */
        void set_database(const std::string &database) {
            _database = database;
        }

        /** Get database name for this session */
        const std::string &database() const {
            return _database;
        }

        /** Get database id for this session */
        const uint64_t database_id() const {
            return _db_id;
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
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Shutting down session: type={}, socket={}",
                         _type == Type::PRIMARY ? "PRIMARY" : "CLIENT",
                         _connection->get_socket());
            CHECK_EQ(_associated_session, nullptr);
            _state = ERROR;
            _handle_error();
        }

        /** Check if session is in ready state or not */
        virtual bool is_ready() const {
            return _state == READY;
        }

        /**
         * Add a msg to the queue; to be executed after current message completes
         * @param msg SessionMsgPtr message to enqueue
         */
        void queue_msg(SessionMsgPtr msg, SessionPtr remote_session) {
            if (_associated_session == nullptr) {
                set_associated_session(remote_session);
            } else if (_associated_session != remote_session) {
                SPDLOG_WARN("Associated session not cleared");
                clear_associated_session();
                set_associated_session(remote_session);
            }
            remote_session->_msg_queue.push(msg);
        }

        /**
         * @brief Queue message on associated session
         * @param msg Message to queue
         */
        void queue_msg(SessionMsgPtr msg) {
            if (_is_shadow) {
                return;
            }
            assert (_associated_session != nullptr);
            _associated_session->_msg_queue.push(msg);
        }

        /**
         * @brief Queue a shutdown message on this session; higher priority than other messages
         * Clears the message queue and sets the session to ready for message
         * @param msg Message to queue
         */
        void queue_shutdown_msg(SessionMsgPtr msg)
        {
            _msg_queue.clear();
            _msg_queue.push(msg);
            _ready_for_message = true;
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
        uint32_t id() const {
            return _id;
        }

        /** Get session type */
        Type type() const {
            return _type;
        }

        /** Type string */
        std::string type_str() const {
            if (_type == Type::CLIENT) {
                return "C";
            }
            return "S";
        }

        /**
         * @brief Set shadow flag
         * @param shadow true if this is a shadow session
         */
        void set_shadow_mode(bool shadow) {
            assert (_type == Type::REPLICA);
            _is_shadow = shadow;
        }

        /**
         * @brief Get shadow flag
         * @return true if this is a shadow session
         */
        bool is_shadow() const {
            return _is_shadow;
        }

        /**
         * @brief Has this session been shutdown
         * @return true if shutdown
         * @return false if not shutdown
         */
        bool is_shutdown() const {
            return _shut_down_flag.test();
        }

        /**
         * @brief Test and set atomic shutdown flag
         * @return true if shutdown was set previously
         * @return false if shutdown was not set previously
         */
        bool test_and_set_shutdown() {
            return _shut_down_flag.test_and_set();
        }

        /**
         * @brief Does this session have a closed connection
         * @return true if connection is closed
         * @return false if connection is open
         */
        virtual bool is_connection_closed() const {
            return _connection->closed();
        }

        /**
         * @brief Reset private session state
         */
        virtual void reset_session() {
            _is_shadow = false;
            _in_transaction = false;
            _login.reset();
            _associated_session.reset();
            _msg_queue.clear();
            _ready_for_message = true;
            _state = RESET_SESSION;
        }

    protected:
        ProxyConnectionPtr _connection;    ///< connection associated with this session
        ProxyServerPtr     _server;        ///< server associated with this session

        std::mutex    _session_mutex;      ///< mutex for session

        State        _state = STARTUP;     ///< state of session, governs process()
        Type         _type;                ///< type of session

        UserPtr      _user;                ///< user associated with this session
        UserLoginPtr _login;               ///< user login creds, temporary

        int32_t      _pid;                 ///< pid for cancel request
        int32_t      _cancel_key;          ///< cancel key for cancel request

        uint64_t     _db_id;               ///< database id associated with this session
        std::string  _database;            ///< database name associated with this session

        DatabaseInstancePtr _instance;     ///< database instance associated with this session

        uint32_t _id;                      ///< unique id for session

        std::atomic<uint32_t> _seq_id = 0; ///< sequence id for this session

        bool _in_transaction = false;      ///< is this session in a transaction

        bool _is_shadow = false;           ///< is this a shadow session; replica shadowing primary

        std::unordered_map<std::string, std::string> _parameters; ///< parameters for the session

        /** Process connection, entry from operator()() */
        bool _process(std::unique_lock<std::mutex> &lock);

        /** Process messages for session connection,
         * must be implemented by derived class */
        virtual void _process_connection() = 0;

        virtual void _process_msg(SessionMsgPtr msg) = 0;

        /** Generate a sequence ID for logging, should be generated by client session */
        uint64_t _gen_seq_id() {
            _seq_id++;
            if (_type != Type::CLIENT) {
                return _seq_id;
            }
            // combine with client session id
            uint64_t seq_id = _id;
            return (seq_id << 32) | _seq_id;
        }

        /** Get user creds */
        UserLoginPtr _get_user_login();

        /** Read full message from data connection, returns header: 1B code, 4B length */
        void _read_msg(BufferList &buffer_list);

        /** Read header: 1B code, 4B length from data connection */
        std::pair<char,int32_t> _read_hdr();

        /** Stream data from one connection directly to the other */
        void _stream_to_remote_session(char code, int32_t msg_length, uint64_t seq_id);

        /** Send data to remote session, assumes buffer contains code and length */
        void _send_to_remote_session(char code, const BufferPtr buffer, uint64_t seq_id);

        /** Log buffer */
        void _log_buffer(bool incoming, char code, int32_t data_length, const char *data, uint64_t seq_id, bool final=true);

    private:
        /** client/server session associated with this one */
        std::shared_ptr<Session> _associated_session = nullptr;

        /** atomic shutdown flag */
        std::atomic_flag _shut_down_flag = ATOMIC_FLAG_INIT;

        /** ready to process messages */
        bool _ready_for_message = true;

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
} // namespace springtail::pg_proxy