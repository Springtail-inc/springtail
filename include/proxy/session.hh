#pragma once

#include <memory>

#include <proxy/buffer_pool.hh>
#include <proxy/connection.hh>
#include <proxy/user_mgr.hh>
#include <proxy/exception.hh>

namespace springtail::pg_proxy {

    // forward declarations to avoid circular dependencies
    class DatabaseInstance;
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;

    /**
     * @brief Help function to help convert the state to its string representation
     *
     * @tparam T - state enum type
     * @param value - state value
     * @param state_to_name - the mapping from state to name
     * @return std::string - string representing the given state
     */
    template <typename T> std::string
    state_to_string(T value, const std::unordered_map<T, std::string_view> &state_to_name)
    {
        static_assert(std::is_enum_v<T>, "T must be an enum or enum class");

        auto it = state_to_name.find(value);
        std::string_view name = (it != state_to_name.end()) ? it->second : "UNKNOWN";

        // convert enum to its underlying integer
        auto int_value = static_cast<std::underlying_type_t<T>>(value);

        return fmt::format("{}({})", name, int_value);
    }

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
        enum class Type : int8_t {
            CLIENT=0,
            PRIMARY=1,
            REPLICA=2
        };
        // friend function for logging and DCHECK_EQ
        friend std::ostream& operator<<(std::ostream& os, Type type) {
            return os << "Session::Type(" << static_cast<int>(type) << ")";
        }

        /** State of session */
        enum class State : int8_t {
            STARTUP=0,        ///< initial state
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
        // friend function for logging and DCHECK_EQ
        friend std::ostream& operator<<(std::ostream& os, State state) {
            return os << "Session::State(" << static_cast<int>(state) << ")";
        }

        static inline const std::unordered_map<Session::State, std::string_view>
        state_names {
            { Session::State::STARTUP,               "STARTUP" },
            { Session::State::AUTH_SERVER,           "AUTH_SERVER" },
            { Session::State::AUTH_DONE,             "AUTH_DONE" },
            { Session::State::READY,                 "READY" },
            { Session::State::DEPENDENCIES,          "DEPENDENCIES" },
            { Session::State::QUERY,                 "QUERY" },
            { Session::State::EXTENDED_ERROR,        "EXTENDED_ERROR" },
            { Session::State::RESET_SESSION,         "RESET_SESSION" },
            { Session::State::RESET_SESSION_READY,   "RESET_SESSION_READY" },
            { Session::State::RESET_SESSION_PARAMS,  "RESET_SESSION_PARAMS" },
            { Session::State::ERROR,                 "ERROR" }
        };

        /** Out-of-band notification message sent to a session via the server */
        struct NotificationMsg {
            enum class Type : int8_t {
                NOTIFY_NONE=0,      ///< no notification; used for return type of peek
                NOTIFY_FAILOVER=1,  ///< failover notification for replica (sent to client)
                NOTIFY_FAILOVER_READY=2, ///< failover complete, ready to switch (sent to client)
            };

            Type type;                                   ///< type of notification
            // keep it simple for now, no need for data variant at this time
        };

        // max number of iterations to read packets on single socket
        // before giving thread up
        constexpr static int    PKT_ITER_MAX_COUNT = 5;

        /**
         * @brief Construct a session with a connection and server ptr.  Type forced to client.
         * For client sessions.
         * @param connection connection
         * @return Session object
         */
        explicit Session(ProxyConnectionPtr connection);

        /**
         * Construct a session with a database instance and user.
         * For server/replica sessions
         * @param instance   database instance
         * @param connection connection
         * @param user       user
         * @param database   database name
         * @param type       type of session (default=PRIMARY)
         * @return Session object
         */
        Session(DatabaseInstancePtr instance,
                ProxyConnectionPtr connection,
                UserPtr user,
                const std::string &database,
                const std::unordered_map<std::string, std::string> &parameters,
                Type type=Type::PRIMARY);

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

        /** Thread entry point, calls child::run() */
        void operator()();

        /** Destruct a connection. */
        virtual ~Session() { LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Session destructor"); };

        /** Virtual run to be overriden by child class */
        virtual void run(std::set<int> &fds) = 0;

        /** Perform a fatal shutdown */
        virtual void shutdown_session() = 0;

        /** Name of session */
        virtual std::string name() const {
            return fmt::format("[{}:{}]", type_str(), _id);
        }

        /** Helper to add file descriptor */
        void add_fd(int fd) {
            _fds.insert(fd);
        }

        /** Helper to clear file descriptors */
        void clear_fds() {
            _fds.clear();
        }

        /** Less than operator for std::set */
        bool operator<(const Session &rhs) const {
            return _id < rhs._id;
        }

        /** Comparator for SessionPtr */
        struct SessionComparator {
            bool operator()(const SessionPtr &lhs, const SessionPtr &rhs) const {
                return lhs->_id < rhs->_id;
            }
        };

        /** Custom hash function for SessionPtr */
        struct SessionHash {
            std::size_t operator()(const SessionPtr& session) const {
                return std::hash<int>{}(session->_id);  // Hash based on `_id`
            }
        };

        /** Custom equality function for SessionPtr */
        struct SessionEqual {
            bool operator()(const SessionPtr& lhs, const SessionPtr& rhs) const {
                return lhs->_id == rhs->_id;  // Compare based on `_id`
            }
        };

        /** Get connection associated with this session */
        ProxyConnectionPtr get_connection() const {
            return _connection;
        }

        /** Set session to be associated with this session */
        void set_associated_session(std::shared_ptr<Session> remote_session) {
            assert(remote_session != nullptr);
            _associated_session = remote_session;
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[{}:{}] Setting associated session", (_type == Type::CLIENT ? 'C': 'S'), _id);
        }

        /** Clear associated session from this session, leaves any association on remote session */
        void clear_associated_session() {
            if (_associated_session == nullptr) {
                return;
            }
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "[{}:{}] Clearing associated session", (_type == Type::CLIENT ? 'C': 'S'), _id);
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

        /** Check if session is in ready state or not */
        virtual bool is_ready() const {
            return _state == State::READY;
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
         * @brief Get JSON representation of session; used by admin server
         * @return nlohmann::json JSON object representing session
         */
        nlohmann::json to_json() const;

        /**
         * @brief Get brief JSON representation of session; used by admin server
         * @return nlohmann::json JSON object representing session
         */
        nlohmann::json _to_json_brief() const;

        /**
         * @brief Get hostname for this session
         * @return std::string hostname
         */
        const std::string hostname() const;

        /**
         * @brief Does this session have a closed connection
         * @return true if connection is closed
         * @return false if connection is open
         */
        virtual bool is_connection_closed() const {
            if (_connection == nullptr) {
                return true;
            }
            return _connection->closed();
        }

        /**
         * @brief Reset private session state
         */
        virtual void reset_session() {
            _is_shadow = false;
            _in_transaction = false;
            _associated_session.reset();
            _state = State::RESET_SESSION;
        }

        /**
         * Read full message from data connection, returns header: 1B code, 4B length
         * @param connection connection to read from
         * @param buffer_list buffer list to read into
         */
        static void read_msg(ProxyConnectionPtr connection, BufferList &buffer_list);

        /**
         * @brief Read and log message, when we know the code and length
         * @param connection connection to read from
         * @param code message code
         * @param msg_length message length
         * @param type session type
         * @param id session id
         * @param seq_id sequence id
         * @return BufferPtr buffer read, contains code and length(5B header),
         *         current_data() points past header
         */
        static BufferPtr read_msg(ProxyConnectionPtr connection, char code, int msg_length,
                                  Session::Type type, uint64_t id, uint64_t seq_id);

        /**
         * @brief Read in header from connection
         * @param connection connection to read from
         * @return std::pair<char,int32> code and length
         */
        static std::pair<char,int32_t> read_hdr(ProxyConnectionPtr connection);

        /**
         * @brief Static version of session log_buffer call, logs buffer to log
         * @param type session type
         * @param id session id
         * @param incoming true if incoming data
         * @param code message code
         * @param data_length length of data
         * @param data data buffer
         * @param seq_id sequence id
         */
        static void log_buffer(Type type, uint64_t id, bool incoming, char code,
                               int32_t data_length, const char *data,
                               uint64_t seq_id, bool final=true);

    protected:
        ProxyConnectionPtr _connection;    ///< connection associated with this session

        std::mutex   _session_mutex;       ///< mutex for session

        State        _state = State::STARTUP; ///< state of session, governs process()
        Type         _type;                ///< type of session

        int32_t      _pid;                 ///< pid for cancel request
        std::vector<uint8_t>  _cancel_key; ///< cancel key for cancel request

        UserPtr      _user;                ///< user
        uint32_t     _db_id;               ///< database id
        std::string  _database;            ///< database name
        DatabaseInstancePtr _instance;     ///< database instance associated with this session

        std::unordered_map<std::string, std::string> _parameters; ///< startup parameters

        std::mutex _notification_mutex;   ///< mutex for server notifications
        std::queue<NotificationMsg> _notification_queue;  ///< out-of-band notifications

        uint64_t _id;                      ///< unique id for session

        bool _in_transaction = false;      ///< is this session in a transaction

        bool _is_shadow = false;           ///< is this a shadow session; replica shadowing primary

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

        /**
         * @brief Error handler wrapper for session calls.
         * Catch any errors and call the session error handler.
         * @tparam Func function
         * @tparam Args arguments
         * @param func function to call
         * @param args arguments to pass to function
         */
        template<typename Func, typename... Args>
        void _wrap_error_handler(Func func, Args && ...args) {
            try {
                func(std::forward<Args>(args)...);
            } catch (ProxyError &e) {
                LOG_ERROR("Error in session: {}", e.what());
                _state = State::ERROR;
            } catch (std::exception &e) {
                LOG_ERROR("Error in session: {}", e.what());
                _state = State::ERROR;
            } catch (...) {
                LOG_ERROR("Unknown exception");
                _state = State::ERROR;
            }

            if (_state == State::ERROR || _connection->closed()) {
                _handle_error();
            }
        }

        /** Stream data from one connection directly to the other */
        void _stream_to_remote_session(char code, int32_t msg_length, uint64_t seq_id);

        /** Send data to associated session connection, assumes buffer contains code and length */
        void _send_to_remote_session(char code, const BufferPtr buffer, uint64_t seq_id);

        /** Send data to session connection, assumes buffer contains code and length */
        void _send_buffer(BufferPtr buffer, uint64_t seq_id);

        /** Internal call to Session::read_message() */
        BufferPtr _read_message(char code, int32_t msg_length, uint64_t seq_id) {
            return read_msg(_connection, code, msg_length, _type, _id, seq_id);
        }

        /** Log buffer */
        void _log_buffer(bool incoming, char code, int32_t data_length, const char *data, uint64_t seq_id, bool final=true);

        /** handle fatal error, by shutting down */
        void _handle_error();

    private:
        /** client/server session associated with this one */
        std::shared_ptr<Session> _associated_session = nullptr;

        /** atomic shutdown flag */
        std::atomic_flag _shut_down_flag = ATOMIC_FLAG_INIT;

        std::atomic_flag _running = ATOMIC_FLAG_INIT;

        std::set<int> _fds;     ///< set of fds to pass to run()

        uint32_t _seq_id = 0;   ///< sequence id for this session
    };
    using SessionPtr = std::shared_ptr<Session>;
} // namespace springtail::pg_proxy