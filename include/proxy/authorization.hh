#pragma once

#include <proxy/connection.hh>
#include <proxy/session.hh>
#include <proxy/user_mgr.hh>

namespace springtail::pg_proxy {

constexpr static char SERVER_VERSION[] = "16.0 (Springtail)";

constexpr static int32_t MSG_STARTUP_V2 = 0x20000;
constexpr static int32_t MSG_STARTUP_V3 = 0x30000;
constexpr static int32_t MSG_SSLREQ = 80877103;
constexpr static int32_t MSG_CANCEL = 80877102;

constexpr static int8_t MSG_AUTH_OK = 0;
constexpr static int8_t MSG_AUTH_MD5 = 5;
constexpr static int8_t MSG_AUTH_SASL = 10;
constexpr static int8_t MSG_AUTH_SASL_CONTINUE = 11;
constexpr static int8_t MSG_AUTH_SASL_COMPLETE = 12;

class ClientAuthorization {
public:
    ClientAuthorization(ProxyConnectionPtr connection,
                        uint64_t id,
                        int32_t pid,
                        const std::vector<uint8_t> &cancel_key)
        : _state(STARTUP), _connection(connection), _id(id), _pid(pid), _cancel_key(cancel_key)
    {
    }

    /**
     * @brief Process data for authentication
     * @param seq_id sequence id
     * @return true if ready state
     * @return false if not ready state
     * @throw ProxyAuthError if authentication fails
     */
    bool process_auth_data(uint64_t seq_id);

    /**
     * @brief Send auth done message
     * @param seq_id sequence id
     */
    void send_auth_done(uint64_t seq_id,
                        const std::unordered_map<std::string, std::string> &parameters);

    /**
     * @brief Get startup params -- sent by client
     * @return std::unordered_map<std::string, std::string> parameters
     */
    std::unordered_map<std::string, std::string> parameters() const { return _parameters; }

    /**
     * @brief Get db_id
     * @return uint64_t db_id
     */
    uint64_t db_id() const { return _db_id; }

    /**
     * @brief Get database name
     * @return std::string database name
     */
    const std::string database() const { return _database; }

    /**
     * @brief Get user
     * @return UserPtr user
     */
    const UserPtr user() const { return _user; }

    /**
     * @brief Get error code if there is one
     * @return std::string error code
     */
    const std::string &get_error_code() const { return _error_code; }

    /**
     * @brief Get the pid and cancel key pair object
     *
     * @return const std::pair<int32_t, std::vector<uint8_t>>
     */
    std::pair<int32_t, std::vector<uint8_t>> get_pid_cancel_key_pair() const
    {
        return std::make_pair(_pid, _cancel_key);
    }

    bool is_cancel() const { return _is_cancel; }

private:
    enum State : int8_t { STARTUP = 0, SSL_HANDSHAKE = 1, AUTH = 2, READY = 3, ERROR = 99 };

    /** Map of state to state names */
    const static inline std::unordered_map<State, std::string_view> _state_names{
        { State::STARTUP,         "STARTUP" },
        { State::SSL_HANDSHAKE,   "SSL_HANDSHAKE" },
        { State::AUTH,            "AUTH" },
        { State::READY,           "READY" },
        { State::ERROR,           "ERROR" }
    };

    /**
     * @brief Convert state to state name
     *
     * @param s - state
     * @return std::string - state name
     */
    static inline std::string
    _to_string(State s)
    {
        return state_to_string(s, _state_names);
    }

    State _state;
    ProxyConnectionPtr _connection;
    uint64_t _id;
    int32_t _pid;
    std::vector<uint8_t> _cancel_key;
    bool _is_cancel;

    std::unordered_map<std::string, std::string> _parameters;
    std::string _database;
    uint64_t _db_id;
    UserPtr _user;
    UserLoginPtr _login;

    std::string _error_code;

    /** read startup packet */
    void _handle_startup(uint64_t seq_id);
    /** accept ssl connection */
    void _handle_ssl_handshake();
    /** read in data and dispatch */
    void _handle_auth(uint64_t seq_id);
    /** handle initial scram request */
    void _handle_scram_auth(const std::string_view data, uint64_t seq_id);
    /** handle scram continuation request */
    void _handle_scram_auth_continue(const std::string_view data, uint64_t seq_id);

    /** read parameter strings */
    void _process_startup_msg(int32_t remaining, uint64_t seq_id);
    /** respond to ssl request */
    void _process_ssl_request();
    /** respond to cancel request */
    void _process_cancel(int32_t remaining);

    /** send auth request based on login type */
    void _send_auth_req(uint64_t seq_id);

    /** encode auth ok buffer */
    void _encode_auth_ok(BufferPtr buffer);
    /** encode auth md5 buffer */
    void _encode_auth_md5(BufferPtr buffer);
    /** encode auth scram buffer */
    void _encode_auth_scram(BufferPtr buffer);
    /** encode parameter status buffer */
    void _encode_parameter_status(BufferPtr buffer,
                                  const std::string &key,
                                  const std::string &value);
};
using ClientAuthorizationPtr = std::shared_ptr<ClientAuthorization>;

class ServerAuthorization {
public:
    ServerAuthorization(ProxyConnectionPtr connection,
                        uint64_t id,
                        const UserPtr user,
                        const std::string &database,
                        const std::string &db_prefix,
                        Session::Type type,
                        const std::unordered_map<std::string, std::string> &parameters)
        : _state(STARTUP),
          _connection(connection),
          _id(id),
          _user(user),
          _database(database),
          _db_prefix(db_prefix),
          _type(type),
          _parameters(parameters)
    {
        if (_type == Session::Type::REPLICA) {
            // modify the login for replica FDW login
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "[S:{}] Using replica login for user {}", _id, _user->username());
            _login = UserMgr::get_instance()->get_replica_login(_user);
        } else {
            _login = user->get_user_login();
        }
    }

    /**
     * @brief Send startup message
     * @param seq_id sequence id
     */
    void send_startup_msg(uint64_t seq_id);

    /**
     * @brief Process data for authentication
     * @param seq_id sequence id
     * @return true if ready state
     * @return false if not ready state
     * @throw ProxyAuthError if authentication fails
     */
    bool process_auth_data(uint64_t seq_id);

    /**
     * @brief Get the server parameters
     * @return std::unordered_map<std::string, std::string> parameters
     */
    std::unordered_map<std::string, std::string> server_parameters() const
    {
        return _server_parameters;
    }

    /**
     * @brief Get the error code and message if there is one
     * @return std::pair<std::string, std::string> error code and message
     */
    std::pair<std::string, std::string> get_error() const
    {
        return {_error_code, _error_message};
    }

    /**
     * @brief Get the pid and cancel key pair object
     *
     * @return const std::pair<int32_t, std::vector<uint8_t>>
     */
    std::pair<int32_t, std::vector<uint8_t>> get_pid_cancel_key_pair() const
    {
        return std::make_pair(_pid, _cancel_key);
    }

private:
    /** internal server auth state */
    enum State : int8_t {
        STARTUP = 0,
        SSL_HANDSHAKE = 1,
        AUTH = 2,
        AUTH_DONE = 3,
        READY = 4,
        ERROR = 99
    };

    /** Map of state to state names */
    const static inline std::unordered_map<State, std::string_view> _state_names{
        { State::STARTUP,         "STARTUP" },
        { State::SSL_HANDSHAKE,   "SSL_HANDSHAKE" },
        { State::AUTH,            "AUTH" },
        { State::AUTH_DONE,       "AUTH_DONE" },
        { State::READY,           "READY" },
        { State::ERROR,           "ERROR" }
    };

    /**
     * @brief Convert state to state name
     *
     * @param s - state
     * @return std::string - state name
     */
    static inline std::string
    _to_string(State s)
    {
        return state_to_string(s, _state_names);
    }

    State _state;
    ProxyConnectionPtr _connection;
    uint64_t _id;

    UserPtr _user;
    std::string _database;
    std::string _db_prefix;
    UserLoginPtr _login;
    Session::Type _type;

    int32_t _pid;
    std::vector<uint8_t> _cancel_key;

    std::string _error_code;
    std::string _error_message;

    /** parameters from client session */
    std::unordered_map<std::string, std::string> _parameters;

    /** parameters received by server */
    std::unordered_map<std::string, std::string> _server_parameters;

    /** handle response from server */
    void _handle_message(uint64_t seq_id);
    /** handle ssl response */
    void _handle_ssl_response(uint64_t seq_id);
    /** do ssl connect */
    void _handle_ssl_handshake(uint64_t seq_id);
    /** process authentication type message */
    void _handle_auth(BufferPtr buffer, uint64_t seq_id);
    /** md5 authentication handler */
    void _handle_auth_md5(BufferPtr buffer, uint64_t seq_id);
    /** scram authentication entry */
    void _handle_auth_scram(BufferPtr buffer, uint64_t seq_id);
    /** scram continuation */
    void _handle_auth_scram_continue(BufferPtr buffer, uint64_t seq_id);
    /** scram completion */
    void _handle_auth_scram_complete(BufferPtr buffer);

    /** send ssl requestion yes/no */
    void _send_ssl_req(uint64_t seq_id);
    /** send startup message */
    void _send_startup_msg(uint64_t seq_id);
    /** accept ssl response, start ssl connection */
    void _send_ssl_handshake(uint64_t seq_id);

    /** helper to send a buffer */
    void _send_buffer(BufferPtr buffer, uint64_t seq_id, char code);
};
using ServerAuthorizationPtr = std::shared_ptr<ServerAuthorization>;

}  // namespace springtail::pg_proxy