#pragma once

#include <memory>
#include <string>
#include <map>
#include <set>
#include <shared_mutex>
#include <mutex>
#include <thread>

#include <common/logging.hh>

#include <proxy/connection.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>
// #include <proxy/server.hh>

namespace springtail {
namespace pg_proxy {

    enum AuthType : int8_t {
        TRUST=0,
        MD5=1,
        SCRAM=2
    };

    /**
     * @brief State describing the login credentials of a user.
     * Used by a session during authentication
     */
    struct UserLogin {
        AuthType _type;

        /** Scram state, freed in destructor */
        ScramState scram_state;

        /** password;
         * for SCRAM - SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
         * for MD5 - md5<bytes>
         */
        std::string _password;
        uint32_t    _salt;

        UserLogin(AuthType type=TRUST)
            : _type(type)
        {}

        UserLogin(AuthType type, const std::string &password, uint32_t salt=0)
            : _type(type),
              _password(password),
              _salt(salt)
        {}

        ~UserLogin() {
            // release the scram state pointers.  The keys are copied into the User object
            if (_type == SCRAM) {
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Freeing scram state");
                free_scram_state(&scram_state);
            }
        }
    };
    using UserLoginPtr = std::shared_ptr<UserLogin>;

    class UserMgr;
    using UserMgrPtr = std::shared_ptr<UserMgr>;
    using UserMgrConstPtr = std::shared_ptr<const UserMgr>;

    /**
     * @brief Identifies the user.
     * Holds credentials from the client login that may be required
     * for the server side login.
     */
    class User {
    public:
        /**
         * @brief Construct a new User object; trust authentication, no password
         * @param username users name
         * @param database database name
         */
        User(UserMgrConstPtr user_mgr,
             const std::string &username)
            : _user_mgr(user_mgr),
              _auth_type(TRUST),
              _username(username)
        {}

        /**
         * @brief Construct a new User object; md5 or scram authentication
         * @param username  users name
         * @param database  database name
         * @param password  password (md5<bytes> or SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:)
         * @param salt      optional salt for md5
         */
        User(UserMgrPtr user_mgr,
             const std::string &username,
             const std::string &password);

        /**
         * @brief Get the user login object containing creds of user
         * @return UserLoginPtr
         */
        UserLoginPtr get_user_login() const;

        /**
         * @brief Set the client scram key after client auth; used in server auth
         * @param client_key
         */
        void set_client_scram_key(const uint8_t *client_key);

        /**
         * @brief Set the server scram key; this should be based on the stored password
         * and should be set in the constructor when it parses the scram password.
         * @param server_key
         */
        void set_server_scram_key(const uint8_t *server_key);

        /** get username */
        const std::string &username() const { return _username; }

        /** add database */
        void add_database(const std::string &name) {
            std::unique_lock lock(_db_mutex);
            _connected_databases.insert(name);
        }

    private:
        struct ScramKeys {
            uint8_t client_key[SCRAM_KEY_LEN];
		    uint8_t server_key[SCRAM_KEY_LEN];
            bool client_key_set=false;
            bool server_key_set=false;
        };

        UserMgrConstPtr  _user_mgr; // XXX is this needed
        AuthType    _auth_type;

        std::string _username;
        std::string _password;
        uint32_t    _salt;

        mutable std::mutex _scram_mutex;
        std::shared_ptr<ScramKeys> _scram_keys;

        mutable std::shared_mutex _db_mutex;
        std::set<std::string> _connected_databases;
    };
    using UserPtr = std::shared_ptr<User>;

    class ProxyServer;
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

    /**
     * @brief Cache of user credentials. Queries Redis for creds.
     */
    class UserMgr : public std::enable_shared_from_this<UserMgr> {
    public:
        UserMgr(ProxyServerPtr proxy_server) : _proxy_server(proxy_server), _sleep_interval(5) {};

        void start() {
            _mgr_thread = std::thread(&UserMgr::_run, this);
        }

        /**
         * @brief Lookup user by username and database name
         * @param username user's name
         * @param database database name
         * @return UserPtr user or nullptr if not found
         */
        UserPtr get_user(const std::string &username, const std::string &database) const
        {
            UserPtr user = std::make_shared<User>(shared_from_this(), username);
            std::shared_lock lock(_mutex);
            auto it = _user_db_access.find(user);
            if (it != _user_db_access.end()) {
                if (it->second.contains(database)) {
                    return it->first;
                }
            }
            return nullptr;
        }

        /**
         * @brief Add new user to the user map
         * @param username users name
         * @param password password
         * @param salt     optional salt for md5
         */
        /*
        void add_user(const std::string &username,
                      const std::string &password={},
                      uint32_t salt=0,
                      const std::set<std::string> &databases)
        {
            std::unique_lock lock(_mutex);
            _add_user(username, password, databases);
        }
        */

        void shutdown() {
            SPDLOG_DEBUG("Stopping User Manager thread {}", _id);
            _shutdown = true;
            _mgr_thread.join();
            SPDLOG_DEBUG("Joined User Manager thread {}", _id);
            _shutdown = true;
        }

    private:
        struct CompareUserByName {
            bool operator()(const UserPtr &lhs, const UserPtr &rhs) const {
                return lhs->username() < rhs->username();
            }
        };

        ProxyServerPtr _proxy_server;
        mutable std::shared_mutex _mutex;

        /** maps username to User object and user object to list of databases*/
        std::map<UserPtr, std::set<std::string>, CompareUserByName> _user_db_access;

        std::thread _mgr_thread;
        std::thread::id _id;                    ///< user manager thread id
        uint32_t _sleep_interval;               ///< sleep interval in seconds
        std::atomic<bool> _shutdown;            ///< shudown flag

        void _add_user(const std::string &username,
                      const std::string &password,
                      const std::set<std::string> &databases) {
            UserPtr user = std::make_shared<User>(shared_from_this(),
                                username, password);
            _user_db_access[user] = databases;
        }

        void _run();
    };
    // using UserMgrPtr = std::shared_ptr<UserMgr>;
} // namespace pg_proxy
} // namespace springtail
