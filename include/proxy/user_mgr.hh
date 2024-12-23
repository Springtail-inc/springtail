#pragma once

#include <functional>
#include <memory>
#include <string>
#include <map>
#include <set>
#include <shared_mutex>
#include <mutex>
#include <thread>

#include <common/logging.hh>
#include <common/singleton.hh>

#include <proxy/connection.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>

namespace springtail {
namespace pg_proxy {

    /** Auth type, if this changes, change string mapping in user_mgr.cc */
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
        AuthType type;

        /** Scram state, freed in destructor */
        ScramState scram_state;

        /** password;
         * for SCRAM - SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
         * for MD5 - md5<bytes>
         */
        std::string password;
        uint32_t    salt;

        UserLogin(AuthType type=TRUST)
            : type(type)
        {}

        UserLogin(AuthType type, const std::string &password, uint32_t salt=0)
            : type(type),
              password(password),
              salt(salt)
        {
            if (type == SCRAM) {
                memset(&scram_state, 0, sizeof(scram_state));
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "new userlogin scram state: {:p}", (void *)&scram_state);
            }
        }

        ~UserLogin() {
            // release the scram state pointers.  The keys are copied into the User object
            if (type == SCRAM) {
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Freeing scram state: {:p}", (void *)&scram_state);
                free_scram_state(&scram_state);
            }
        }
    };
    using UserLoginPtr = std::shared_ptr<UserLogin>;

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
        User(const std::string &username)
            : _username(username),
              _auth_type(TRUST)
        {}

        /**
         * @brief Construct a new User object; md5 or scram authentication
         * @param username  users name
         * @param password  password (md5<bytes> or SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:)
         */
        User(const std::string &username,
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

        const std::string &password() const {
            std::shared_lock lock(_user_mutex);
            return _password;
        }

        /** add database */
        void add_database(const std::string &name) {
            std::unique_lock lock(_user_mutex);
            _connected_databases.insert(name);
        }

         /**
         * @brief Set user list of databases
         *
         * @param databases
         */
        void set_databases(const std::set<std::string> &databases) {
            std::unique_lock lock(_user_mutex);
            _connected_databases = databases;
        }

        /**
         * @brief Verify if the user is allowed to access database
         *
         * @param database - database name
         * @return true - allowed
         * @return false - not allowed
         */
        bool find_database(const std::string &database) {
            std::shared_lock lock(_user_mutex);
            if (_connected_databases.contains(database)) {
                return true;
            }
            return false;
        }

        /**
         * @brief Change user password
         *
         * @param password - password
         */
        void set_password(const std::string &password);
    private:
        struct ScramKeys {
            uint8_t client_key[SCRAM_KEY_LEN];
		    uint8_t server_key[SCRAM_KEY_LEN];
            bool client_key_set=false;
            bool server_key_set=false;
        };

        mutable std::shared_mutex _user_mutex;          ///< mutex for user data access
        std::shared_ptr<ScramKeys> _scram_keys;         ///< user scram keys
        std::set<std::string> _connected_databases;     ///< connected databases

        std::string _username;                          ///< username
        std::string _password;                          ///< password
        uint32_t    _salt;                              ///< salt
        AuthType    _auth_type;                         ///< authentication type
    };
    using UserPtr = std::shared_ptr<User>;

    /**
     * @brief Cache of user credentials. Queries Redis for creds.
     */
    class UserMgr final : public SingletonWithThread<UserMgr> {
    public:
        /**
         * @brief Initialize UserMgr object
         *
         * @param sleep_interval - UserMgr thread sleep interval in seconds
         */
        void init(const uint32_t sleep_interval) {
            _sleep_interval = sleep_interval;
            start_thread();
        }

        /**
         * @brief Lookup user by username and database name
         * @param username user's name
         * @param database database name
         * @return UserPtr user or nullptr if not found
         */
        UserPtr get_user(const std::string &username, const std::string &database) const
        {
            UserPtr user = std::make_shared<User>(username);
            std::shared_lock lock(_mutex);
            auto it = _users.find(user);
            if (it != _users.end()) {
                if ((*it)->find_database(database)) {
                    return *it;
                }
            }
            return nullptr;
        }

    protected:
        /**
         * @brief Stop function for stopping UserMgr thread
         *
         */
        void _internal_shutdown() override {
            SPDLOG_DEBUG("Stopping User Manager thread {}", _id);
        }

    private:
        friend class SingletonWithThread<UserMgr>;      ///< the base class should be friend
        /**
         * @brief Private constructor
         *
         */
        UserMgr() = default;
        /**
         * @brief Private destructor
         *
         */
        ~UserMgr() override = default;

        /**
         * @brief Comparison operator for ordering User objects by username inside the map container
         *
         */
        struct CompareUserByName {
            bool operator()(const UserPtr &lhs, const UserPtr &rhs) const {
                return lhs->username() < rhs->username();
            }
        };

        mutable std::shared_mutex _mutex;       ///< mutex for storage access
        std::set<UserPtr, CompareUserByName> _users; ///< collection of users

        std::thread::id _id;                    ///< user manager thread id
        uint32_t _sleep_interval;               ///< sleep interval in seconds

        /**
         * @brief Add new user to the user map
         * @param username users name
         * @param password password
         * @param databases list databases accessible to this user
         */
        void _add_user(const std::string &username,
                      const std::string &password,
                      const std::set<std::string> &databases) {
            UserPtr user = std::make_shared<User>(username, password);
            user->set_databases(databases);
            _users.insert(user);
        }

        /**
         * @brief Function executed by UserMgr thread
         *
         */
        void _internal_run() override;

    };
} // namespace pg_proxy
} // namespace springtail
