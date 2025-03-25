#pragma once

#include <memory>
#include <string>
#include <set>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <condition_variable>

#include <common/logging.hh>
#include <common/service_register.hh>
#include <common/singleton.hh>

#include <pg_repl/libpq_connection.hh>

#include <proxy/connection.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>

namespace springtail::pg_proxy {

    /** Password type, if this changes, change string mapping in user_mgr.cc */
    enum PasswordType : int8_t {
        INVALID=0,
        TEXT=1,
        MD5=2,
        SCRAM=3,
    };

    /**
     * @brief State describing the login credentials of a user.
     * Used by a session during authentication
     */
    struct UserLogin {
        PasswordType type;

        /** Scram state, freed in destructor */
        ScramState scram_state;

        /** password;
         * for SCRAM - SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
         * for MD5 - md5<bytes>
         */
        std::string password;
        uint32_t    salt;

        explicit UserLogin(PasswordType type=TEXT)
            : type(type)
        {
            memset(&scram_state, 0, sizeof(scram_state));
        }

        UserLogin(PasswordType type, const std::string &password, uint32_t salt=0)
            : type(type),
              password(password),
              salt(salt)
        {
            if (type == SCRAM || type == TEXT) {
                memset(&scram_state, 0, sizeof(scram_state));
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "new userlogin scram state: {:p}", (void *)&scram_state);
            }
        }

        ~UserLogin() {
            // release the scram state pointers.  The keys are copied into the User object
            if (type == SCRAM || type == TEXT) {
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
         * @brief Construct a new User object; primarily for testing or comparison
         * @param username  users name
         */
        explicit User(const std::string &username)
            : _username(username),
              _password_type(INVALID)
        {}

        /**
         * @brief Construct a new User object; md5 or scram authentication
         * @param username  users name
         * @param password  password (md5<bytes> or SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:)
         * @param type      password type
         */
        User(const std::string &username,
             const std::string &password,
             PasswordType type);

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

        /** get password */
        const std::string &password() const {
            std::shared_lock lock(_user_mutex);
            return _password;
        }

        /** get password type */
        PasswordType password_type() const {
            std::shared_lock lock(_user_mutex);
            return _password_type;
        }

        /**
         * @brief Add database to user's list of connected databases
         * @param name - database name
         */
        void add_database(const std::string &name) {
            std::unique_lock lock(_user_mutex);
            _connected_databases.insert(name);
        }

         /**
         * @brief Set user list of databases
         * @param databases
         */
        void set_databases(const std::set<std::string> &databases) {
            std::unique_lock lock(_user_mutex);
            _connected_databases = databases;
        }

        /**
         * @brief Verify if the user is allowed to access database
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
         * @param password - password
         */
        void set_password(const std::string &password, PasswordType type);

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

        std::string  _username;                          ///< username
        std::string  _password;                          ///< password
        uint32_t     _salt;                              ///< salt
        PasswordType _password_type;                     ///< type of stored password
    };
    using UserPtr = std::shared_ptr<User>;

    /**
     * @brief Cache of user credentials. Queries Redis for creds.
     */
    class UserMgr final : public SingletonWithThread<UserMgr> {
    public:
        /**
         * @brief Sleep interval for user manager thread
         *
         */
        static constexpr uint32_t USER_MGR_SLEEP_INTERVAL_SECS = 5;

        /**
         * @brief Initialize UserMgr object
         * @param sleep_interval - UserMgr thread sleep interval in seconds
         */
        void init(const uint32_t sleep_interval);

        void stop_thread() override {
            SingletonWithThread<UserMgr>::stop_thread();
            _sleep_cv.notify_all();
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

        /** The password string types used in the secrets mgr */
        static constexpr const char* PASSWORD_STRING_TEXT = "text";
        static constexpr const char* PASSWORD_STRING_MD5 = "md5";
        static constexpr const char* PASSWORD_STRING_SCRAM = "scram-sha-256";

        /**
         * @brief Private constructor
         */
        UserMgr() = default;
        /**
         * @brief Private destructor
         */
        ~UserMgr() override = default;

        /**
         * @brief Comparison operator for ordering User objects by username inside the map container
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

        std::mutex _sleep_mutex;                ///< mutex for sleep
        std::condition_variable _sleep_cv;      ///< condition variable for sleep

        bool _use_pg_shadow = false;            ///< use pg_shadow table for user updates

        /**
         * @brief Add new user to the user map
         * @param username users name
         * @param password password
         * @param type password type type of password, either TEXT, MD5, or SCRAM
         * @param databases list databases accessible to this user
         */
        void _add_user(const std::string &username,
                      const std::string &password,
                      PasswordType type,
                      const std::set<std::string> &databases) {
            UserPtr user = std::make_shared<User>(username, password, type);
            user->set_databases(databases);
            _users.insert(user);
        }

        /**
         * @brief Modify the users in the user map based on a new set of users
         * @param users list of users <username, password, databases>
         */
        void _modify_users(std::vector<std::tuple<std::string,  // username
                                                  std::string,  // password
                                                  PasswordType, // password type
                                                  std::set<std::string>> // databases
                                       > &users);

        /**
         * @brief Query users from pg_shadow table; if _use_pg_shadow is true
         */
        void _pg_shadow_query_thread();

        /**
         * @brief Query AWS secrets for user updates; if _use_pg_shadow is false
         */
        void _aws_secrets_query_thread();

        /**
         * @brief Function executed by UserMgr thread
         */
        void _internal_run() override;

        /**
         * @brief Connect to primary database
         * @param conn - connection object
         */
        void _connect_primary_db(LibPqConnection &conn);

    };

    class UserMgrRunner : public ServiceRunner {
    public:
        explicit UserMgrRunner(uint32_t sleep_interval) :
            ServiceRunner("UserMgr"),
            _sleep_interval(sleep_interval) {}

        ~UserMgrRunner() override = default;

        bool start() override
        {
            UserMgr::get_instance()->init(_sleep_interval);
            return true;
        }

        void stop() override
        {
            UserMgr::get_instance()->stop_thread();
            UserMgr::shutdown();
        }
    private:
        uint32_t _sleep_interval;
    };

} // namespace springtail::pg_proxy
