#pragma once

#include <memory>
#include <string>
#include <map>
#include <set>
#include <shared_mutex>
#include <mutex>

#include <common/logging.hh>

#include <proxy/connection.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>

namespace springtail {

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
                SPDLOG_DEBUG("Freeing scram state");
                free_scram_state(&scram_state);
            }
        }
    };
    using UserLoginPtr = std::shared_ptr<UserLogin>;

    class UserMgr;
    using UserMgrPtr = std::shared_ptr<UserMgr>;

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
        User(UserMgrPtr user_mgr,
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
             const std::string &password,
             uint32_t salt=0);

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

        UserMgrPtr  _user_mgr; // XXX is this needed
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

    /**
     * @brief Cache of user credentials. Queries Redis for creds.
     */
    class UserMgr : public std::enable_shared_from_this<UserMgr> {
    public:
        UserMgr() {};

        /**
         * @brief Lookup user by username and database name
         * @param username user's name
         * @return UserPtr user or nullptr if not found
         */
        UserPtr get_user(const std::string &username) const
        {
            std::shared_lock lock(_mutex);
            auto it = _user_map.find(username);
            if (it != _user_map.end()) {
                return it->second;
            }
            return nullptr;
        }

        /**
         * @brief Add new user to the user map
         * @param username users name
         * @param password password
         * @param salt     optional salt for md5
         */
        void add_user(const std::string &username,
                      const std::string &password={},
                      uint32_t salt=0)
        {
            std::unique_lock lock(_mutex);
            auto it = _user_map.find(username);
            if (it == _user_map.end()) {
                _user_map[username] = std::make_shared<User>(shared_from_this(),
                                                             username,
                                                             password, salt);
            }
        }

    private:
        mutable std::shared_mutex _mutex;

        /** user map from user to User object */
        std::map<std::string, UserPtr> _user_map;
    };
    using UserMgrPtr = std::shared_ptr<UserMgr>;
}