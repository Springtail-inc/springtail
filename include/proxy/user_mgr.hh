#pragma once

#include <memory>
#include <string>
#include <map>

#include <common/logging.hh>

#include <proxy/pool.hh>
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

    /** Class representing a database, includes database name and hostname DNS */
    class Database {
    public:
        Database(const std::string &name,
                 const std::string &hostname,
                 int port=5432)
            : _name(name), _hostname(hostname), _port(port)
        {}

        /** get database name */
        const std::string &name() const { return _name; }

        /** get hostname */
        const std::string &hostname() const { return _hostname; }

        /** get port */
        int port() const { return _port; }

        /** create connection */
        ProxyConnectionPtr create_connection() {
            return ProxyConnection::create(_hostname, _port);
        }

    private:
        std::string _name;
        std::string _hostname;
        int _port;

        // XXX may want to cache dns lookups
    };
    using DatabasePtr = std::shared_ptr<Database>;

    class UserMgr;
    using UserMgrPtr = std::shared_ptr<UserMgr>;

    /**
     * @brief Identifies the user as a user:database pair.
     * Holds credentials from the client login that may be required
     * for the server side login.  Contains the pool that holds
     * the client session, the primary and the replica sessions.
     */
    class User {
    public:
        /**
         * @brief Construct a new User object; trust authentication, no password
         * @param username users name
         * @param database database name
         */
        User(UserMgrPtr user_mgr,
             const std::string &username,
             const std::string &database)
            : _user_mgr(user_mgr),
              _auth_type(TRUST),
              _username(username),
              _database(database)
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
             const std::string &database,
             const std::string &password,
             uint32_t salt=0);

        /**
         * @brief Get the user login object containing creds of user
         * @return UserLoginPtr
         */
        UserLoginPtr get_user_login();

        /**
         * @brief Set the client scram key after client auth; used in server auth
         * @param client_key
         */
        void set_client_scram_key(const uint8_t *client_key)
        {
            if (!_scram_keys) {
                _scram_keys = std::make_shared<ScramKeys>();
            }
            memcpy(_scram_keys->ClientKey, client_key, 32);
        }

        /**
         * @brief Set the server scram key; this should be based on the stored password
         * and should be set in the constructor when it parses the scram password.
         * @param server_key
         */
        void set_server_scram_key(const uint8_t *server_key)
        {
            if (!_scram_keys) {
                _scram_keys = std::make_shared<ScramKeys>();
            }
            memcpy(_scram_keys->ServerKey, server_key, 32);
        }

        /** get username */
        const std::string &username() const { return _username; }

        /** get databese name */
        const std::string &dbname() const { return _database; }

        /** get database object */
        const DatabasePtr get_database() const;

    private:
        struct ScramKeys {
            uint8_t ClientKey[SCRAM_KEY_LEN];
		    uint8_t ServerKey[SCRAM_KEY_LEN];
        };

        UserMgrPtr  _user_mgr;

        AuthType    _auth_type;

        std::string _username;
        std::string _database;
        std::string _password;
        uint32_t    _salt;

        Pool        _pool;

        std::shared_ptr<ScramKeys> _scram_keys;
    };
    using UserPtr = std::shared_ptr<User>;

    /**
     * @brief Cache of user credentials. Queries Redis for creds.
     */
    class UserMgr : public std::enable_shared_from_this<UserMgr> {
    public:
        UserMgr();

        /**
         * @brief Lookup user by username and database name
         * @param username user's name
         * @param database database name
         * @return UserPtr user or nullptr if not found
         */
        UserPtr get_user(const std::string &username, const std::string &database)
        {
            auto it = _user_map.find(database);
            if (it != _user_map.end()) {
                auto it2 = it->second.find(username);
                if (it2 != it->second.end()) {
                    return it2->second;
                }
            }
            return nullptr;
        }

        /**
         * @brief Add new user to the user map
         * @param username users name
         * @param database database name
         * @param password password
         * @param salt     optional salt for md5
         */
        void add_user(const std::string &username, const std::string &database,
                      const std::string &password={}, uint32_t salt=0)
        {
            auto it = _user_map.find(database);
            if (it == _user_map.end()) {
                _user_map[database] = std::map<std::string, UserPtr>();
            } else {
                auto it2 = it->second.find(username);
                if (it2 != it->second.end()) {
                    return;
                }
            }
            // user not found, add user to the map
            _user_map[database][username] = std::make_shared<User>(shared_from_this(), username,
                                                                   database, password, salt);
        }

        /**
         * @brief Add new database to the database map
         * @param name database name
         * @param hostname hostname of the database
         * @param port port of the database
         */
        DatabasePtr get_database(const std::string &name)
        {
            auto it = _database_map.find(name);
            if (it != _database_map.end()) {
                return it->second;
            }
            return nullptr;
        }

    private:
        /** user map from database -> map username -> UserPtr */
        std::map<std::string, std::map<std::string, UserPtr>> _user_map;

        /** database map from db name to database object */
        std::map<std::string, DatabasePtr> _database_map;
    };
    using UserMgrPtr = std::shared_ptr<UserMgr>;
}