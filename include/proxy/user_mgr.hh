#pragma once

#include <memory>
#include <string>
#include <map>

#include <common/logging.hh>

#include <proxy/pool.hh>
#include <proxy/auth/scram.hh>
#include <proxy/auth/scram-common.h>

namespace springtail {

    enum AuthType : int8_t {
        TRUST=0,
        MD5=1,
        SCRAM=2
    };

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
            if (_type == SCRAM) {
                SPDLOG_DEBUG("Freeing scram state");
                free_scram_state(&scram_state);
            }
        }
    };
    using UserLoginPtr = std::shared_ptr<UserLogin>;

    class User {
    public:
        User(const std::string &username, const std::string &database)
            : _auth_type(TRUST),
              _username(username),
              _database(database)
        {}

        User(const std::string &username, const std::string &database,
             const std::string &password, uint32_t salt=0);

        /**
         * @brief Get the user login object containing creds of user
         * @return UserLoginPtr
         */
        UserLoginPtr get_user_login();

        void set_client_scram_key(const uint8_t *client_key)
        {
            if (!_scram_keys) {
                _scram_keys = std::make_shared<ScramKeys>();
            }
            memcpy(_scram_keys->ClientKey, client_key, 32);
        }

        void set_server_scram_key(const uint8_t *server_key)
        {
            if (!_scram_keys) {
                _scram_keys = std::make_shared<ScramKeys>();
            }
            memcpy(_scram_keys->ServerKey, server_key, 32);
        }

    private:
        struct ScramKeys {
            uint8_t ClientKey[SCRAM_KEY_LEN];
		    uint8_t ServerKey[SCRAM_KEY_LEN];
        };

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
    class UserMgr {
    public:
        UserMgr();

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
            _user_map[database][username] = std::make_shared<User>(username, database, password, salt);
        }

    private:
        std::map<std::string, std::map<std::string, UserPtr>> _user_map;
    };
    using UserMgrPtr = std::shared_ptr<UserMgr>;
}