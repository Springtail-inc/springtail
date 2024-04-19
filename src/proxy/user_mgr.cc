#include <memory>

#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {

    User::User(UserMgrPtr user_mgr,
               const std::string &username,
               const std::string &database,
               const std::string &password,
               uint32_t salt)
        : _user_mgr(user_mgr),
          _username(username),
          _database(database),
          _password(password),
          _salt(salt)
    {
        if (_password.starts_with("SCRAM")) {
            _auth_type = SCRAM;
            _scram_keys = std::make_shared<ScramKeys>();

            int iters;
            char *saltp;
            uint8_t stored_key[SCRAM_KEY_LEN];

            // extract server key
            parse_scram_secret(_password.c_str(), &iters, &saltp, stored_key, _scram_keys->server_key);
            memset(_scram_keys->client_key, 0, SCRAM_KEY_LEN);

            free (saltp);
        } else if (_password.starts_with("md5")) {
            _auth_type = MD5;
        } else {
            _auth_type = TRUST;
        }
    }

    UserLoginPtr
    User::get_user_login()
    {
        UserLoginPtr login = std::make_shared<UserLogin>(_auth_type, _password, _salt);

        // if scram type copy both the keys, although only serverkey may be set
        if (_auth_type == SCRAM) {
            std::unique_lock lock(_scram_mutex);
            memcpy(login->scram_state.ServerKey, _scram_keys->server_key, SCRAM_KEY_LEN);
            memcpy(login->scram_state.ClientKey, _scram_keys->client_key, SCRAM_KEY_LEN);
        }
        return login;
    }

    const DatabasePtr
    User::get_database() const
    {
        return _user_mgr->get_database(_database);
    }
}