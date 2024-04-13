#include <memory>

#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>

namespace springtail {

    User::User(const std::string &username, const std::string &database,
               const std::string &password, uint32_t salt)
        : _username(username),
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
            parse_scram_secret(_password.c_str(), &iters, &saltp, stored_key, _scram_keys->ServerKey);
            memset(_scram_keys->ClientKey, 0, SCRAM_KEY_LEN);

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
            memcpy(login->scram_state.ServerKey, _scram_keys->ServerKey, SCRAM_KEY_LEN);
            memcpy(login->scram_state.ClientKey, _scram_keys->ClientKey, SCRAM_KEY_LEN);
        }
        return login;
    }

    UserMgr::UserMgr()
    {
        // add test user for test db with trust
        add_user("test", "test");

        // add test user for test db with md5
        std::string username = "test_md5";
        std::string passwd = "test";
        char md5[36]; // md5sum('pwd'+'user') = md5+digest
        pg_md5_encrypt(passwd.c_str(), username.c_str(), strlen(username.c_str()), md5);
        uint32_t salt;
        get_random_bytes((uint8_t*)&salt, 4);
        add_user("test_md5", "test", md5, salt);

        // add user for test db with scram
        add_user("test_scram", "test", "SCRAM-SHA-256$4096:ELqGVsjLPt+bQ4cm7iyV3g==$5/DxDP2LghUcln0Xkkzq+8SDjC7AmJ6NLwt7lW1/ilY=:HBf0FAcuI5FNmasZ6qGZtKVkeGaGeLbYjFDd77tzBEk=");
    }
}