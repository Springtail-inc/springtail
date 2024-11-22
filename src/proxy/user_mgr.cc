#include <memory>

#include <common/properties.hh>
#include <pg_repl/libpq_connection.hh>
#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>
#include <proxy/exception.hh>

namespace springtail {
namespace pg_proxy {

    static constexpr char USER_SELECT[] = "select username, password, databases from public.get_user_access()";
    static std::vector<std::string> AUTH_TYPE_TO_STR = {
        "TRUST",
        "MD5",
        "SCRAM"
    };

    User::User(const std::string &username,
               const std::string &password)
        : _username(username),
          _password(password),
          _salt(0)
    {
        set_password(password);
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Created new {} user user: {}, password: {}, salt: {}",
                                        AUTH_TYPE_TO_STR[_auth_type], _username, _password, _salt);
    }

    void User::set_password(const std::string &password) {
        std::unique_lock lock(_user_mutex);
        _salt = 0;
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
            get_random_bytes((uint8_t*)&_salt, 4);
            _auth_type = MD5;
        } else {
            _auth_type = TRUST;
        }
    }

    UserLoginPtr
    User::get_user_login() const
    {
        UserLoginPtr login = std::make_shared<UserLogin>(_auth_type, _password, _salt);

        // if scram type copy both the keys, although only serverkey may be set
        if (_auth_type == SCRAM) {
            std::shared_lock lock(_user_mutex);
            memcpy(login->scram_state.ServerKey, _scram_keys->server_key, SCRAM_KEY_LEN);
            memcpy(login->scram_state.ClientKey, _scram_keys->client_key, SCRAM_KEY_LEN);
        }
        return login;
    }

    void
    User::set_client_scram_key(const uint8_t *client_key)
    {
        std::unique_lock lock(_user_mutex);
        if (!_scram_keys) {
            _scram_keys = std::make_shared<ScramKeys>();
        }
        if (!_scram_keys->client_key_set) {
            memcpy(_scram_keys->client_key, client_key, 32);
            _scram_keys->client_key_set = true;
        }
    }

    void
    User::set_server_scram_key(const uint8_t *server_key)
    {
        std::unique_lock lock(_user_mutex);
        if (!_scram_keys) {
            _scram_keys = std::make_shared<ScramKeys>();
        }
        if (!_scram_keys->server_key_set) {
            memcpy(_scram_keys->server_key, server_key, 32);
            _scram_keys->server_key_set = true;
        }
    }

    void UserMgr::_run() {
        _id = std::this_thread::get_id();

        springtail::LibPqConnection conn;
        while (!_is_shutting_down()) {
            while (!conn.is_connected()) {
                try {
                    // connect to primary database
                    std::string host, user, password;
                    int port;
                    Properties::get_primary_db_config(host, port, user, password);

                    auto db_name = _get_db_fn();
                    if (db_name.has_value()) {
                        conn.connect(host, db_name.value(), user, password, port, false);
                    } else {
                        SPDLOG_ERROR("No replicated database name is found");
                        throw ProxyError("No replicated database name found");
                    }
                } catch (Error &e) {
                    SPDLOG_ERROR("Failed to connect to primary database; will retry in {} seconds", _sleep_interval);
                    SPDLOG_ERROR("error message: {}\n", conn.error_message());
                    sleep(_sleep_interval);
                }
            }

            try {
                conn.exec(USER_SELECT);
            } catch (Error &e) {
                SPDLOG_ERROR(fmt::format("Failed to excute the query; will try to reconnect"));
                conn.disconnect();
                sleep(_sleep_interval);
                continue;
            }

            std::unique_lock lock(_mutex);
            // copy existing users set and clear out the set
            // we are going to move existing users back into the set
            // and add the new users, the rest will be discarded
            std::set<UserPtr, CompareUserByName> old_users = _users;
            _users.clear();
            for (int i = 0; i < conn.ntuples(); i++) {
                // get data from the query
                std::string username = conn.get_string(i, 0);
                std::string password = conn.get_string(i, 1);
                std::string databases = conn.get_string(i, 2);
                nlohmann::json db_names_json = nlohmann::json::parse(databases);
                std::set<std::string> database_set;
                assert(db_names_json.is_array());
                for (auto &db_name: db_names_json) {
                    assert(db_name.is_string());
                    database_set.insert(db_name.get<std::string>());
                }

                // extract user from the list of existing users
                UserPtr user = std::make_shared<User>(username);
                auto user_node = old_users.extract(user);
                if (!user_node.empty()) {
                    // the user exists, therefore update password if changed and database set
                    if (user_node.value()->password() != password) {
                        user_node.value()->set_password(password);
                    }
                    user_node.value()->set_databases(database_set);
                    _users.insert(std::move(user_node));
                } else {
                    // the user does not exist, add new user
                    _add_user(username, password, database_set);
                }
            }
            lock.unlock();
            conn.clear();

            sleep(_sleep_interval);
        }

        conn.disconnect();
    }
} // namespace pg_proxy
} // namespace springtail
