#include <memory>

#include <common/properties.hh>
#include <common/json.hh>
#include <common/aws.hh>
#include <pg_repl/libpq_connection.hh>
#include <proxy/database.hh>
#include <proxy/user_mgr.hh>
#include <proxy/auth/md5.h>
#include <proxy/auth/scram.hh>
#include <proxy/exception.hh>

namespace springtail::pg_proxy {

    /** SELECT query for fetching users from primary for authentication */
    static constexpr char USER_SELECT[] = "select username, password, databases from __pg_springtail_triggers.springtail_get_user_access();";

    static const std::vector<std::string> AUTH_TYPE_TO_STR = {
        "TRUST",
        "MD5",
        "SCRAM"
    };

    User::User(const std::string &username,
               const std::string &password)
        : _username(username),
          _salt(0)
    {
        set_password(password);
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Created new {} user user: {}, password: {}, salt: {}",
                            AUTH_TYPE_TO_STR[_auth_type], _username, _password, _salt);
    }

    void User::set_password(const std::string &password) {
        std::unique_lock lock(_user_mutex);
        _password = password;
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
        std::shared_lock lock(_user_mutex);
        UserLoginPtr login = std::make_shared<UserLogin>(_auth_type, _password, _salt);

        // if scram type copy both the keys, although only serverkey may be set
        if (_auth_type == SCRAM) {
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

    void
    UserMgr::init(uint32_t sleep_interval)
    {
        _sleep_interval = sleep_interval;

        // set the user manager to use pg_shadow or AWS secrets
        nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
        _use_pg_shadow = Json::get_or<bool>(json, "use_pg_shadow", false);

        SPDLOG_INFO("UserMgr setup to use {}", _use_pg_shadow ? "pg_shadow" : "AWS secrets");

        start_thread();
    }

    void
    UserMgr::_internal_run()
    {
        _id = std::this_thread::get_id();
        if (_use_pg_shadow) {
            _pg_shadow_query_thread();
        } else {
            _aws_secrets_query_thread();
        }
    }

    void
    UserMgr::_aws_secrets_query_thread()
    {
        // setup aws secrets manager
        AwsHelperPtr aws_helper = std::make_shared<AwsHelper>();
        std::string key = fmt::format(AwsHelper::DB_USERS_SECRET, Properties::get_organization_id(),
                                      Properties::get_account_id(), Properties::get_db_instance_id());

        while (!_is_shutting_down()) {
            try {

                // get the user credentials from AWS secrets manager
                nlohmann::json secret = aws_helper->get_secret(key);
                CHECK(secret.is_array());

                auto database_map = Properties::get_databases();

                std::vector<std::tuple<std::string, std::string, std::set<std::string>>> users;
                for (auto &user: secret) {
                    std::string username = user["username"];
                    std::string password = user["password"];
                    std::set<std::string> databases;
                    // right now no databases are set in the secret
                    // so we are adding all databases
                    for (auto &db: database_map) {
                        databases.insert(db.second);
                    }
                    users.emplace_back(username, password, databases);
                }

                // update the user map
                _modify_users(users);

            } catch (Error &e) {
                SPDLOG_ERROR("Failed to get users from AWS secrets manager; will retry in {} seconds", _sleep_interval);
            }

            {
                std::unique_lock sleep_lock(_sleep_mutex);
                _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
            }
        }
    }

    void
    UserMgr::_pg_shadow_query_thread()
    {
        springtail::LibPqConnection conn;
        while (!_is_shutting_down()) {
            while (!conn.is_connected()) {
                try {
                    // connect to primary database
                    std::string host, user, password;
                    int port;
                    Properties::get_primary_db_config(host, port, user, password);

                    auto db_name = DatabaseMgr::get_instance()->get_any_replicated_db_name();
                    if (db_name.has_value()) {
                        conn.connect(host, db_name.value(), user, password, port, false);
                    } else {
                        SPDLOG_ERROR("No replicated database name is found");
                        throw ProxyError("No replicated database name found");
                    }
                } catch (Error &e) {
                    SPDLOG_ERROR("Failed to connect to primary database; will retry in {} seconds", _sleep_interval);
                    SPDLOG_ERROR("error message: {}\n", conn.error_message());
                    std::unique_lock sleep_lock(_sleep_mutex);
                    _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
                }
            }

            try {
                conn.exec(USER_SELECT);
            } catch (Error &e) {
                SPDLOG_ERROR(fmt::format("Error: {}. Failed to excute the query; will try to reconnect", e.what()));
                conn.disconnect();
                std::unique_lock sleep_lock(_sleep_mutex);
                _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
                continue;
            }

            // list of users from the query
            // username, password, databases (set)
            std::vector<std::tuple<std::string, std::string, std::set<std::string>>> users;

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
                users.emplace_back(username, password, database_set);
            }
            conn.disconnect();

            _modify_users(users);

            { // iteruptible sleep
                std::unique_lock sleep_lock(_sleep_mutex);
                _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
            }
        } // end while
    }

    void
    UserMgr::_modify_users(std::vector<std::tuple<std::string, std::string, std::set<std::string>>> &users)
    {
        std::unique_lock lock(_mutex);
        // copy existing users set and clear out the set
        // we are going to move existing users back into the set
        // and add the new users, the rest will be discarded
        std::set<UserPtr, CompareUserByName> old_users = _users;
        _users.clear();

        for (auto &[username, password, database_set]: users) {
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
    }
} // namespace springtail::pg_proxy
