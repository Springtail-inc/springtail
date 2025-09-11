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

    static constexpr char USER_DB_SELECT[] = "SELECT r.rolname AS username, d.datname AS database_name "
        "FROM pg_database d "
        "CROSS JOIN pg_roles r "
        "WHERE has_database_privilege(r.rolname, d.datname, 'CONNECT') "
        "AND r.rolname IN ({}) "
        "AND d.datistemplate = false;";

    static const std::vector<std::string> PASSWORD_TYPE_TO_STR = {
        "INVALID",
        "TEXT",
        "MD5",
        "SCRAM"
    };

    User::User(const std::string &username,
               const std::string &password,
               PasswordType type)
        : _username(username),
          _salt(0)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Creating new {} user user: {}, password: XXXX, salt: {}",
            PASSWORD_TYPE_TO_STR[type], _username, _salt);
        set_password(password, type);
    }

    void
    User::set_password(const std::string &password, PasswordType type)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Setting password for user: {}, type: {}", _username, PASSWORD_TYPE_TO_STR[type]);

        std::unique_lock lock(_user_mutex);
        _password = password;
        _salt = 0;
        _password_type = type;

        switch (type) {
            case SCRAM: {
                _scram_keys = std::make_shared<ScramKeys>();

                int iters;
                char *saltp = nullptr;
                uint8_t stored_key[SCRAM_KEY_LEN];

                // extract server key
                if (!parse_scram_secret(_password.c_str(), &iters, &saltp, stored_key, _scram_keys->server_key)) {
                    LOG_ERROR("Failed to parse SCRAM secret for user: {}", _username);
                    _password_type = INVALID;
                    return;
                }
                _scram_keys->server_key_set = true;

                memset(_scram_keys->client_key, 0, SCRAM_KEY_LEN);

                free (saltp);
                break;
            }

            case MD5:
                get_random_bytes((uint8_t*)&_salt, 4);
                break;

            case TEXT:
            case INVALID:
                break;
        }
    }

    UserLoginPtr
    User::get_user_login() const
    {
        std::shared_lock lock(_user_mutex);
        UserLoginPtr login = std::make_shared<UserLogin>(_username, _password_type, _password, _salt);

        // if scram type copy both the keys, although only serverkey may be set
        if (_password_type == SCRAM) {
            CHECK_NE(_scram_keys, nullptr);
            CHECK(_scram_keys->server_key_set);
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
    UserMgr::_init()
    {
        // set the user manager to use pg_shadow or AWS secrets
        nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
        _sleep_interval = Json::get_or<uint32_t>(json, "user_mgr_sleep_interval_secs", USER_MGR_SLEEP_INTERVAL_SECS);

        LOG_INFO("UserMgr setup to use AWS secrets, user_mgr_sleep_interval_secs = {}",
                _sleep_interval);

        start_thread();
    }

    void
    UserMgr::_internal_run()
    {
        _id = std::this_thread::get_id();
        _aws_secrets_query_thread();
    }

    void
    UserMgr::_aws_secrets_query_thread()
    {
        // setup aws secrets manager
        AwsHelperPtr aws_helper = std::make_shared<AwsHelper>();
        std::string key = fmt::format(AwsHelper::DB_USERS_SECRET, Properties::get_organization_id(),
                                      Properties::get_account_id(), Properties::get_db_instance_id());

        LibPqConnection conn;

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Starting AWS secrets query thread");
        while (!_is_shutting_down()) {
            try {
                // get the user credentials from AWS secrets manager
                nlohmann::json secret = aws_helper->get_secret(key);
                CHECK(secret.is_array());

                // try to connect to database, may block if db is down
                _connect_primary_db(conn);

                std::ostringstream user_list;

                // create map of username to password from secrets
                std::map<std::string, std::pair<std::string, PasswordType>> user_password_map;
                for (auto &user: secret) {
                    std::string username = user["username"];
                    std::string password = user["password"];
                    std::string role = user["role"];
                    std::string type = user["type"];

                    LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Found user: {}, {}, {}", username, role, type);

                    // only add users with role database
                    if (role != "database") {
                        continue;
                    }

                    PasswordType password_type;
                    if (type == PASSWORD_STRING_TEXT) {
                        password_type = TEXT;
                    } else if (type == PASSWORD_STRING_MD5) {
                        password_type = MD5;
                    } else if (type == PASSWORD_STRING_SCRAM) {
                        password_type = SCRAM;
                    } else {
                        LOG_WARN("Unknown password type: {}", type);
                        continue;
                    }

                    user_password_map[username] = std::pair{password, password_type};

                    // add user to user_list with delimiter
                    user_list << "'" << conn.escape_string(username) << "',";
                }

                if (user_password_map.empty()) {
                    LOG_WARN("No users found in AWS secrets manager");
                    continue;
                }

                // remove the last comma
                std::string user_list_str = user_list.str();
                user_list_str.pop_back();

                // do query to fetch databases for each user
                std::string query = fmt::format(USER_DB_SELECT, user_list_str);
                conn.exec(query);

                // list of users from the query insert into map
                std::map<std::string, std::set<std::string>> user_db_map;
                for (int i = 0; i < conn.ntuples(); i++) {
                    std::string username = conn.get_string(i, 0);
                    std::string database = conn.get_string(i, 1);
                    user_db_map[username].insert(database);
                }

                conn.clear();
                conn.disconnect();

                // construct vector of users for the user map
                std::vector<std::tuple<std::string, std::string, PasswordType, std::set<std::string>>> users;
                for (auto &[username, databases] : user_db_map) {
                    auto it = user_password_map.find(username);
                    if (it != user_password_map.end()) {
                        // username, password, password type, databases
                        users.emplace_back(username, it->second.first, it->second.second, databases);
                    }
                }

                // update the user map
                _modify_users(users);

            } catch (Error &e) {
                LOG_ERROR("Failed to get users from AWS secrets manager; will retry in {} seconds", _sleep_interval);
            } catch (std::exception &e) {
                LOG_ERROR("Error: {}. Failed to execute the query; will try to reconnect", e.what());
            }

            {
                std::unique_lock sleep_lock(_sleep_mutex);
                _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
            }
        }
    }

    void
    UserMgr::_connect_primary_db(LibPqConnection &conn)
    {
        while (!conn.is_connected() && !_is_shutting_down()) {
            try {
                // connect to primary database
                std::string host, user, password;
                int port;
                Properties::get_primary_db_config(host, port, user, password);

                auto db_name = DatabaseMgr::get_instance()->get_any_replicated_db_name();
                if (db_name.has_value()) {
                    conn.connect(host, db_name.value(), user, password, port, false);
                } else {
                    LOG_ERROR("No replicated database name is found");
                    throw ProxyError("No replicated database name found");
                }
            } catch (Error &e) {
                LOG_ERROR("Failed to connect to primary database; will retry in {} seconds", _sleep_interval);
                LOG_ERROR("error message: {}", conn.error_message());
                std::unique_lock sleep_lock(_sleep_mutex);
                _sleep_cv.wait_for(sleep_lock, std::chrono::seconds(_sleep_interval));
            }
        }
    }

    void
    UserMgr::_modify_users(std::vector<std::tuple<std::string, std::string, PasswordType, std::set<std::string>>> &users)
    {
        std::unique_lock lock(_mutex);
        // copy existing users set and clear out the set
        // we are going to move existing users back into the set
        // and add the new users, the rest will be discarded
        std::set<UserPtr, CompareUserByName> old_users = _users;
        _users.clear();

        for (auto &[username, password, type, database_set]: users) {
            // extract user from the list of existing users
            UserPtr user = std::make_shared<User>(username);
            auto user_node = old_users.extract(user);
            if (!user_node.empty()) {
                // the user exists, therefore update password if changed and database set
                if (user_node.value()->password() != password || user_node.value()->password_type() != type) {
                    user_node.value()->set_password(password, type);
                }
                user_node.value()->set_databases(database_set);
                _users.insert(std::move(user_node));
            } else {
                // the user does not exist, add new user
                _add_user(username, password, type, database_set);
            }
        }
    }

    UserPtr
    UserMgr::get_user(const std::string &username, const std::string &database) const
    {
        UserPtr user = std::make_shared<User>(username);
        std::shared_lock lock(_mutex);

        auto it = _users.find(user);
        if (it != _users.end()) {
            if ((*it)->find_database(database)) {
                return *it;
            } else {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "User {} not allowed to access database {}", username, database);
            }
        } else {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "User {} not found", username);
        }

        return nullptr;
    }

} // namespace springtail::pg_proxy
