#include <common/counter.hh>
#include <common/common.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/constants.hh>

#include <redis/db_state_change.hh>
#include <redis/redis_ddl.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/libpq_connection.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <sys_tbl_mgr/system_tables.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_ddl_mgr.hh>

#include <pg_fdw/constants.hh>

namespace springtail::pg_fdw {

    /** Get a list of all non-system schema names */
    static constexpr char SCHEMA_SELECT[] =
        "SELECT schema_name "
        "FROM information_schema.schemata "
        "WHERE schema_name NOT LIKE 'pg_%' "
        " AND schema_name <> 'information_schema'";

    static constexpr char CREATE_USER[] =
        "CREATE USER {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{}' IN ROLE pg_read_all_data";

    static constexpr char DROP_DATABASE[] =
        "DROP DATABASE IF EXISTS {} WITH (FORCE)";

    static constexpr char VERIFY_DB_EXISTS[] =
        "SELECT 1 FROM  pg_database WHERE datname = '{}'";

    static constexpr char ALTER_TABLE_RLS[] =
        "ALTER FOREIGN TABLE {}.{} "
        "  ENABLE ROW LEVEL SECURITY {}";

    static constexpr char POLICY_DIFF_SELECT[] =
        "SELECT diff_type, table_name, schema_name, "
        "       policy_oid, policy_name, "
        "       policy_permissive, policy_roles, policy_qual "
        "FROM __pg_springtail_triggers.policy_diff('{}');";

    static constexpr char ROLE_DIFF_SELECT[] =
        "SELECT diff_type, rolname, rolsuper, "
        "    rolinherit, rolcanlogin, rolbypassrls, rolconfig, rolconfig_changed "
        "FROM __pg_springtail_triggers.role_diff('{}');";

    static constexpr char ROLE_MEMBER_DIFF_SELECT[] =
        "SELECT diff_type, role_name, member_name "
        "FROM __pg_springtail_triggers.role_member_diff('{}');";

    static constexpr char TABLE_OWNER_DIFF_SELECT[] =
        "SELECT diff_type, table_name, schema_name, "
        "       role_owner_name, table_oid "
        "FROM __pg_springtail_triggers.table_owner_diff('{}');";


    PgDDLMgr::PgDDLMgr() :
        _fdw_conn_cache(MAX_CONNECTION_CACHE_SIZE, [](LibPqConnectionPtr conn) -> bool {
            // evict callback for the connection cache
            conn->disconnect();
            return true;
        })
    {
        // initialize the cache watcher
        _cache_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string& path, const nlohmann::json& new_value) {
                _on_database_ids_changed(path, new_value);
            }
        );
    }

    void PgDDLMgr::_on_database_ids_changed(const std::string &path,
                                            const nlohmann::json &new_value)
    {
        LOG_DEBUG(LOG_FDW, "Replicated databases: {}", new_value.dump(4));
        CHECK_EQ(path, Properties::DATABASE_IDS_PATH);

        // get a vector of old database ids from _log_mgrs
        std::shared_lock<std::shared_mutex> lock(_db_mutex);
        auto keys = std::views::keys(_db_xid_map);
        lock.unlock();
        std::vector<uint64_t> old_db_ids{ keys.begin(), keys.end() };

        // get a vector of new database ids from new_value
        std::vector<uint64_t> new_db_ids = Properties::get_instance()->get_database_ids(new_value);

        // diff the vectors
        RedisCache::array_diff(old_db_ids, new_db_ids, true, true);

        // everything in old_db_ids needs to be removed
        for (auto db_id: old_db_ids) {
            _remove_replicated_database(db_id);
        }

        // everything in new_db_ids needs to be added
        for (auto db_id: new_db_ids) {
            _add_replicated_database(db_id);
        }
    }

    void
    PgDDLMgr::_internal_shutdown() {
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        _sync_thread.join();
    }

    void
    PgDDLMgr::init(const std::string &fdw_id,
                   const std::string &username,
                   const std::string &password,
                   const std::optional<std::string> &hostname)
    {
        // set fdw id
        _fdw_id = fdw_id;

        // set username and password for ddl mgr user
        // this user has more permissions than the fdw user
        _username = username;
        _password = password;

        // fetch config for fdw (host, port, user, password)
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(fdw_id);

        // get the connection information from the FDW config
        // override hostname if passed in, used for unix domain socket connections
        if (hostname.has_value()) {
            _hostname = hostname.value();
        } else {
            Json::get_to<std::string>(fdw_config, "host", _hostname);
        }
        Json::get_to<int>(fdw_config, "port", _port);

        // get fdw user for proxy to use, this user only has select permissions
        Json::get_to<std::string>(fdw_config, "fdw_user", _fdw_username);
        Json::get_to<std::string>(fdw_config, "password", _fdw_password);

        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            _db_prefix = fdw_config.at("db_prefix").get<std::string>();
        }

        LOG_DEBUG(LOG_FDW, "FDW ID: {}, Host: {}, Port: {}, Username: {}, FDW Username: {}",
                     _fdw_id, _hostname, _port, _username, _fdw_username);

        // add subscribers to pubsub threads
        _db_instance_id = Properties::get_db_instance_id();

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        _init_fdw();

        _thread_manager = std::make_shared<common::MultiQueueThreadManager>(MAX_THREAD_POOL_SIZE);
        _thread_manager->start();

        // create a new thread to run the policy and role sync
        _sync_thread = std::thread(&PgDDLMgr::_sync_thread_func, this);
    }

    void
    PgDDLMgr::_policy_sync_database(LibPqConnectionPtr conn,
                                    std::vector<std::string> &sql_commands)
    {
        // execute on primary
        conn->exec(fmt::format(POLICY_DIFF_SELECT, _fdw_id));

        if (conn->ntuples() == 0) {
            conn->clear();
            return;
        }

        // iterate through the result set and process the policy diffs
        for (int i = 0; i < conn->ntuples(); i++) {
            std::string diff_type = conn->get_string(i, 0);
            std::string table_name = conn->escape_identifier(conn->get_string(i, 1));
            std::string schema_name = conn->escape_identifier(conn->get_string(i, 2));
            std::string policy_name = conn->escape_identifier(conn->get_string(i, 3));
            auto policy_name_old = conn->get_string_optional(i, 4);

            if (diff_type == "REMOVED") {
                sql_commands.push_back(
                    fmt::format("DROP POLICY IF EXISTS {} ON {}.{}",
                                policy_name, schema_name, table_name));
                continue;
            }

            // these are only valid for ADDED and MODIFIED diffs
            bool policy_permissive = conn->get_boolean(i, 5);
            std::string policy_roles = conn->get_string(i, 6);
            std::string policy_qual = conn->get_string(i, 7);

            // convert roles to {} from json array string
            nlohmann::json roles_json = nlohmann::json::parse(policy_roles);
            std::vector<std::string> roles;
            for (auto &role : roles_json) {
                roles.push_back(conn->escape_identifier(role.get<std::string>()));
            }
            policy_roles = fmt::format("{}", common::join_string(", ", roles));

            if (diff_type == "ADDED") {
                sql_commands.push_back(
                    fmt::format("DROP POLICY IF EXISTS {} ON {}.{}",
                                policy_name, schema_name, table_name));

                // create the new policy
                sql_commands.push_back(
                    fmt::format("CREATE POLICY {} ON {}.{} "
                                "AS {} "
                                "FOR SELECT TO {} "
                                "USING ({})",
                                policy_name, schema_name, table_name, policy_permissive ? "PERMISSIVE" : "RESTRICTIVE",
                                policy_roles, policy_qual));
                continue;
            }

            if (diff_type == "MODIFIED") {
                // if the policy name has changed, we need to drop the old one
                if (policy_name_old.has_value() && policy_name_old.value() != policy_name) {
                    sql_commands.push_back(
                        fmt::format("ALTER POLICY {} ON {}.{} RENAME TO {}",
                                    policy_name_old.value(), schema_name, table_name, policy_name));
                }
                // modify the existing policy
                // XXX if roles/qual hasn't changed, we don't need to do anything
                sql_commands.push_back(
                    fmt::format("ALTER POLICY {} ON {}.{} "
                                "FOR SELECT TO {} USING ({})",
                                policy_name, schema_name, table_name,
                                policy_roles, policy_qual));
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_roles_sync_database(LibPqConnectionPtr conn,
                                   std::vector<std::string> &sql_commands)
    {
        // execute on primary
        conn->exec(fmt::format(ROLE_DIFF_SELECT, _fdw_id));

        if (conn->ntuples() == 0) {
            conn->clear();
            return;
        }

        // iterate through the result set and process the role diffs
        for (int i = 0; i < conn->ntuples(); i++) {
            std::string role = conn->get_string(i, 1);
            if (role == _fdw_username || role == _username) {
                // skip the fdw user and ddl mgr user, they are not replicated from primary
                LOG_DEBUG(LOG_FDW, "Skipping role {} as it is the fdw or ddl mgr user", role);
                continue;
            }
            std::string role_name = conn->escape_identifier(conn->get_string(i, 1));
            std::string diff_type = conn->get_string(i, 0);
            bool rolsuper = conn->get_boolean(i, 2);
            bool rolinherit = conn->get_boolean(i, 3);
            bool rolcanlogin = conn->get_boolean(i, 4);
            bool rolbypassrls = conn->get_boolean(i, 5);
            auto rolconfig = conn->get_string_optional(i, 6);
            bool rolconfig_changed = conn->get_boolean(i, 7);

            if (diff_type == "REMOVED") {
                sql_commands.push_back(fmt::format("DROP ROLE IF EXISTS {}", role_name));
                continue;
            }

            if (diff_type == "ADDED") {
                // create a new user with password, even if nologin is set
                // in case it changes later, we can control with LOGIN/NOLOGIN
                sql_commands.push_back(fmt::format(CREATE_USER, role_name, _fdw_password));
                // fall through
            }

            if (diff_type == "MODIFIED" || diff_type == "ADDED") {
                // modify the existing role
                sql_commands.push_back(
                    fmt::format("ALTER ROLE {} WITH {} {} {}",
                                role_name,
                                (rolsuper || rolbypassrls) ? "BYPASSRLS" : "NOBYPASSRLS",
                                rolinherit ? "INHERIT" : "NOINHERIT",
                                rolcanlogin ? "LOGIN" : "NOLOGIN"));

                if (rolconfig_changed) {
                    // if the role config has changed, we need to set it
                    if (!rolconfig.has_value() || rolconfig.value().empty()) {
                        sql_commands.push_back(fmt::format("ALTER ROLE {} RESET ALL", role_name));
                    } else {
                        // stored as json text array
                        nlohmann::json config_json = nlohmann::json::parse(rolconfig.value());
                        DCHECK(config_json.is_array()) << "Role config must be a JSON array";

                        // iterate through the config and set each one
                        for (const auto &config : config_json.items()) {
                            std::string config_str = config.value().get<std::string>();
                            sql_commands.push_back(fmt::format("ALTER ROLE {} SET {}", role_name, config_str));
                        }
                    }
                }
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_role_member_sync_database(LibPqConnectionPtr conn,
                                         std::vector<std::string> &sql_commands)
    {
        // execute on primary
        conn->exec(fmt::format(ROLE_MEMBER_DIFF_SELECT, _fdw_id));

        if (conn->ntuples() == 0) {
            conn->clear();
            return;
        }

        // iterate through the result set and process the role member diffs
        for (int i = 0; i < conn->ntuples(); i++) {
            std::string diff_type = conn->get_string(i, 0);
            std::string role_name = conn->escape_identifier(conn->get_string(i, 1));
            std::string member_name = conn->escape_identifier(conn->get_string(i, 2));

            DCHECK_NE(diff_type, "MODIFIED") << "Role member diffs should not be modified";

            if (diff_type == "REMOVED") {
                sql_commands.push_back(fmt::format("REVOKE {} FROM {}", member_name, role_name));
                continue;
            }

            if (diff_type == "ADDED") {
                sql_commands.push_back(fmt::format("GRANT {} TO {}", member_name, role_name));
                continue;
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_table_owner_sync_database(LibPqConnectionPtr conn, uint64_t db_id, const std::string &db_name)
    {
        // execute on primary
        conn->exec(fmt::format(TABLE_OWNER_DIFF_SELECT, _fdw_id));

        if (conn->ntuples() == 0) {
            conn->clear();
            return;
        }

        // prepare a list of SQL commands to apply; <cmd, table_oid>
        std::vector<std::pair<std::string, uint32_t>> sql_commands;

        // iterate through the result set and process the table owner diffs
        for (int i = 0; i < conn->ntuples(); i++) {
            std::string diff_type = conn->get_string(i, 0);
            std::string table_name = conn->escape_identifier(conn->get_string(i, 1));
            std::string schema_name = conn->escape_identifier(conn->get_string(i, 2));
            std::string role_owner_name = conn->escape_identifier(conn->get_string(i, 3));
            uint32_t table_oid = conn->get_int32(i, 4);

            if (diff_type == "REMOVED") {
                // nothing to do these tables are already dropped
                continue;
            }

            if (diff_type == "ADDED" || diff_type == "MODIFIED") {
                sql_commands.push_back(
                    std::make_pair(fmt::format("ALTER FOREIGN TABLE {}.{} OWNER TO {}",
                                               schema_name, table_name, role_owner_name), table_oid));
            }
        }
        conn->clear();

        if (sql_commands.empty()) {
            return;
        }

        // get the connection for the database
        auto fdw_conn = _get_fdw_connection(db_id, db_name);
        if (!fdw_conn) {
            throw Error("Failed to get connection for database " + std::to_string(db_id));
        }

        // apply the sql commands, it is possible some of these may fail if the table does not exist
        for (const auto &[sql, table_oid] : sql_commands) {
            LOG_DEBUG(LOG_FDW, "Applying SQL command: {}", sql);
            if (!fdw_conn->exec_no_throw(sql)) {
                LOG_WARN("Failed to apply SQL command: {}", sql);
                // if the command fails, we remove the table so that it will trigger as being re-added
                // on the next sync.
                DCHECK_EQ(fdw_conn->get_sql_state(), LibPqConnection::SQL_UNDEFINED_TABLE)
                    << "Unexpected SQLSTATE: " << fdw_conn->get_sql_state();
                conn->exec(fmt::format("SELECT __pg_springtail_triggers.table_owner_remove('{}', {})",
                                        _fdw_id, table_oid));
                conn->clear();
            }
            fdw_conn->clear();
        }

        // release the connection back to the cache
        _release_fdw_connection(db_id, fdw_conn);
    }

    void
    PgDDLMgr::_apply_sql_commands(uint64_t db_id, const std::string &db_name,
                                  const std::vector<std::string> &sql_commands)
    {
        if (sql_commands.empty()) {
            return;
        }

        // get the connection for the database
        auto conn = _get_fdw_connection(db_id, db_name);
        if (!conn) {
            throw Error("Failed to get connection for database " + std::to_string(db_id));
        }

        // apply each sql command
        for (const auto &sql : sql_commands) {
            LOG_DEBUG(LOG_FDW, "Applying SQL command: {}", sql);
            conn->exec(sql);
        }

        _release_fdw_connection(db_id, conn);
    }

    void
    PgDDLMgr::_sync_thread_func()
    {
        std::string host;
        int port;
        std::string user;
        std::string password;

        // run the sync thread every 10 seconds
        while (!_is_shutting_down) {
            try {
                // get primary db connection
                Properties::get_primary_db_config(host, port, user, password);
                auto conn = std::make_shared<LibPqConnection>();

                // iterate through all databases and perform sync operations
                auto databases = Properties::get_instance()->get_databases();
                for (auto &[db_id, db_name] : databases) {
                    std::vector<std::string> sql_commands;

                    // connect to the primary database
                    conn->connect(host, db_name, user, password, port, false);

                    // sync user roles for this database
                    _roles_sync_database(conn, sql_commands);

                    // sync role members for this database
                    _role_member_sync_database(conn, sql_commands);

                    // sync policies for this database
                    _policy_sync_database(conn, sql_commands);

                    // apply the sql commands to the database
                    _apply_sql_commands(db_id, db_name, sql_commands);

                    // sync table owners for this database
                    _table_owner_sync_database(conn, db_id, db_name);

                    conn->disconnect();
                }
            } catch (const std::exception &e) {
                LOG_ERROR("Error syncing policies: {}", e.what());
                // on error we should drop the primary fdw policy table and try resyncing from scratch
            }

            // sleep for POLICY_SYNC_INTERVAL_SECONDS
            auto lock = std::unique_lock<std::mutex>(_sync_shutdown_mutex);
            while (!_is_shutting_down) {
                _sync_shutdown_cv.wait_for(
                    lock,
                    std::chrono::seconds(SYNC_INTERVAL_SECONDS),
                    [this]() { return this->_is_shutting_down.load(); }
                );
            }
        }
    }

    std::unordered_map<uint64_t, std::map<uint64_t, std::pair<std::string, std::string>>>
    PgDDLMgr::_get_usertypes(uint64_t db_id, uint64_t xid)
    {
        // namespace id -> type_id -> <type_name, value_json>
        std::unordered_map<uint64_t, std::map<uint64_t, std::pair<std::string, std::string>>> usertype_map;

        // iterate through the user types and add them to the map
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::UserTypes::ID, xid);
        auto fields = table->extent_schema()->get_fields();
        for (auto row : (*table)) {
            uint64_t namespace_id = fields->at(sys_tbl::UserTypes::Data::NAMESPACE_ID)->get_uint64(&row);
            uint64_t type_id = fields->at(sys_tbl::UserTypes::Data::TYPE_ID)->get_uint64(&row);

            // make sure entry exists at this xid
            bool exists = fields->at(sys_tbl::UserTypes::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                // find type_id and remove it
                auto it = usertype_map.find(namespace_id);
                if (it != usertype_map.end()) {
                    auto type_it = it->second.find(type_id);
                    if (type_it != it->second.end()) {
                        it->second.erase(type_it);
                    }
                }
                continue;
            }

            std::string type_name(fields->at(sys_tbl::UserTypes::Data::NAME)->get_text(&row));
            std::string value_json(fields->at(sys_tbl::UserTypes::Data::VALUE)->get_text(&row));

            // only enums supported
            DCHECK_EQ(fields->at(sys_tbl::UserTypes::Data::TYPE)->get_uint8(&row), constant::USER_TYPE_ENUM);

            // insert into map by namespace_id
            LOG_DEBUG(LOG_FDW, "Adding user type: {}.{} = {}:{}", namespace_id, type_id, type_name, value_json);
            usertype_map[namespace_id][type_id] = std::make_pair(type_name, value_json);
        }

        return usertype_map;
    }

    std::unordered_map<uint64_t, std::string>
    PgDDLMgr::_get_schemas(uint64_t db_id, uint64_t xid)
    {
        // get the db config and parse out the included schemas
        auto db_config = Properties::get_db_config(db_id);

        bool all_schemas = false;
        std::set<std::string> schemas;
        std::unordered_map<uint64_t, std::string> schema_map;

        // scan through includes
        auto includes = db_config["include"];
        if (includes.contains("schemas")) {
            for (const auto &schema : includes["schemas"]) {
                std::string schema_name = schema.get<std::string>();
                if (schema_name == "*") {
                    all_schemas = true;
                    break;
                }
                schemas.insert(schema_name);
            }
        }

        // go through tables
        if (!all_schemas && includes.contains("tables")) {
            for (const auto &table : includes["tables"]) {
                std::string schema_name = table["schema"].get<std::string>();
                schemas.insert(schema_name);
            }
        }

        if (!all_schemas && schemas.empty()) {
            return {};
        }

        // iterate through the schemas and get the schema ids
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, xid);
        auto fields = table->extent_schema()->get_fields();
        for (auto row : (*table)) {
            // make sure entry exists at this xid
            uint64_t namespace_id = fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&row);
            bool exists = fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row);
            if (!exists) {
                schema_map.erase(namespace_id);
                continue;
            }
            // check if we have a schema name match, if so add it to the map
            std::string namespace_name(fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row));
            if (all_schemas || schemas.contains(namespace_name)) {
                schema_map[namespace_id] = namespace_name;
            }
        }

        return schema_map;
    }

    void
    PgDDLMgr::_init_rls(uint64_t db_id,
                        uint64_t xid,
                        const std::unordered_map<uint64_t, std::string> &schema_map,
                        LibPqConnectionPtr conn)
    {
        // Iterate through the TableNames table to look for rls_enabled tables
        auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, xid);
        auto fields = table->extent_schema()->get_fields();

        for (auto row : (*table)) {
            // make sure entry exists at this xid
            bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
            bool rls_enabled = fields->at(sys_tbl::TableNames::Data::RLS_ENABLED)->get_bool(&row);
            if (!exists || !rls_enabled) {
                continue;
            }

            // get the table name and schema name
            std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));

            // get schema name from schema map
            uint64_t namespace_oid = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
            DCHECK_NE(namespace_oid, 0) << "Namespace OID should not be 0";

            std::string schema_name;
            auto it = schema_map.find(namespace_oid);
            if (it != schema_map.end()) {
                schema_name = it->second;
            } else {
                LOG_WARN("Schema OID {} not found in schema map for table {}.{}",
                         namespace_oid, schema_name, table_name);
                DCHECK(it != schema_map.end()) << "Schema OID should be in the schema map";
                continue; // skip this table if schema is not found
            }

            bool rls_forced = fields->at(sys_tbl::TableNames::Data::RLS_FORCED)->get_bool(&row);

            // create the RLS policy for the table
            auto sql = fmt::format(ALTER_TABLE_RLS,
                conn->escape_identifier(schema_name), conn->escape_identifier(table_name),
                rls_forced ? ", FORCE ROW LEVEL SECURITY" : "");
        }
    }

    void
    PgDDLMgr::_init_fdw()
    {
        // get map of dbs id:name from redis
        auto dbs = Properties::get_databases();

        // go through each db and drop/create the database on the fdw
        for (const auto &[db_id, db_name] : dbs) {
            _add_replicated_database(db_id, db_name, false);
        }
    }

    std::string
    PgDDLMgr::_get_create_schema_with_grants_query(std::string_view schema)
    {
        /** Create schema with grants, params: schema, schema, user, schema, user, schema, user */
        return fmt::format("CREATE SCHEMA {};"
                "  GRANT USAGE ON SCHEMA {} TO {};"
                "  GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};"
                "  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {};",
                schema,
                schema, _fdw_username,
                schema, _fdw_username,
                schema, _fdw_username);
    }

    std::string
    PgDDLMgr::_get_alter_schema_with_grants_query(std::string_view old_schema, std::string_view new_schema)
    {
        /** Alter schema with grants, params: old_schema, new_schema, new_schema, user, new_schema, user, new_schema, user */
        return fmt::format("ALTER SCHEMA {} RENAME TO {};"
            "  GRANT USAGE ON SCHEMA {} TO {};"
            "  GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};"
            "  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {};",
                old_schema, new_schema,
                new_schema, _fdw_username,
                new_schema, _fdw_username,
                new_schema, _fdw_username);
    }

    std::string
    PgDDLMgr::_get_create_type_query(std::string_view escaped_schema,
                                     std::string_view escaped_name,
                                     std::string_view value_json_str,
                                     LibPqConnectionPtr conn)
    {
        nlohmann::json value_json = nlohmann::json::parse(value_json_str);
        CHECK(value_json.is_array());

        std::stringstream value_ss;
        int i = 0;

        // iterate through json array of form: [{'val1':1}, {'val2':2}, {'val3':3}]
        for (const auto &obj : value_json) {
            DCHECK(obj.is_object());
            auto it = obj.begin();
            if (i > 0) {
                value_ss << ", ";
            }
            value_ss << "'" << conn->escape_string(it.key()) << "'";
            i++;
        }

        return fmt::format("CREATE TYPE {}.{} AS ENUM ({})",
                           escaped_schema, escaped_name, value_ss.str());
    }

    void
    PgDDLMgr::run()
    {
        // init redis ddl client
        RedisDDL redis_ddl;

        // move any pending DDLs to the active queue
        redis_ddl.abort_fdw(_fdw_id);

        while (!_is_shutting_down) {
            try {
                // blocking redis call to get next set of DDL statements
                auto &&ddls_vec = redis_ddl.get_next_ddls(_fdw_id);
                if (ddls_vec.empty()) {
                    continue; // check for shutdown and then re-check for queued DDL changes
                }

                // check if we should skip applying any of the DDLs based on already-applied XIDs
                std::shared_lock db_lock(_db_mutex);
                std::map<uint64_t, std::map<uint64_t, nlohmann::json>> db_map;
                for (auto &entry : ddls_vec) {
                    uint64_t db_id = entry.at("db_id").get<uint64_t>();
                    uint64_t schema_xid = entry.at("xid").get<uint64_t>();
                    auto ddls = entry.at("ddls");

                    if (_db_xid_map.contains(db_id) && _db_xid_map[db_id] >= schema_xid) {
                        LOG_WARN("Schema XID has already been applied: db_id={}, current={}, new={}",
                                    db_id, _db_xid_map[db_id], schema_xid);
                    } else {
                        db_map[db_id][schema_xid] = ddls;
                    }
                }
                if (db_map.empty()) {
                    LOG_WARN("All schemas have already been applied");
                    db_lock.unlock();
                    redis_ddl.commit_fdw_no_update(_fdw_id);
                    continue;
                }
                db_lock.unlock();

                // queue each DBs DDL statements for processing
                for (const auto &[db_id, xid_map] : db_map) {
                    _thread_manager->queue_request(std::make_shared<common::MultiQueueRequest>(
                        db_id, [this, &redis_ddl, db_id, xid_map]() {
                            try {
                                // apply the DDL statements
                                bool status = _update_schemas(db_id, xid_map);
                                if (!status) {
                                    // error occured, abort the DDL
                                    LOG_ERROR("Failed to apply DDL statements");
                                    redis_ddl.abort_fdw(_fdw_id);
                                    DCHECK(false);
                                    return;
                                }

                                // success, update schema XID if applied, otherwise they may be
                                // queued
                                uint64_t schema_xid = xid_map.rbegin()->first;
                                LOG_DEBUG(
                                    LOG_FDW, "Updating redis ddl @ through schema XID: {}, db_id: {}",
                                    schema_xid, db_id);
                                redis_ddl.update_schema_xid(_fdw_id, db_id, schema_xid);

                                std::unique_lock db_lock_unique(_db_mutex);
                                _db_xid_map[db_id] = schema_xid;
                                db_lock_unique.unlock();

                            } catch (Error &e) {
                                LOG_ERROR("Springtail exception in ddl manager task: {}", e.what());
                                DCHECK(false);  // assert in debug
                                e.log_backtrace();
                            } catch (std::exception &e) {
                                LOG_ERROR("Caught std::exception in ddl mgr task: {}", e.what());
                                DCHECK(false);  // assert in debug
                            } catch (...) {
                                // handle exception
                                LOG_ERROR("Exception in thread manager task");
                                DCHECK(false);  // assert in debug
                            }
                        }));
                }

            } catch (Error &e) {
                LOG_ERROR("Springtail exception in DDL thread");
                DCHECK(false); // assert in debug
                e.log_backtrace();
            } catch (...) {
                // handle exception
                LOG_ERROR("Exception in DDL thread");
                DCHECK(false); // assert in debug
            }
        }
        _thread_manager->notify_shutdown();
        _thread_manager->shutdown();
    }

    LibPqConnectionPtr
    PgDDLMgr::_get_fdw_connection(std::optional<uint64_t> db_id_opt, const std::string &db_name)
    {
        // check if we have a connection in the cache
        LibPqConnectionPtr conn = nullptr;

        if (!db_id_opt.has_value()) {
            conn = std::make_shared<LibPqConnection>();
            conn->connect(_hostname, db_name, _username, _password, _port, false);
            return conn;
        }

        uint64_t db_id = db_id_opt.value();

        std::unique_lock<std::mutex> lock(_fdw_conn_cache_mutex);
        conn = _fdw_conn_cache.get(db_id);

        // check if the connection is still valid
        if (conn != nullptr) {
            if (conn->is_connected()) {
                LOG_DEBUG(LOG_FDW, "Reusing connection for db_id: {}", db_id);
                // evict the connection from the cache with no callback
                // this is so that it can be used and no-one else will try to use it
                _fdw_conn_cache.evict(db_id, true);
                return conn;
            }
            _fdw_conn_cache.evict(db_id);
        }

        LOG_DEBUG(LOG_FDW, "Establishing connection for db_id: {}", db_id);

        // use libpq to connect to the database
        conn = std::make_shared<LibPqConnection>();
        conn->connect(_hostname, _db_prefix + db_name, _username, _password, _port, false);

        // the connection should be released back to the cache
        return conn;
    }

    void
    PgDDLMgr::_release_fdw_connection(uint64_t db_id, LibPqConnectionPtr conn)
    {
        // check if the connection is still valid
        if (conn == nullptr || !conn->is_connected()) {
            return;
        }

        // insert the connection into the cache
        std::unique_lock<std::mutex> lock(_fdw_conn_cache_mutex);
        if (_fdw_conn_cache.peek(db_id) == nullptr) {
            LOG_DEBUG(LOG_FDW, "Releasing connection for db_id: {}", db_id);
            _fdw_conn_cache.insert(db_id, conn);
            return;
        }

        LOG_DEBUG(LOG_FDW, "Releasing existing connection for db_id: {}", db_id);
        conn->disconnect();
    }

    bool
    PgDDLMgr::_update_schemas(uint64_t db_id,
                              const std::map<uint64_t, nlohmann::json> &xid_map)
    {
        // get the database name for the db_id; XXX should see if we can swtich to OID
        std::string db_name = Properties::get_db_name(db_id);
        LibPqConnectionPtr conn = _get_fdw_connection(db_id, _db_prefix + db_name);

        try {
            // generate a DDL statement for each JSON in the transaction
            std::vector<std::string> txn;
            for (auto &[xid, ddls] : xid_map) {
                for (const auto &ddl : ddls) {
                    auto query_string = _gen_sql_from_json(conn, SPRINGTAIL_FDW_SERVER_NAME, ddl);
                    if (!query_string.empty()) {
                        txn.push_back(query_string);
                    }
                }
            }

            // generate a statement to alter the server options with the schema XID
            uint64_t schema_xid = xid_map.rbegin()->first;
            txn.push_back(fmt::format("ALTER SERVER {} OPTIONS (SET {} '{}')",
                                      SPRINGTAIL_FDW_SERVER_NAME,
                                      SPRINGTAIL_FDW_SCHEMA_XID_OPTION,
                                      schema_xid));

            // execute the set of statements
            _execute_ddl(conn, db_id, schema_xid, txn);

            _release_fdw_connection(db_id, conn);

            return true;

        } catch (Error &e) {
            _release_fdw_connection(db_id, conn);
            e.log_backtrace();
            assert(0); // assert in debug
            return false;
        }
    }

    void
    PgDDLMgr::_execute_ddl(LibPqConnectionPtr conn,
                           uint64_t db_id,
                           uint64_t schema_xid,
                           const std::vector<std::string> &txn)
    {
        // start a transaction
        conn->start_transaction();

        // exectute each DDL statement
        for (const auto &sql : txn) {
            LOG_DEBUG(LOG_FDW, "Executing DDL: {}", sql);
            conn->exec(sql);
            conn->clear();
        }

        conn->end_transaction();
    }

    std::string
    PgDDLMgr::_gen_sql_from_json(LibPqConnectionPtr conn,
                                 const std::string &server_name,
                                 const nlohmann::json &ddl)
    {
        assert(ddl.is_object());
        assert(ddl.contains("action"));

        LOG_DEBUG(LOG_FDW, "DDL JSON: {}", ddl.dump(4));

        auto const &action = ddl.at("action");
        if (action == "create") { // create table
            // generate the CREATE TABLE statement
            return _gen_fdw_table_sql(conn, server_name, ddl.at("schema"), ddl.at("table"),
                                      ddl.at("tid"), ddl.at("columns"));
        }

        else if (action == "rename") { // rename table
            std::string rename = fmt::format("ALTER FOREIGN TABLE {}.{} RENAME TO {};",
                                             conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                             conn->escape_identifier(ddl.at("old_table").get<std::string>()),
                                             conn->escape_identifier(ddl.at("table").get<std::string>()));

            // XXX it's not clear to me that we need to support a schema change here?
            if (ddl.at("schema").get<std::string>() != ddl.at("old_schema").get<std::string>()) {
                return rename + fmt::format("ALTER FOREIGN TABLE {}.{} SET SCHEMA {};",
                                            conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                            conn->escape_identifier(ddl.at("table").get<std::string>()),
                                            conn->escape_identifier(ddl.at("schema").get<std::string>()));
            } else {
                return rename;
            }
        }

        else if (action == "drop") {  // drop table
            return fmt::format("DROP FOREIGN TABLE IF EXISTS {}.{};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()));
        }
#if ENABLE_SCHEMA_MUTATES
        // XXX THIS CODE IS CURRENTLY DISABLED
        else if (action == "col_add") { // alter table add column
            auto &col = ddl.at("column");

            std::string constraints;
            std::string null_constraint = col.at("nullable").get<bool>()
                ? "NULL"
                : "NOT NULL";
            if (col.contains("default")) {
                constraints = fmt::format("{} {}", null_constraint,
                                         col.at("default").get<std::string>());
            } else {
                constraints = null_constraint;
            }

            uint32_t type_oid = col.at("type").get<uint32_t>();
            // XXX a fix is required to pull type name from the trigger data
            // see _gen_fdw_table_sql()
            assert(false);
            return fmt::format("ALTER FOREIGN TABLE {}.{} ADD COLUMN {} {} {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               type_map.at(type_oid),
                               constraints);

            CHECK(false); // XXX col["type_name"] must be added and checked
        }

        else if (action == "col_drop") {  // alter table drop column
            return fmt::format("ALTER FOREIGN TABLE {}.{} DROP COLUMN {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("column").get<std::string>()));
        }
#endif /* ENABLE_SCHEMA_MUTATES */

        else if (action == "col_rename") {
            return fmt::format("ALTER FOREIGN TABLE {}.{} RENAME COLUMN {} TO {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("old_name").get<std::string>()),
                               conn->escape_identifier(ddl.at("new_name").get<std::string>()));
        }

#if ENABLE_SCHEMA_MUTATES
        else if (action == "col_nullable") {
            auto &col = ddl.at("column");
            return fmt::format("ALTER FOREIGN TABLE {}.{} ALTER COLUMN {} {} NOT NULL;",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               col.at("nullable").get<bool>() ? "DROP" : "SET");
        }
#endif /* ENABLE_SCHEMA_MUTATES */

        else if (action == "create_index") {
            // TODO: do something?
            LOG_ERROR("CREATE INDEX");
            CHECK(false);
            return "";
        }
        else if (action == "drop_index") {
            // TODO: do something?
            LOG_ERROR("DROP INDEX");
            CHECK(false);
            return "";
        }
        else if (action == "ns_create") {
            LOG_DEBUG(LOG_FDW, "Creating schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return _get_create_schema_with_grants_query(escaped_schema);
        }
        else if (action == "ns_alter") {
            LOG_DEBUG(LOG_FDW, "Altering schema with JSON: {}", ddl.dump());
            const auto old_schema = conn->escape_identifier(ddl.at("old_name").get<std::string>());
            const auto new_schema = conn->escape_identifier(ddl.at("name").get<std::string>());

            return _get_alter_schema_with_grants_query(old_schema, new_schema);
        }
        else if (action == "ns_drop") {
            LOG_DEBUG(LOG_FDW, "Dropping schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return fmt::format("DROP SCHEMA IF EXISTS {} CASCADE", escaped_schema);
        }
        else if (action == "set_namespace") {
            LOG_DEBUG(LOG_FDW, "Set namespace with JSON: {}", ddl.dump());
            const auto schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto table = conn->escape_identifier(ddl.at("table").get<std::string>());
            const auto old_schema = conn->escape_identifier(ddl.at("old_schema").get<std::string>());

            return fmt::format("ALTER FOREIGN TABLE {}.{} SET SCHEMA {};",
                               old_schema, table, schema);
        }
        else if (action == "ut_create") {
            LOG_DEBUG(LOG_FDW, "Creating user type with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto escaped_name = conn->escape_identifier(ddl.at("name").get<std::string>());
            const auto value_json_str = ddl.at("value").get<std::string>();

            return _get_create_type_query(escaped_schema, escaped_name, value_json_str, conn);
        }
        else if (action == "ut_drop") {
            LOG_DEBUG(LOG_FDW, "Dropping user type with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto escaped_name = conn->escape_identifier(ddl.at("name").get<std::string>());

            return fmt::format("DROP TYPE IF EXISTS {}.{} CASCADE", escaped_schema, escaped_name);
        } else if (action == "ut_alter") {
            LOG_DEBUG(LOG_FDW, "Altering user type with JSON: {}", ddl.dump());

            // need to check if it is renamed
            const auto old_name = conn->escape_identifier(ddl.at("old_name").get<std::string>());
            const auto new_name = conn->escape_identifier(ddl.at("name").get<std::string>());
            const auto escaped_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            if (old_name != new_name) {
                return fmt::format("ALTER TYPE {}.{} RENAME TO {}", escaped_schema, old_name, new_name);
            }

            // need to check if schema has changed
            const auto old_schema = conn->escape_identifier(ddl.at("old_schema").get<std::string>());
            const auto new_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            if (old_schema != new_schema) {
                return fmt::format("ALTER TYPE {}.{} SET SCHEMA {};",
                                   old_schema, new_name, new_schema);
            }

            // need to check if value has changed
            const auto value_json_str = ddl.at("value").get<std::string>();
            const auto old_value_json_str = ddl.at("old_value").get<std::string>();
            if (value_json_str != old_value_json_str) {
                return gen_alter_enum_sql(ddl.at("schema").get<std::string>(),
                                          ddl.at("name").get<std::string>(),
                                          nlohmann::json::parse(old_value_json_str),
                                          nlohmann::json::parse(value_json_str),
                                          conn);
            }

            // nothing to actually change in the FDW
            return {};
        } else if (action == "set_rls_enabled") {
            // enable/disable row level security
            // this is a no-op for the FDW, as we don't support RLS
            LOG_DEBUG(LOG_FDW, "RLS enabled/disabled with JSON: {}", ddl.dump());

            bool enabled = ddl.at("rls_enabled").get<bool>();
            std::string enable_action = enabled ? "ENABLE" : "DISABLE";

            return fmt::format("ALTER FOREIGN TABLE {}.{} {} ROW LEVEL SECURITY",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               enable_action);

        } else if (action == "set_rls_forced") {
            // force row level security
            // this is a no-op for the FDW, as we don't support RLS
            LOG_DEBUG(LOG_FDW, "RLS forced with JSON: {}", ddl.dump());

            bool forced = ddl.at("rls_forced").get<bool>();
            std::string force_action = forced ? "FORCE" : "NO FORCE";

            return fmt::format("ALTER FOREIGN TABLE {}.{} {} ROW LEVEL SECURITY",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               force_action);
        }

        // can't currently support other kinds of DDL mutations
        LOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        CHECK(false);
    }

    std::string
    PgDDLMgr::_gen_fdw_table_sql(LibPqConnectionPtr conn,
                                 const std::string &server_name,
                                 const std::string &schema,
                                 const std::string &table,
                                 uint64_t tid,
                                 const nlohmann::json &columns)
    {
        std::string escaped_schema = conn->escape_identifier(schema);

        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {}.{} (\n",
                                         escaped_schema,
                                         conn->escape_identifier(table));

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        int i = 0, num_cols = columns.size();

        // iterate over the columns again, adding each to the create statement
        for (const auto &col : columns) {
            // check for userdefined type
            uint32_t type_oid = col.at("type").get<uint32_t>();
            std::string type_name = col.at("type_name").get<std::string>();
            std::string type_namespace = col.at("type_namespace").get<std::string>();

            CHECK(!type_name.empty());

            // the constant FirstNormalObjectId is defined in postgres include/access/transam.h
            if (type_oid >= constant::FIRST_USER_DEFINED_PG_OID) {
                // this is a user defined type, fully qualify it
                type_name = fmt::format("{}.{}", conn->escape_identifier(type_namespace),
                                                 conn->escape_identifier(type_name));
            }

            std::string column = fmt::format("{} {} {} {}", conn->escape_identifier(col.at("name")),
                                             type_name, col.at("nullable").get<bool>() ? "" : "NOT NULL",
                                             (i == num_cols - 1) ? "" : ",");

            i++;
            create += column;
        }

        create += fmt::format("\n) SERVER {} OPTIONS (tid '{}');", server_name, tid);

        LOG_DEBUG(LOG_FDW, "Generated SQL: {}", create);

        return create;
    }

    void
    PgDDLMgr::_create_database(LibPqConnectionPtr conn,
                               const uint64_t db_id,
                               const std::string &db_name)
    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        LOG_DEBUG(LOG_FDW, "Creating DB ID: {}, DB Name: {}", db_id, db_name);

        // drop and create database on fdw
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format("DROP DATABASE IF EXISTS {} WITH (FORCE)", prefixed_name);
        std::string create_db = fmt::format("CREATE DATABASE {}", prefixed_name);

        conn->exec(drop_db);
        conn->clear();

        conn->exec(create_db);
        conn->clear();

        // grant connect to the fdw user
        conn->exec(fmt::format("GRANT CONNECT ON DATABASE {} TO {}", prefixed_name, _fdw_username));
        conn->clear();
    }

    void
    PgDDLMgr::_create_schemas(const uint64_t db_id,
                              const std::string &db_name)
    {

        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        RedisDDL redis_ddl;

        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

        // get schemas, parse include, fetch from primary db if necessary
        auto &&schemas = _get_schemas(db_id, xid);

        // get user types from system tables
        auto &&user_types = _get_usertypes(db_id, xid);

        auto conn = _get_fdw_connection(db_id, db_name);

        // drop and create the fdw extension
        conn->exec(fmt::format("DROP EXTENSION IF EXISTS {} CASCADE", SPRINGTAIL_FDW_EXTENSION));
        conn->clear();

        conn->exec(fmt::format("CREATE EXTENSION {} WITH SCHEMA PUBLIC", SPRINGTAIL_FDW_EXTENSION));
        conn->clear();

        // drop and create the foreign server
        conn->exec(fmt::format("DROP SERVER IF EXISTS {}", SPRINGTAIL_FDW_SERVER_NAME));
        conn->clear();

        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        conn->exec(fmt::format("CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (id '{}', db_id '{}', db_name '{}', schema_xid '{}')",
                                SPRINGTAIL_FDW_SERVER_NAME, SPRINGTAIL_FDW_EXTENSION, _fdw_id, db_id, prefixed_name, xid));
        conn->clear();

        for (const auto &schema: schemas) {
            // create schema if not exists
            std::string escaped_schema = conn->escape_identifier(schema.second);
            conn->exec(fmt::format("CREATE SCHEMA IF NOT EXISTS {}", escaped_schema));
            conn->clear();

            auto it = user_types.find(schema.first);
            if (it != user_types.end()) {
                // create the user defined types (enums)
                // these need to be in a transaction as create type is
                // not visible until the transaction is committed
                conn->start_transaction();
                for (const auto &entry : it->second) {
                    const std::string &type_name = entry.second.first;
                    const std::string &value_json_str = entry.second.second;
                    std::string escaped_type = conn->escape_identifier(type_name);
                    LOG_DEBUG(LOG_FDW, "Creating user type: {}.{} = {}", escaped_schema, escaped_type, value_json_str);
                    std::string create_type_query = _get_create_type_query(escaped_schema, escaped_type, value_json_str, conn);
                    conn->exec(create_type_query);
                    conn->clear();
                }
                conn->end_transaction();
            }

            // import foreign schema
            conn->exec(fmt::format("IMPORT FOREIGN SCHEMA {} FROM SERVER {} INTO {}",
                                    escaped_schema, SPRINGTAIL_FDW_SERVER_NAME,
                                    escaped_schema));
            conn->clear();

            // grant usage and select on all tables and sequences to the fdw user
            conn->exec(fmt::format("GRANT USAGE ON SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
            conn->exec(fmt::format("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
            conn->exec(fmt::format("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
        }

        // import catalog schema
        std::string escaped_schema = conn->escape_identifier(SPRINGTAIL_FDW_CATALOG_SCHEMA);
        conn->exec(fmt::format("CREATE SCHEMA IF NOT EXISTS {}", escaped_schema));
        conn->clear();
        conn->exec(fmt::format("IMPORT FOREIGN SCHEMA {} FROM SERVER {} INTO {}",
                                escaped_schema, SPRINGTAIL_FDW_SERVER_NAME,
                                escaped_schema));
        conn->clear();

        // initialize the RLS policies for the schemas
        _init_rls(db_id, xid, schemas, conn);

        _release_fdw_connection(db_id, conn);

        // set the schema xid in the map
        std::unique_lock db_lock(_db_mutex);
        _db_xid_map[db_id] = xid;
        db_lock.unlock();

        // update redis with the schema xid
        redis_ddl.update_schema_xid(_fdw_id, db_id, xid);
    }

    void
    PgDDLMgr::_add_replicated_database(uint64_t db_id,
                                       const std::optional<std::string> &db_name_opt,
                                       bool check_exists)

    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        nlohmann::json db_config = Properties::get_db_config(db_id);

        std::string db_name;
        if (!db_name_opt.has_value()) {
            // if db_name is not provided, get it from the properties
            nlohmann::json db_config = Properties::get_db_config(db_id);
            db_name = db_config["name"];
        } else {
            db_name = db_name_opt.value();
        }

        if (check_exists) {
            // check if the database already exists in the fdw
            std::shared_lock shared_lock(_db_mutex);
            if (_db_xid_map.contains(db_id)) {
                LOG_DEBUG(LOG_FDW, "Database {} already exists in the fdw", db_name);
                return;
            }
        }

        // verify that the database does not exist before trying to add it
        LibPqConnectionPtr conn = _get_fdw_connection(std::nullopt, "postgres");
        if (check_exists) {
            std::string prefixed_name = conn->escape_string(_db_prefix + db_name);
            conn->exec(fmt::format(VERIFY_DB_EXISTS, prefixed_name));
            if (conn->ntuples() > 0) {
                conn->disconnect();
                return;
            }
            conn->clear();
        }

        // add database
        _create_database(conn, db_id, db_name);
        conn->disconnect();

        // create schemas
        _create_schemas(db_id, db_name);
    }

    void
    PgDDLMgr::_remove_replicated_database(uint64_t db_id)
    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        std::shared_lock shared_lock(_db_mutex);
        if (!_db_xid_map.contains(db_id)) {
            return;
        }
        shared_lock.unlock();

        // read db_config to get database name
        nlohmann::json db_config = Properties::get_db_config(db_id);
        std::string db_name = db_config["name"];

        // drop database
        LibPqConnectionPtr conn = _get_fdw_connection(std::nullopt, "postgres");
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format(DROP_DATABASE, prefixed_name);
        conn->exec(drop_db);
        conn->disconnect();

        // remove it from internal storage
        std::unique_lock unique_lock(_db_mutex);
        _db_xid_map.erase(db_id);
    }

    std::string
    PgDDLMgr::gen_alter_enum_sql(const std::string &schema_str,
                                 const std::string &type_str,
                                 const nlohmann::json &from,
                                 const nlohmann::json &to,
                                 const LibPqConnectionPtr conn)
    {
        size_t i = 0, j = 0;

        std::string schema = conn->escape_identifier(schema_str);
        std::string type_name = conn->escape_identifier(type_str);

        CHECK(from.is_array());
        CHECK(to.is_array());

        LOG_DEBUG(LOG_FDW, "Comparing enum types: {}.{} from: {} to: {}", schema, type_name, from.dump(), to.dump());

        while (i < from.size() && j < to.size()) {

            LOG_DEBUG(LOG_FDW, "i={}, j={}", i, j);

            LOG_DEBUG(LOG_FDW, "got from vals");

            std::string from_key = from[i].begin().key();
            std::string to_key = to[j].begin().key();

            LOG_DEBUG(LOG_FDW, "From key: {}, From val: {}, To key: {}, To val: {}", from_key,
                    (float)from[i].begin().value(), to_key, (float)to[j].begin().value());

            if (from_key == to_key) {
                ++i;
                ++j;
                continue;
            }
            else if ((i + 1 < from.size()) && (from[i + 1].begin().key() == to_key)) {
                // from[i] was removed
                CHECK(false);  // removal is not supported
            }
            else if ((j + 1 < to.size()) && (to[j + 1].begin().key() == from_key)) {
                // to[i] was added
                return fmt::format("ALTER TYPE {}.{} ADD VALUE '{}' BEFORE '{}';",
                                   schema, type_name, conn->escape_string(to_key), conn->escape_string(from_key));
            }
            else if ((i + 1 < from.size() &&
                      j + 1 < to.size() &&
                      from[i + 1].begin().key() == to[j + 1].begin().key()) ||
                     (i + 1 == from.size() && j + 1 == to.size())) {
                // assume we renamed the current key
                return fmt::format("ALTER TYPE {}.{} RENAME VALUE '{}' TO '{}';", schema, type_name,
                                   conn->escape_string(from_key), conn->escape_string(to_key));
            } else {
                // Otherwise treat as added after previous
                if (i > 0) {
                    return fmt::format("ALTER TYPE {}.{} ADD VALUE '{}' AFTER '{}';",
                        schema, type_name, conn->escape_string(to_key), conn->escape_string(from[i - 1].begin().key()));
                } else {
                    return fmt::format("ALTER TYPE {}.{} ADD VALUE '{}' BEFORE '{}';",
                        schema, type_name, conn->escape_string(to_key), conn->escape_string(from_key));
                }
            }

            CHECK(false);
        }

        if (j < to.size()) {
            // Some elements left in `to` -> must be addition
            CHECK(!from.empty());
            return fmt::format("ALTER TYPE {}.{} ADD VALUE '{}' AFTER '{}';",
                schema, type_name, conn->escape_string(to[j].begin().key()), conn->escape_string(from.back().begin().key()));
        }

        // Check leftover elements
        if (i < from.size()) {
            // Some elements left in `from` -> must be removal
            CHECK(false);  // removal is not supported
        }

        DCHECK(false);  // no changes
        return {};
    }

} // namespace springtail::pg_fdw
