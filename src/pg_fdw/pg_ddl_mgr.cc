#include <redis/db_state_change.hh>
#include <redis/redis_ddl.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr_client.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_ddl_mgr.hh>

#include <pg_fdw/constants.hh>
#include <pg_fdw/pg_fdw_ddl_common.hh>
#include <pg_fdw/pg_xid_collector.hh>

namespace springtail::pg_fdw {

    /** Coordinator worker ID for sync thread */
    static constexpr char DDL_SYNC_WORKER_ID[] = "DDL_MGR_SYNC:{}";

    /** Get a list of all non-system schema names */
    static constexpr char DROP_DATABASE[] =
        "DROP DATABASE IF EXISTS {} WITH (FORCE)";

    static constexpr char VERIFY_DB_EXISTS[] =
        "SELECT 1 FROM  pg_database WHERE datname = '{}'";

    static constexpr char SYSTEM_TYPES[] =
        "SELECT oid, typname, typcategory "
        "FROM pg_type "
        "WHERE typisdefined = true";

    static constexpr char ALTER_TABLE_RLS[] =
        "ALTER {} TABLE {}.{} "
        "  ENABLE ROW LEVEL SECURITY {}";

    /** Calls primary function to get diff of policy changes; see policy.sql */
    static constexpr char POLICY_DIFF_SELECT[] =
        "SELECT diff_type, table_name, schema_name, "
        "       policy_oid, policy_name, policy_name_old, "
        "       policy_permissive, policy_roles, policy_qual, table_oid "
        "FROM __pg_springtail_triggers.policy_diff('{}');";

    /** Calls primary function to get diff of role changes; see roles.sql */
    static constexpr char ROLE_DIFF_SELECT[] =
        "SELECT diff_type, rolname, rolsuper, "
        "    rolinherit, rolcanlogin, rolbypassrls, rolconfig, rolconfig_changed "
        "FROM __pg_springtail_triggers.role_diff('{}');";

    /** Calls primary function to get diff of role member changes; see role_members.sql */
    static constexpr char ROLE_MEMBER_DIFF_SELECT[] =
        "SELECT diff_type, role_name, member_name "
        "FROM __pg_springtail_triggers.role_member_diff('{}');";

    /** Calls primary function to get diff of table owner changes; see table_owners.sql */
    static constexpr char TABLE_OWNER_DIFF_SELECT[] =
        "SELECT diff_type, table_name, schema_name, "
        "       role_owner_name, table_oid "
        "FROM __pg_springtail_triggers.table_owner_diff('{}');";

    /** Resets the history of snapshot tables for a given FDW */
    static constexpr char RESET_HISTORY_DIFFS[] =
        "SELECT __pg_springtail_triggers.reset_role_diff('{}'); "
        "SELECT __pg_springtail_triggers.reset_role_member_diff('{}'); "
        "SELECT __pg_springtail_triggers.reset_table_owner_diff('{}'); "
        "SELECT __pg_springtail_triggers.reset_policy_diff('{}');";

    /** Drops roles that aren't system or FDW roles */
    static constexpr char DROP_ROLES[] =
        "DO $$ "
        "DECLARE "
        "  r RECORD; "
        "BEGIN "
        "  FOR r IN (SELECT rolname "
        "            FROM pg_roles "
        "            WHERE rolname NOT LIKE 'pg_%' AND "
	    "                  rolname <> '{}' AND "
        "                  rolname NOT IN (SELECT DISTINCT rolname FROM pg_roles JOIN pg_database ON pg_roles.oid = datdba) "
        "           ) LOOP "
        "    EXECUTE format('DROP ROLE IF EXISTS %s', r.rolname); "
        "  END LOOP; "
        "END $$;";

    /**
     * Query to check if a table exists given:
     * primary table oid, schema name and table name.
     * This query will return 1 if the table exists,
     * NULL if it does not exist.
     * For foreign tables, it will check if the
     * foreign table options contain the tid option that matches
     * the primary table oid.
     */
    static constexpr char TABLE_EXISTS_SELECT[] =
        "SELECT "
        "  CASE "
        "    WHEN c.relkind = 'f' THEN ( "
        "      SELECT 1 FROM unnest(ft.ftoptions) AS opt "
        "      WHERE opt = format('tid=%s', '{}') LIMIT 1 "
        "    ) "
        "    WHEN c.relkind IN ('p') AND d.description = format('TID:%s', '{}') THEN 1 "
        "    WHEN c.relkind IN ('r') THEN 1 "
        "    ELSE NULL "
        "  END AS exists "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid "
        "LEFT JOIN pg_foreign_server fs ON ft.ftserver = fs.oid "
        "LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0 "
        "WHERE n.nspname = '{}' AND c.relname = '{}'";

    static constexpr char CREATE_USER[] =
        "CREATE USER {} WITH LOGIN PASSWORD '{}' IN ROLE pg_read_all_data;";

    PgDDLMgr::PgDDLMgr()
        : Singleton<PgDDLMgr>(ServiceId::PgDDLMgrId),
          _fdw_conn_cache(MAX_CONNECTION_CACHE_SIZE)
    {
        // initialize the cache watcher
        _cache_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string& path, const nlohmann::json& new_value) {
                _on_database_ids_changed(path, new_value);
            }
        );
        PgXidCollector::get_instance()->start_thread();
    }

    void PgDDLMgr::_on_database_ids_changed(const std::string &path,
                                            const nlohmann::json &new_value)
    {
        LOG_INFO("Replicated databases changed event: {}", new_value.dump(4));
        CHECK_EQ(path, Properties::DATABASE_IDS_PATH);

        // get a vector of old database ids from _log_mgrs
        std::shared_lock lock(_db_name_mutex);
        auto keys = std::views::keys(_db_name_map);
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
    PgDDLMgr::_internal_shutdown()
    {
        // wake up sync thread in case it was sleeping
        _sync_shutdown_cv.notify_all();

        // cleanup redis cache watcher
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        // join with threads
        _pg_ddl_mgr_thread.join();
        _sync_thread.join();
        PgXidCollector::shutdown();

        Properties::get_instance()->set_fdw_state(Properties::FDW_STATE_STOPPED);
    }

    void
    PgDDLMgr::start()
    {
        auto props = Properties::get_instance();

        auto fdw_user = props->get_system_role(Properties::DB_ROLE_FDW);
        std::string username = std::get<0>(fdw_user);
        std::string password = std::get<1>(fdw_user);

        auto proxy_user = props->get_system_role(Properties::DB_ROLE_PROXY);
        std::string proxy_password = std::get<1>(proxy_user);

        std::optional<std::string> hostname = springtail_retreive_argument<std::optional<std::string>>(ServiceId::PgDDLMgrId, "hostname").value();

        // start the ddl main thread
        std::string fdw_id = props->get_fdw_id();

        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Starting DDL Mgr with fdw_id: {}, username: {}, password: {}, socket_hostname: {}",
                    fdw_id, username, password, hostname.value_or(""));
        PgDDLMgr::get_instance()->init(fdw_id, username, password, proxy_password, hostname);
        PgDDLMgr::get_instance()->_pg_ddl_mgr_thread = std::thread(&PgDDLMgr::run, PgDDLMgr::get_instance());
        pthread_setname_np(PgDDLMgr::get_instance()->_pg_ddl_mgr_thread.native_handle(), "PgDDLMgrThread");
    }

    void
    PgDDLMgr::init(const std::string &fdw_id,
                   const std::string &username,
                   const std::string &password,
                   const std::string &proxy_password,
                   const std::optional<std::string> &hostname)
    {
        // set fdw id
        _fdw_id = fdw_id;

        // set username and password for ddl mgr user
        // this user has more permissions than the fdw user
        _username = username;
        _password = password;

        // set proxy user password for roles created by this fdw
        _proxy_password = proxy_password;

        // fetch config for fdw (host, port, user, password)
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(fdw_id);

        if (!fdw_config.contains("state") || fdw_config["state"] != Properties::FDW_STATE_INITIALIZE) {
            LOG_ERROR("PgDDLMgr::init() called with fdw_id: {} but state is not initialize", fdw_id);
        }

        // get the connection information from the FDW config
        // override hostname if passed in, used for unix domain socket connections
        if (hostname.has_value()) {
            _hostname = hostname.value();
        } else {
            Json::get_to<std::string>(fdw_config, "host", _hostname);
        }

        Json::get_to<int>(fdw_config, "port", _port);
        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            _db_prefix = fdw_config.at("db_prefix").get<std::string>();
        }
        LOG_INFO("FDW init: ID={}, Host={}, Port={}, FDW Username={}",
                 _fdw_id, _hostname, _port, _username);

        // add subscribers to pubsub threads
        _db_instance_id = Properties::get_db_instance_id();

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        int sync_interval_secs = Json::get_or(fdw_config, "sync_seconds", SYNC_INTERVAL_SECONDS);

        _init_fdw();

        _thread_manager = std::make_shared<common::MultiQueueThreadManager>(MAX_THREAD_POOL_SIZE, "DDL_MQThrMgr");
        _thread_manager->start();

        // create a new thread to run the policy and role sync
        _sync_thread = std::thread(&PgDDLMgr::_sync_thread_func, this, sync_interval_secs);
        pthread_setname_np(_sync_thread.native_handle(), "DDLMgrSync");
        LOG_INFO("PgDDLMgr::init() done");
    }

    bool
    PgDDLMgr::_execute_in_savepoint(LibPqConnectionPtr fdw_conn,
                                    LibPqConnectionPtr primary_conn,
                                    const std::string &savepoint_name,
                                    const std::string &sql,
                                    const std::string &rollback_sql)
    {
        // execute the SQL
        if (fdw_conn->exec_no_throw(sql, false)) {
            return true; // success
        }

        LOG_ERROR("Failed to execute SQL in savepoint: error: sql_state: {}, msg: {}", fdw_conn->get_sql_state(), fdw_conn->result_error_message());

        // rollback the savepoint on fdw
        fdw_conn->rollback_savepoint(savepoint_name);

        // execute rollback SQL on primary
        primary_conn->exec(rollback_sql);

        return false;
    }

    bool
    PgDDLMgr::_check_table_exists(LibPqConnectionPtr fdw_conn,
                                  const std::string &schema_name,
                                  const std::string &table_name,
                                  uint32_t table_oid)
    {
        // check if the table exists in the fdw database
        fdw_conn->exec(fmt::format(TABLE_EXISTS_SELECT, table_oid, table_oid,
                                   fdw_conn->escape_string(schema_name),
                                   fdw_conn->escape_string(table_name)));

        if (fdw_conn->ntuples() == 0) {
            LOG_ERROR("Table {}.{} with OID {} does not exist in the FDW database",
                      schema_name, table_name, table_oid);
            return false;
        }

        return true;
    }

    void
    PgDDLMgr::_policy_sync_database(LibPqConnectionPtr conn,
                                    LibPqConnectionPtr fdw_conn,
                                    uint64_t db_id)
    {
        // execute on primary
        conn->exec(fmt::format(POLICY_DIFF_SELECT, _fdw_id));

        if (conn->ntuples() == 0) {
            return;
        }

        std::string sql;

        // iterate through the result set and process the policy diffs
        for (int i = 0; i < conn->ntuples(); i++) {
            std::string diff_type = conn->get_string(i, 0);
            std::string table_name_nesc = conn->get_string(i, 1);
            std::string table_name = conn->escape_identifier(table_name_nesc);
            std::string schema_name_nesc = conn->get_string(i, 2);
            std::string schema_name = conn->escape_identifier(schema_name_nesc);
            uint32_t policy_oid = conn->get_int32(i, 3);
            std::string policy_name = conn->escape_identifier(conn->get_string(i, 4));

            if (diff_type == "REMOVED") {
                fdw_conn->exec(fmt::format("DROP POLICY IF EXISTS {} ON {}.{}",
                                           policy_name, schema_name, table_name));
                continue;
            }

            // these are only valid for ADDED and MODIFIED diffs
            bool policy_permissive = conn->get_boolean(i, 6);
            std::string policy_roles = conn->get_string(i, 7);
            std::string policy_qual = conn->get_string(i, 8);
            uint32_t table_oid = conn->get_int32(i, 9);

            // convert roles to {} from json array string
            nlohmann::json roles_json = nlohmann::json::parse(policy_roles);
            std::vector<std::string> roles;
            for (auto &role : roles_json) {
                roles.push_back(conn->escape_identifier(role.get<std::string>()));
            }
            policy_roles = fmt::format("{}", common::join_string(", ", roles));

            auto rollback_sql = fmt::format("SELECT __pg_springtail_triggers.policy_remove('{}', {})",
                                            _fdw_id, policy_oid);
            /*
             * Check if the table associated with the policy exist, if not we remove
             * the policy from the sync and skip creating it.
             * Always drop and recreate the policy, as it is easier to manage
             */
            if (diff_type == "ADDED" || diff_type == "MODIFIED") {
                // verify tid exists
                if (!_check_table_exists(fdw_conn, schema_name_nesc, table_name_nesc, table_oid)) {
                    // table does not exist; remove entry from sync
                    conn->exec(rollback_sql);
                    continue;
                }

                auto savepoint_name = "ddl_policy_sync";
                fdw_conn->savepoint(savepoint_name);

                // easier to drop and recreate the policy
                sql = fmt::format("DROP POLICY IF EXISTS {} ON {}.{}",
                                  policy_name, schema_name, table_name);
                if (!_execute_in_savepoint(fdw_conn, conn, savepoint_name, sql, rollback_sql)) {
                    continue;
                }

                // create the new policy -- this could fail if the table doesn't exist yet
                sql = fmt::format("CREATE POLICY {} ON {}.{} "
                                  "AS {} "
                                  "FOR SELECT TO {} "
                                  "USING ({})",
                                  policy_name, schema_name, table_name, policy_permissive ? "PERMISSIVE" : "RESTRICTIVE",
                                  policy_roles, policy_qual);
                if (!_execute_in_savepoint(fdw_conn, conn, savepoint_name, sql, rollback_sql)) {
                    continue;
                }

                // release the savepoint
                fdw_conn->release_savepoint(savepoint_name);

                continue;
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_roles_sync_database(LibPqConnectionPtr conn,
                                   LibPqConnectionPtr fdw_conn,
                                   uint64_t db_id)
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
            if (role == _username) {
                // skip the fdw user, they are not replicated from primary
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Skipping role {} as it is the fdw or ddl mgr user", role);
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
                fdw_conn->exec(fmt::format("DROP ROLE IF EXISTS {}", role_name));
                continue;
            }

            if (diff_type == "ADDED") {
                // create a new user with password, even if nologin is set
                // in case it changes later, we can control with LOGIN/NOLOGIN
                if (!fdw_conn->exec_no_throw(fmt::format(CREATE_USER, role_name, _password))) {
                    auto sql_state = fdw_conn->get_sql_state();
                    if (sql_state.has_value() && sql_state.value() == LibPqConnection::SQL_DUPLICATE_OBJECT) {
                        // user already exists, we can skip this
                        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Role {} already exists, skipping creation", role_name);
                    } else {
                        // some other error, log it
                        LOG_ERROR("Failed to create role {}: {}", role_name, fdw_conn->result_error_message());
                        throw DDLSqlSyncError(fmt::format("Failed to create role {}: {}", role_name, fdw_conn->result_error_message()));
                    }
                }
                // fall through
            }

            if (diff_type == "MODIFIED" || diff_type == "ADDED") {
                // modify the existing role
                fdw_conn->exec(
                    fmt::format("ALTER ROLE {} WITH {} {} {}",
                                role_name,
                                (rolsuper || rolbypassrls) ? "BYPASSRLS" : "NOBYPASSRLS",
                                rolinherit ? "INHERIT" : "NOINHERIT",
                                rolcanlogin ? "LOGIN" : "NOLOGIN"));

                if (!rolconfig_changed) {
                    continue; // no config changes, skip
                }
                // if the role config has changed, we need to set it
                if (!rolconfig.has_value() || rolconfig.value().empty()) {
                    fdw_conn->exec(fmt::format("ALTER ROLE {} RESET ALL", role_name));
                } else {
                    // stored as json text array
                    nlohmann::json config_json = nlohmann::json::parse(rolconfig.value());
                    DCHECK(config_json.is_array()) << "Role config must be a JSON array";

                    // iterate through the config and set each one
                    for (const auto &config : config_json.items()) {
                        std::string config_str = config.value().get<std::string>();
                        fdw_conn->exec(fmt::format("ALTER ROLE {} SET {}", role_name, config_str));
                    }
                }
            }
        }

        conn->clear();
    }

    void
    PgDDLMgr::_role_member_sync_database(LibPqConnectionPtr conn,
                                         LibPqConnectionPtr fdw_conn,
                                         uint64_t db_id)
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
                fdw_conn->exec(fmt::format("REVOKE {} FROM {}", role_name, member_name));
                continue;
            }

            if (diff_type == "ADDED") {
                fdw_conn->exec(fmt::format("GRANT {} TO {}", role_name, member_name));
                continue;
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_table_owner_sync_database(LibPqConnectionPtr conn,
                                         LibPqConnectionPtr fdw_conn,
                                         uint64_t db_id)
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
            std::string table_name_nesc = conn->get_string(i, 1);
            std::string table_name = conn->escape_identifier(table_name_nesc);
            std::string schema_name_nesc = conn->get_string(i, 2);
            std::string schema_name = conn->escape_identifier(schema_name_nesc);
            std::string role_owner_name = conn->escape_identifier(conn->get_string(i, 3));
            uint32_t table_oid = conn->get_int32(i, 4);

            if (diff_type == "REMOVED") {
                // nothing to do these tables are already dropped
                continue;
            }

            auto rollback_sql = fmt::format("SELECT __pg_springtail_triggers.table_owner_remove('{}', {})",
                                            _fdw_id, table_oid);

            if (diff_type == "ADDED" || diff_type == "MODIFIED") {
                // verify tid exists
                if (!_check_table_exists(fdw_conn, schema_name_nesc, table_name_nesc, table_oid)) {
                    // table does not exist; remove entry from sync
                    conn->exec(rollback_sql);
                    continue;
                }

                auto savepoint_name = "ddl_table_owner_sync";
                fdw_conn->savepoint(savepoint_name);

                if (!_execute_in_savepoint(fdw_conn, conn, savepoint_name,
                                      fmt::format("ALTER FOREIGN TABLE {}.{} OWNER TO {}",
                                                  schema_name, table_name, role_owner_name),
                                      rollback_sql)) {
                    continue;
                }

                // release the savepoint
                fdw_conn->release_savepoint(savepoint_name);
            }
        }
        conn->clear();
    }

    void
    PgDDLMgr::_sync_thread_func(int sync_interval_secs)
    {
        std::string host;
        int port;
        std::string user;
        std::string password;

        bool first_pass = true;

        std::string coordinator_id = fmt::format(DDL_SYNC_WORKER_ID, _fdw_id);
        auto coordinator = Coordinator::get_instance();
        auto& keep_alive = coordinator->register_thread(Coordinator::DaemonType::DDL_MGR, coordinator_id);

        // run the sync thread every 10 seconds
        while (!_is_shutting_down()) {
            // mark keep alive with coordinator
            Coordinator::mark_alive(keep_alive);

            try {
                // get primary db connection
                Properties::get_primary_db_config(host, port, user, password);
                auto conn = std::make_shared<LibPqConnection>();

                // iterate through all databases and perform sync operations
                auto databases = Properties::get_instance()->get_databases();
                for (auto &[db_id, db_name] : databases) {
                    std::vector<std::string> sql_commands;

                    // connect to the primary database
                    // TODO: retry connection on failure SPR-896
                    conn->connect(host, db_name, user, password, port, false);
                    conn->start_transaction();

                    auto fdw_conn = _get_fdw_connection(db_id, db_name);
                    fdw_conn->start_transaction();

                    // these operations perform on the fdw connection as well as the primary
                    // if there is an error, we rollback both transactions
                    try {
                        if (first_pass) {
                            // on the first pass, reset the snapshot history tables
                            conn->exec(fmt::format(RESET_HISTORY_DIFFS,
                                                   _fdw_id, _fdw_id, _fdw_id, _fdw_id));
                        }

                        // sync user roles for this database
                        _roles_sync_database(conn, fdw_conn, db_id);

                        // sync role members for this database
                        _role_member_sync_database(conn, fdw_conn, db_id);

                        // sync policies for this database
                        _policy_sync_database(conn, fdw_conn, db_id);

                        // sync table owners for this database
                        _table_owner_sync_database(conn, fdw_conn, db_id);

                        // commit the transaction for both connections
                        fdw_conn->end_transaction();
                        conn->end_transaction();
                    } catch (const std::exception &e) {
                        LOG_ERROR("Error applying SQL commands for database {}: {}", db_name, e.what());
                        // on error we should drop the primary fdw policy table and try resyncing from scratch
                        fdw_conn->rollback_transaction();
                        conn->rollback_transaction();
                    }

                    // disconnect from both connections
                    _release_fdw_connection(db_id, fdw_conn);
                    conn->disconnect();
                }
            } catch (const std::exception &e) {
                LOG_ERROR("Error syncing policies: {}", e.what());
                DCHECK(false) << "Error syncing policies and roles";
                // on error we should drop the primary fdw policy table and try resyncing from scratch
            }

            // after the first successful pass, set the FDW state to running
            if (first_pass) {
                LOG_INFO("Initial DDL sync pass complete");
                first_pass = false;
                Properties::get_instance()->set_fdw_state(Properties::FDW_STATE_RUNNING);
            }

            // sleep for sync_interval_secs
            auto lock = std::unique_lock<std::mutex>(_sync_shutdown_mutex);
            if (!_is_shutting_down()) {
                _sync_shutdown_cv.wait_for(
                    lock,
                    std::chrono::seconds(sync_interval_secs)
                );
            }
        }
        LOG_INFO("Sync thread exiting");
    }

    PgDDLMgr::UserTypeMap
    PgDDLMgr::_get_usertypes(uint64_t db_id, uint64_t xid)
    {
        // namespace id -> type_id -> <type_name, value_json>
        UserTypeMap usertype_map;

        // iterate through the user types and add them to the map
        auto table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::UserTypes::ID, xid);
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
            LOG_INFO("Adding user type: {}.{} = {}:{}", namespace_id, type_id, type_name, value_json);
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

        LOG_INFO("Scanning for schemas in db_id={}, xid={}", db_id, xid);

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
        auto table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, xid);
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
        auto table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, xid);
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

            // check if table is a parent partition; thus a regular table
            bool is_parent_partition = false;
            if (!fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->is_null(&row)) {
                is_parent_partition = true;
            }

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
                is_parent_partition ? "" : "FOREIGN",
                conn->escape_identifier(schema_name), conn->escape_identifier(table_name),
                rls_forced ? ", FORCE ROW LEVEL SECURITY" : "");


            // execute the SQL command to enable RLS
            LOG_INFO("Applying RLS SQL command: {}", sql);
            conn->exec(sql);
            conn->clear();
        }
    }

    void
    PgDDLMgr::_init_fdw()
    {
        // get map of dbs id:name from redis
        auto dbs = Properties::get_databases();

        // connect to the db to setup the replicated dbs, the fdw user and foreign servers
        LibPqConnectionPtr conn = _get_fdw_connection(std::nullopt, "template1");

        // Populate system defined types
        conn->exec(SYSTEM_TYPES);
        int rows = conn->ntuples();
        for (int i = 0; i < rows; i++) {
            uint64_t oid = conn->get_int32(i, 0);
            std::string name = conn->escape_identifier(conn->get_string(i, 1));
            std::string category = conn->escape_identifier(conn->get_string(i, 2));
            _type_cache[oid] = std::make_tuple(name, category);
        }
        conn->clear();
        conn->disconnect();
        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Populated system defined types: {}", _type_cache.size());

        // go through each db and drop/create the database on the fdw
        for (const auto &[db_id, db_name] : dbs) {
            _add_replicated_database(db_id, db_name, false);
        }

        // drop other roles that are not fdw or ddl mgr user
        // must be done after databases are dropped to remove dependencies
        conn = _get_fdw_connection(std::nullopt, "template1");

        // get all roles except the fdw user
        conn->exec(fmt::format(DROP_ROLES, _username));
        conn->clear();
        conn->disconnect();
    }

    std::string
    PgDDLMgr::_get_create_schema_with_grants_query(std::string_view schema)
    {
        /** Create schema with grants, params: schema, schema, user, schema, user, schema, user */
        return fmt::format("CREATE SCHEMA {};", schema);
#if 0
        return fmt::format("CREATE SCHEMA {};"
                "  GRANT USAGE ON SCHEMA {} TO {};"
                "  GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};"
                "  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {};",
                schema,
                schema, _fdw_username,
                schema, _fdw_username,
                schema, _fdw_username);
#endif
    }

    std::string
    PgDDLMgr::_get_alter_schema_with_grants_query(std::string_view old_schema, std::string_view new_schema)
    {
        return fmt::format("ALTER SCHEMA {} RENAME TO {};", old_schema, new_schema);
#if 0
        /** Alter schema with grants, params: old_schema, new_schema, new_schema, user, new_schema, user, new_schema, user */
        return fmt::format("ALTER SCHEMA {} RENAME TO {};"
            "  GRANT USAGE ON SCHEMA {} TO {};"
            "  GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};"
            "  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {};",
                old_schema, new_schema,
                new_schema, _fdw_username,
                new_schema, _fdw_username,
                new_schema, _fdw_username);
#endif
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

    // Recursive DFS
    void traverse(int tid,
                  const std::unordered_map<int, std::vector<int>>& children_map,
                  const std::unordered_map<int, std::vector<nlohmann::json>>& table_map,
                  nlohmann::json& output)
    {
        // Process this node's actions: drop before create
        auto it = table_map.find(tid);
        if (it != table_map.end()) {
            std::vector<nlohmann::json> actions = it->second;
            std::ranges::stable_sort(actions, [](const nlohmann::json& a, const nlohmann::json& b) {
                bool resyncA = a.value("is_resync", false);
                bool resyncB = b.value("is_resync", false);

                // Only reorder when both are resync actions
                if (!(resyncA && resyncB)) {
                    return false; // preserve input order
                }

                // Within resync, ensure DROP before CREATE
                if (a["action"] == b["action"])
                    return false;

                return a["action"] == "drop" && b["action"] == "create";
            });
            for (const auto& act : actions) {
                output.push_back(act);
            }
        }

        // Recurse into children
        auto child_iter = children_map.find(tid);
        if (child_iter != children_map.end()) {
            std::vector<int> child_ids = child_iter->second;
            std::ranges::sort(child_ids);
            for (int childId : child_ids) {
                traverse(childId, children_map, table_map, output);
            }
        }
    }

    nlohmann::json
    sort_ddls_by_hierarchy(nlohmann::json arr)
    {
        // Build lookup maps
        std::unordered_map<int, std::vector<nlohmann::json>> table_map;
        std::unordered_map<int, std::vector<int>> children_map;
        std::unordered_map<int, int> parent_map;
        std::vector<int> roots;

        // Collect passthrough actions (not create/drop)
        nlohmann::json passthrough = nlohmann::json::array();

        // Pass 1: collect all tables
        for (const auto &obj : arr) {
            auto action = obj["action"].get<std::string>();

            // Only create or drop actions are processed. They are the ones that are used in the resync flow
            if (action == "create" || action == "drop") {
                int tid = obj["tid"].get<int>();
                table_map[tid].push_back(obj);
            } else {
                passthrough.push_back(obj);
            }
        }

        // Pass 2: build parent-child relationships
        for (const auto &obj : arr) {
            auto action = obj["action"].get<std::string>();
            // Only create or drop actions are processed. They are the ones that are used in the resync flow
            if (action != "create" && action != "drop")
                continue;

            int tid = obj["tid"].get<int>();
            if (obj.contains("parent_table_id")) {
                int parent = obj["parent_table_id"].get<int>();

                if (table_map.contains(parent)) {
                    parent_map[tid] = parent;
                    auto &vec = children_map[parent];
                    if (std::ranges::find(vec, tid) == vec.end()) {
                        vec.push_back(tid);
                    }
                } else {
                    // Parent truly missing
                    if (std::ranges::find(roots, tid) == roots.end()) {
                        roots.push_back(tid);
                    }
                    LOG_INFO("[WARN] Parent {} not found in JSON, treating {} as root", parent, tid);
                }
            } else {
                // No parent_table_id -> root
                if (std::ranges::find(roots, tid) == roots.end()) {
                    roots.push_back(tid);
                }
            }
        }

        // Pass 3: detect any without parent
        for (const auto &[tid, _] : table_map) {
            if (!parent_map.contains(tid) && std::ranges::find(roots, tid) == roots.end()) {
                roots.push_back(tid);
            }
        }

        std::ranges::sort(roots);

        // Traverse hierarchy
        nlohmann::json sorted = nlohmann::json::array();
        for (int rootTid : roots) {
            traverse(rootTid, children_map, table_map, sorted);
        }

        // Combine passthrough + sorted
        nlohmann::json sorted_ddls = nlohmann::json::array();
        for (const auto &obj : passthrough) {
            sorted_ddls.push_back(obj);
        }
        for (const auto &obj : sorted) {
            sorted_ddls.push_back(obj);
        }

        return sorted_ddls;
    }

    void
    PgDDLMgr::run()
    {
        // move any pending DDLs to the active queue
        RedisDDL::get_instance()->abort_fdw(_fdw_id);

        while (!_is_shutting_down()) {
            try {
                // blocking redis call to get next set of DDL statements
                auto &&ddls_vec = RedisDDL::get_instance()->get_next_ddls(_fdw_id);
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
                    auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});

                    LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Original DDLS: {}", ddls.dump(4));

                    // Sort the DDLs based on their hierarchy
                    auto sorted_ddls = sort_ddls_by_hierarchy(ddls);

                    LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Sorted DDLS: {}", sorted_ddls.dump(4));

                    uint64_t current_xid = constant::INVALID_XID;
                    if (_db_xid_map.contains(db_id)) {
                        current_xid = _db_xid_map[db_id];
                    }
                    if (current_xid >= schema_xid) {
                        LOG_WARN("Schema XID has already been applied: db_id={}, current={}, new={}",
                                    db_id, current_xid, schema_xid);
                    } else {
                        LOG_INFO("New schema XID will be applied: db_id={}, current={}, new={}",
                                  db_id, current_xid, schema_xid);
                        db_map[db_id][schema_xid] = sorted_ddls;
                    }
                }

                db_lock.unlock();

                if (db_map.empty()) {
                    LOG_WARN("All schemas have already been applied");
                    RedisDDL::get_instance()->commit_fdw_no_update(_fdw_id);
                    continue;
                }

                // queue each DBs DDL statements for processing
                for (const auto &[db_id, xid_map] : db_map) {
                    _thread_manager->queue_request(std::make_shared<common::MultiQueueRequest>(
                        db_id, [this, db_id, xid_map]() {
                            while (true) {
                                std::shared_lock db_name_lock(_db_name_mutex);
                                std::shared_lock db_xid_lock(_db_mutex);
                                if (!_db_name_map.contains(db_id)) {
                                    if (_db_xid_map.contains(db_id)) {
                                        // warn and return
                                        LOG_WARN("DDL changes for database {} won't be applied as database is being removed", db_id);
                                        return;
                                    } else {
                                        db_xid_lock.unlock();
                                        db_name_lock.unlock();
                                        try {
                                            std::string db_state = Properties::get_db_state(db_id);
                                            if (db_state != redis::db_state_change::REDIS_STATE_INITIALIZE) {
                                                return;
                                            }
                                        } catch (RedisNotFoundError &e) {
                                            LOG_ERROR("Failed to find database state: {}", e.what());
                                            return;
                                        }
                                        // sleep and continue
                                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                                        continue;
                                    }
                                }
                                break;
                            }
                            try {
                                uint64_t schema_xid = xid_map.rbegin()->first;
                                auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});

                                LOG_DEBUG(
                                    LOG_FDW, LOG_LEVEL_DEBUG1, "Updating redis ddl @ through schema XID: {}, db_id: {}",
                                    schema_xid, db_id);

                                // apply the DDL statements
                                bool status = _update_schemas(db_id, xid_map);
                                if (!status) {
                                    // error occurred, abort the DDL
                                    LOG_ERROR("Failed to apply DDL statements");
                                    RedisDDL::get_instance()->abort_fdw(_fdw_id);
                                    DCHECK(false);
                                    return;
                                }

                                // success, update schema XID if applied, otherwise they may be
                                // queued
                                RedisDDL::get_instance()->update_schema_xid(_fdw_id, db_id, schema_xid);

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

        LOG_INFO("DDL manager thread exiting");
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
                LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Reusing connection for db_id: {}, db_name: {}", db_id, db_name);
                // evict the connection from the cache with no callback
                // this is so that it can be used and no-one else will try to use it
                _fdw_conn_cache.evict(db_id, true);
                return conn;
            }
            _fdw_conn_cache.evict(db_id);
        }

        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Establishing connection for db_id: {}, db_name: {}", db_id, db_name);

        // use libpq to connect to the database
        std::vector<std::pair<std::string, std::string>> options = {{"springtail_fdw.ddl_connection", "on"}};
        conn = std::make_shared<LibPqConnection>();
        conn->connect(_hostname, _db_prefix + db_name, _username, _password, _port, false, options);

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
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Releasing connection for db_id: {}", db_id);
            _fdw_conn_cache.insert(db_id, conn);
            return;
        }

        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Releasing existing connection for db_id: {}", db_id);
        conn->disconnect();
    }

    bool
    PgDDLMgr::_update_schemas(uint64_t db_id,
                              const std::map<uint64_t, nlohmann::json> &xid_map)
    {
        // get the database name for the db_id; XXX should see if we can swtich to OID
        std::string db_name = Properties::get_db_name(db_id);
        LibPqConnectionPtr conn = _get_fdw_connection(db_id, db_name);
        std::vector<std::string> txn;

        try {
            // generate a DDL statement for each JSON in the transaction
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
            _execute_ddl(conn, db_id, txn);

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
                           const std::vector<std::string> &txn)
    {
        // start a transaction
        conn->start_transaction();

        // exectute each DDL statement
        for (const auto &sql : txn) {
            LOG_INFO("Executing DDL: {}", sql);
            conn->exec(sql);
            conn->clear();
        }

        conn->end_transaction();
    }

    PartitionInfo
    _get_partition_info(const nlohmann::json &ddl){
        uint64_t parent_table_id = 0;
        std::string parent_namespace_name;
        std::string parent_table_name;
        std::string partition_key;
        std::string partition_bound;

        if (ddl.contains("parent_table_id") && !ddl.at("parent_table_id").is_null()) {
            parent_table_id = ddl.at("parent_table_id").get<uint64_t>();
        }

        if (ddl.contains("parent_namespace_name") && !ddl.at("parent_namespace_name").is_null()) {
            parent_namespace_name = ddl.at("parent_namespace_name").get<std::string>();
        }

        if (ddl.contains("parent_table_name") && !ddl.at("parent_table_name").is_null()) {
            parent_table_name = ddl.at("parent_table_name").get<std::string>();
        }

        if (ddl.contains("partition_key") && !ddl.at("partition_key").is_null()) {
            partition_key = ddl.at("partition_key").get<std::string>();
        }

        if (ddl.contains("partition_bound") && !ddl.at("partition_bound").is_null()) {
            partition_bound = ddl.at("partition_bound").get<std::string>();
        }

        PartitionInfo partition_info(parent_table_id,
                                     parent_namespace_name,
                                     parent_table_name,
                                     partition_key,
                                     partition_bound);

        return partition_info;
    }

    std::string
    PgDDLMgr::_gen_sql_from_json(LibPqConnectionPtr conn,
                                 const std::string &server_name,
                                 const nlohmann::json &ddl)
    {
        assert(ddl.is_object());
        assert(ddl.contains("action"));

        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "DDL JSON: {}", ddl.dump(4));

        // determine if this is a foreign table or regular table operation
        // if partition_key is empty, then it's a foreign table, since it is a leaf table
        PartitionInfo partition_info = _get_partition_info(ddl);
        bool is_foreign_table_type = partition_info.partition_key().empty();

        auto const &action = ddl.at("action");
        if (action == "create") { // create table
            // generate the CREATE TABLE statement
            std::vector<PgFdwCommon::ColumnInfo> columns;

            for (const auto &col : ddl.at("columns")) {
                auto fully_qualified_type_name = fmt::format("{}.{}",
                                        conn->escape_identifier(col.at("type_namespace").get<std::string>()),
                                        conn->escape_identifier(col.at("type_name").get<std::string>()));
                columns.emplace_back(col.at("name"),
                                     fully_qualified_type_name,
                                     col.at("nullable"));
            }

            return PgFdwCommon::_gen_fdw_table_sql(server_name, ddl.at("schema"), ddl.at("table"), ddl.at("tid"), columns,
                                                   partition_info, is_foreign_table_type,
                                                   [conn](const std::string &name) {
                                                        return conn->escape_identifier(name.c_str());
                                                   });
        }

        else if (action == "rename") { // rename table
            std::string rename = fmt::format("ALTER {} TABLE {}.{} RENAME TO {};",
                                             is_foreign_table_type ? "FOREIGN" : "",
                                             conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                             conn->escape_identifier(ddl.at("old_table").get<std::string>()),
                                             conn->escape_identifier(ddl.at("table").get<std::string>()));

            // XXX it's not clear to me that we need to support a schema change here?
            if (ddl.at("schema").get<std::string>() != ddl.at("old_schema").get<std::string>()) {
                return rename + fmt::format("ALTER {} TABLE {}.{} SET SCHEMA {};",
                                            is_foreign_table_type ? "FOREIGN" : "",
                                            conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                            conn->escape_identifier(ddl.at("table").get<std::string>()),
                                            conn->escape_identifier(ddl.at("schema").get<std::string>()));
            } else {
                return rename;
            }
        }

        else if (action == "drop") {  // drop table
            return fmt::format("DROP {} TABLE IF EXISTS {}.{};",
                               is_foreign_table_type ? "FOREIGN" : "",
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
            std::string type_name;
            auto it = _type_cache.find(type_oid);
            if (it != _type_cache.end()) {
                type_name = std::get<0>(it->second);
            }
            return fmt::format("ALTER {} TABLE {}.{} ADD COLUMN {} {} {};",
                               is_foreign_table_type ? "FOREIGN" : "",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               type_name,
                               constraints);
        }

        else if (action == "col_drop") {  // alter table drop column
            return fmt::format("ALTER {} TABLE {}.{} DROP COLUMN {};",
                               is_foreign_table_type ? "FOREIGN" : "",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("column").get<std::string>()));
        }
#endif /* ENABLE_SCHEMA_MUTATES */

        else if (action == "col_rename") {
            return fmt::format("ALTER {} TABLE {}.{} RENAME COLUMN {} TO {};",
                               is_foreign_table_type ? "FOREIGN" : "",
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
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Creating schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return _get_create_schema_with_grants_query(escaped_schema);
        }
        else if (action == "ns_alter") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Altering schema with JSON: {}", ddl.dump());
            const auto old_schema = conn->escape_identifier(ddl.at("old_name").get<std::string>());
            const auto new_schema = conn->escape_identifier(ddl.at("name").get<std::string>());

            return _get_alter_schema_with_grants_query(old_schema, new_schema);
        }
        else if (action == "ns_drop") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Dropping schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return fmt::format("DROP SCHEMA IF EXISTS {} CASCADE", escaped_schema);
        }
        else if (action == "set_namespace") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Set namespace with JSON: {}", ddl.dump());
            const auto schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto table = conn->escape_identifier(ddl.at("table").get<std::string>());
            const auto old_schema = conn->escape_identifier(ddl.at("old_schema").get<std::string>());

            return fmt::format("ALTER {} TABLE {}.{} SET SCHEMA {};",
                               is_foreign_table_type ? "FOREIGN" : "",
                               old_schema, table, schema);
        }
        else if (action == "ut_create") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Creating user type with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto escaped_name = conn->escape_identifier(ddl.at("name").get<std::string>());
            const auto value_json_str = ddl.at("value").get<std::string>();

            return _get_create_type_query(escaped_schema, escaped_name, value_json_str, conn);
        }
        else if (action == "ut_drop") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Dropping user type with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto escaped_name = conn->escape_identifier(ddl.at("name").get<std::string>());

            return fmt::format("DROP TYPE IF EXISTS {}.{} CASCADE", escaped_schema, escaped_name);
        } else if (action == "ut_alter") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Altering user type with JSON: {}", ddl.dump());

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
        } else if (action == "attach_partition") {
            std::string alter = fmt::format("ALTER TABLE {}.{} ATTACH PARTITION {}.{} {};",
                                            conn->escape_identifier(ddl.at("schema")),
                                            conn->escape_identifier(ddl.at("table")),
                                            conn->escape_identifier(ddl.at("partition_schema")),
                                            conn->escape_identifier(ddl.at("partition_name")),
                                            ddl.at("partition_bound").get<std::string>());

            return alter;
        } else if (action == "detach_partition") {
            std::string alter = fmt::format("ALTER TABLE {}.{} DETACH PARTITION {}.{};",
                                            conn->escape_identifier(ddl.at("schema")),
                                            conn->escape_identifier(ddl.at("table")),
                                            conn->escape_identifier(ddl.at("partition_schema")),
                                            conn->escape_identifier(ddl.at("partition_name")));

            return alter;
        } else if (action == "set_rls_enabled") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Setting RLS enabled with JSON: {}", ddl.dump());
            const auto schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto table = conn->escape_identifier(ddl.at("table").get<std::string>());
            bool rls_enabled = ddl.at("rls_enabled").get<bool>();

            return fmt::format("ALTER {} TABLE {}.{} {} ROW LEVEL SECURITY;",
                               is_foreign_table_type ? "FOREIGN" : "",
                               schema, table,
                               rls_enabled ? "ENABLE" : "DISABLE");
        } else if (action == "set_rls_forced") {
            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Setting RLS forced with JSON: {}", ddl.dump());
            const auto schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto table = conn->escape_identifier(ddl.at("table").get<std::string>());
            bool rls_forced = ddl.at("rls_forced").get<bool>();

            return fmt::format("ALTER {} TABLE {}.{} {} ROW LEVEL SECURITY;",
                               is_foreign_table_type ? "FOREIGN" : "",
                               schema, table,
                               rls_forced ? "FORCE" : "NO FORCE");
        }

        // can't currently support other kinds of DDL mutations
        LOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        CHECK(false);

        return {};
    }

    void
    PgDDLMgr::_create_database(LibPqConnectionPtr conn,
                               const uint64_t db_id,
                               const std::string &db_name)
    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        LOG_INFO("Creating DB ID: {}, DB Name: {} (dropping then creating)", db_id, db_name);

        // drop and create database on fdw
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format("DROP DATABASE IF EXISTS {} WITH (FORCE)", prefixed_name);
        std::string create_db = fmt::format("CREATE DATABASE {}", prefixed_name);

        conn->exec(drop_db);
        conn->clear();

        conn->exec(create_db);
        conn->clear();

        // grant connect to the fdw user
        // conn->exec(fmt::format("GRANT CONNECT ON DATABASE {} TO {}", prefixed_name, _fdw_username));
        conn->clear();
    }

    std::string
    PgDDLMgr::_get_type_name(int32_t pg_type,
                             uint64_t namespace_id,
                             const std::string &namespace_name,
                             const UserTypeMap &user_types)
    {
        if (pg_type >= constant::FIRST_USER_DEFINED_PG_OID) {
            auto it = user_types.find(namespace_id);
            if (it != user_types.end()) {
                auto it2 = it->second.find(pg_type);
                if (it2 != it->second.end()) {
                    const auto fully_qualified_type_name = fmt::format("{}.{}", namespace_name, it2->second.first);
                    return fully_qualified_type_name;
                }
            }
        }

        auto it = _type_cache.find(pg_type);
        if (it != _type_cache.end()) {
            return std::get<0>(it->second);
        }

        // Unknown type, should never get here
        DCHECK(false);
        return "UNKNOWN";
    }

    void
    PgDDLMgr::_create_schemas(const uint64_t db_id,
                              const std::string &db_name)
    {

        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
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
                    LOG_DEBUG(LOG_FDW,LOG_LEVEL_DEBUG1,  "Creating user type: {}.{} = {}", escaped_schema, escaped_type, value_json_str);
                    std::string create_type_query = _get_create_type_query(escaped_schema, escaped_type, value_json_str, conn);
                    conn->exec(create_type_query);
                    conn->clear();
                }
                conn->end_transaction();
            }

            // Create the parent partition tables
            std::vector<std::string> ddl = PgFdwCommon::get_schema_ddl(db_id, xid, SPRINGTAIL_FDW_SERVER_NAME, schema.second,
                                             false, false, {},
                                             [this, escaped_schema, &user_types](uint32_t pg_type, uint64_t namespace_id) {
                                                 return _get_type_name(pg_type, namespace_id, escaped_schema, user_types);
                                             },
                                             [conn](const std::string &name) {
                                                 return conn->escape_identifier(name.c_str());
                                             }, false);

            // Create the partition tables
            for (const auto &partition_table_sql : ddl) {
                conn->exec(partition_table_sql);
                conn->clear();
            }

            // import foreign schema
            conn->exec(fmt::format("IMPORT FOREIGN SCHEMA {} FROM SERVER {} INTO {}",
                                    escaped_schema, SPRINGTAIL_FDW_SERVER_NAME,
                                    escaped_schema));
            conn->clear();
            _add_partition_table_comment(conn, db_id, schema.second, xid);
            conn->clear();

            // grant usage and select on all tables and sequences to the fdw user
#if 0 // I don't think this is needed with the new role policy stuff
            conn->exec(fmt::format("GRANT USAGE ON SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
            conn->exec(fmt::format("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
            conn->exec(fmt::format("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}", escaped_schema, _fdw_username));
            conn->clear();
#endif
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
        RedisDDL::get_instance()->update_schema_xid(_fdw_id, db_id, xid);
        LOG_INFO("Schema initialization complete for db_id={}, db_name={}, xid={}",
                 db_id, db_name, xid);
    }

    void
    PgDDLMgr::_add_partition_table_comment(LibPqConnectionPtr conn, const uint64_t db_id, const std::string &schema_name, const uint64_t xid)
    {
        // 1. look up schema id in NamespaceNames (use inverse iterator)
        auto ns_table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::NamespaceNames::ID, xid);
        auto ns_fields = ns_table->extent_schema()->get_fields();
        auto ns_search_key = sys_tbl::NamespaceNames::Secondary::key_tuple(schema_name, xid, constant::MAX_LSN);
        auto table_iter = ns_table->inverse_lower_bound(ns_search_key, 1);

        // verify that the record is found
        CHECK(table_iter != ns_table->end(1));

        auto &ns_row = *table_iter;
        uint64_t ns_id = ns_fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&ns_row);
        uint64_t ns_xid = ns_fields->at(sys_tbl::NamespaceNames::Data::XID)->get_uint64(&ns_row);
        bool ns_exists = ns_fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&ns_row);
        DCHECK(ns_xid <= xid);
        CHECK(ns_exists);

        // 2. look up tables for the found namespace ids where partition key != NULL
        //      walk through the whole schema, collect found tables, and exclude deleted tables
        //      limit search till xid exceeds xid passed as parameter
        //      per table store table_name and table_id
        std::set<std::pair<uint64_t, std::string>> tables;
        auto table = TableMgrClient::get_instance()->get_table(db_id, sys_tbl::TableNames::ID, xid);
        auto fields = table->extent_schema()->get_fields();
        for (auto row: (*table)) {
            uint64_t table_ns_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
            std::string table_name(fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
            uint64_t table_id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
            uint64_t table_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
            bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
            std::optional<std::string> partition_key = std::nullopt;
            if (!fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->is_null(&row)) {
                partition_key = fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->get_text(&row);
            }
            if (table_xid > xid) {
                break;
            }
            if (table_ns_id != ns_id || !partition_key.has_value()) {
                continue;
            }
            if (!exists) {
                tables.erase(std::pair(table_id, table_name));
            } else {
                tables.emplace(table_id, table_name);
            }
        }

        // 3. go through the list of found tables and generate the comment SQL
        std::string escaped_schema = conn->escape_identifier(schema_name);
        for (const auto &[table_id, table_name]: tables) {
            std::string escaped_table = conn->escape_identifier(table_name);
            conn->exec(fmt::format("COMMENT ON TABLE {}.{} IS 'TID:{}';", escaped_schema, escaped_table, table_id));
        }
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

        LibPqConnectionPtr conn = nullptr;
        if (check_exists) {
            // check if the database already exists in the fdw
            // flag is set if coming from redis callback for adding db
            {
                std::shared_lock shared_lock(_db_name_mutex);
                if (_db_name_map.contains(db_id)) {
                    LOG_INFO("Database {} already exists in the fdw", db_name);
                    return;
                }
            }

            // NOTE: I think it is no longer needed
            // looks like it does not exist in database name map, so get a connection and really check
            conn = _get_fdw_connection(std::nullopt, "template1");
            std::string prefixed_name = conn->escape_string(_db_prefix + db_name);
            conn->exec(fmt::format(VERIFY_DB_EXISTS, prefixed_name));
            if (conn->ntuples() > 0) {
                // it really does exist return
                conn->disconnect();
                return;
            }
            conn->clear();
        } else {
            conn = _get_fdw_connection(std::nullopt, "template1");
        }

        // drop/add database
        _create_database(conn, db_id, db_name);
        conn->disconnect();

        // create schemas
        _create_schemas(db_id, db_name);

        {
            std::unique_lock lock(_db_name_mutex);
            _db_name_map.emplace(db_id, db_name);
        }
    }

    void
    PgDDLMgr::_remove_replicated_database(uint64_t db_id)
    {
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        std::string db_name;
        {
            std::unique_lock lock(_db_name_mutex);
            auto it = _db_name_map.find(db_id);
            if (it == _db_name_map.end()) {
                return;
            }
            db_name = it->second;
            _db_name_map.erase(it);
        }

        RedisDDL::get_instance()->clear_ddls(db_id);

        // drop database
        LibPqConnectionPtr conn = _get_fdw_connection(std::nullopt, "template1");
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format(DROP_DATABASE, prefixed_name);
        conn->exec_no_throw(drop_db);
        conn->disconnect();

        // remove it from internal storage
        {
            std::unique_lock unique_lock(_db_mutex);
            _db_xid_map.erase(db_id);
        }

        // remove connections from connection cache
        {
            std::unique_lock conn_lock(_fdw_conn_cache_mutex);
            _fdw_conn_cache.evict(db_id);
        }
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

        LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG1, "Comparing enum types: {}.{} from: {} to: {}", schema, type_name, from.dump(), to.dump());

        while (i < from.size() && j < to.size()) {
            std::string from_key = from[i].begin().key();
            std::string to_key = to[j].begin().key();

            LOG_DEBUG(LOG_FDW, LOG_LEVEL_DEBUG2, "From key: {}, From val: {}, To key: {}, To val: {}", from_key,
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
