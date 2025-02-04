#include <nlohmann/json.hpp>
#include <libpq-fe.h>

#include <common/counter.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/json.hh>

#include <redis/db_state_change.hh>
#include <redis/redis_ddl.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/libpq_connection.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_ddl_mgr.hh>

// clang-format off
// This #include must come last, as it ends up including postgres
// files that end up doing a #define ERROR, and the side effects
// of this define break other c++ headers from being includable.
#include <pg_fdw/pg_fdw_common.h>
// clang-format on

namespace springtail::pg_fdw {

    /** Get a list of all non-system schema names */
    static constexpr char SCHEMA_SELECT[] =
        "SELECT schema_name "
        "FROM information_schema.schemata "
        "WHERE schema_name NOT LIKE 'pg_%' "
        " AND schema_name <> 'information_schema'";

    static constexpr char CREATE_FDW_USER[] =
        "CREATE USER {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{}'";

    static constexpr char DROP_DATABASE[] =
        "DROP DATABASE IF EXISTS {} WITH (FORCE)";

    static constexpr char VERIFY_DB_EXISTS[] =
        "SELECT 1 FROM  pg_database WHERE datname = '{}'";

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
        try {
            fdw_config = Properties::get_fdw_config(fdw_id);
        } catch (const Error &error) {
            SPDLOG_ERROR("Error fetching fdw config: {}", error.what());
            return;
        }

        // get the connection information from the FDW config
        // override hostname if passed in, used for unix domain socket connections
        if (hostname.has_value()) {
            _hostname = hostname.value();
        } else {
            Json::get_to<std::string>(fdw_config, "host", _hostname);
        }
        Json::get_to<int>(fdw_config, "port", _port);

        // get fdw user for proxy to use, this user only has select permissions
        std::string fdw_username, fdw_password;
        Json::get_to<std::string>(fdw_config, "fdw_user", fdw_username);
        Json::get_to<std::string>(fdw_config, "password", fdw_password);

        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            _db_prefix = fdw_config.at("db_prefix").get<std::string>();
        }

        SPDLOG_DEBUG("FDW ID: {}, Host: {}, Port: {}, Username: {}, FDW Username: {}",
                     _fdw_id, _hostname, _port, _username, fdw_username);

        // add subscribers to pubsub threads
        _db_instance_id = Properties::get_db_instance_id();
        std::string db_change_channel = fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, _db_instance_id);
        _config_sub_thread.add_subscriber(db_change_channel,
            [this, &username, &password]() {
                // initialize the fdw, setup fdw server, import foreign schemas, etc
                _init_fdw(username, password);
            },
            [this](const std::string &msg) {
                _handle_replicated_dbs_change(msg);
            });

        // start redis subscriber threads
        _config_sub_thread.start();

        // start the main thread
        start_thread();
    }

    std::set<std::string>
    PgDDLMgr::_get_schemas(uint64_t db_id, const std::string &db_name)
    {
        // get the db config and parse out the included schemas
        auto db_config = Properties::get_db_config(db_id);

        bool all_schemas = false;
        std::set<std::string> schemas;

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

        if (!all_schemas) {
            return schemas;
        }

        // otherwise all schemas, need to query the primary
        // use libpq to connect to the database
        LibPqConnectionPtr conn = _connect_primary(db_id, db_name);
        conn->exec(SCHEMA_SELECT);

        // iterate through the results and get the schema names
        for (int i = 0; i < conn->ntuples(); i++) {
            schemas.insert(conn->get_string(i, 0));
        }
        conn->clear();
        conn->disconnect();

        return schemas;
    }

    void
    PgDDLMgr::_init_fdw(const std::string &username, const std::string &password)
    {
        // get map of dbs id:name from redis
        auto dbs = Properties::get_databases();

        LibPqConnectionPtr conn = _connect_fdw(std::nullopt, "postgres");

        // see if the fdw user exists, if not create it
        _fdw_username = username;
        conn->exec(fmt::format("SELECT 1 FROM pg_roles WHERE rolname = '{}'", username));
        if (conn->ntuples() == 0) {
            // create the user
            conn->clear();
            conn->exec(fmt::format(CREATE_FDW_USER, username, password));
            conn->clear();
        }

        // go through each db and drop/create the database on the fdw
        for (const auto &[db_id, db_name] : dbs) {
            _create_database(conn, db_id, db_name);
        }

        // close the connection
        conn->disconnect();

        // go through each db and create the foreign server, connect to each db
        for (const auto &[db_id, db_name] : dbs) {
            _create_schemas(conn, db_id, db_name);
        }
    }

    std::string
    PgDDLMgr::_get_create_schema_with_grants_query(std::string schema)
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
    PgDDLMgr::_get_alter_schema_with_grants_query(std::string old_schema, std::string new_schema)
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

    void
    PgDDLMgr::_internal_run()
    {
        // init redis ddl client after springtail_init()
        RedisDDL redis_ddl;

        // move any pending DDLs to the active queue
        redis_ddl.abort_fdw(_fdw_id);

        while (!_is_shutting_down()) {
            try {
                // blocking redis call to get next set of DDL statements
                // XXX we could potentially parallelize updates to different db IDs
                nlohmann::json ddls = redis_ddl.get_next_ddls(_fdw_id);
                if (ddls.empty()) {
                    continue;
                }

                uint64_t db_id = ddls.at("db_id").get<uint64_t>();
                uint64_t schema_xid = ddls.at("xid").get<uint64_t>();

                SPDLOG_DEBUG_MODULE(LOG_FDW, "Applying DDLs for db_id: {}, schema_xid: {}", db_id, schema_xid);

                std::shared_lock db_lock(_db_mutex);
                if (_db_xid_map.contains(db_id) && _db_xid_map[db_id] >= schema_xid) {
                    SPDLOG_WARN("Schema XID has already been applied: db_id={}, current={}, new={}",
                                db_id, _db_xid_map[db_id], schema_xid);
                    // unlocking here because continue will send it to the start of the while loop
                    db_lock.unlock();
                    redis_ddl.commit_fdw_no_update(_fdw_id);
                    continue;
                }
                db_lock.unlock();

                // apply the DDL statements
                bool status = _update_schemas(redis_ddl, db_id, schema_xid, ddls);
                if (!status) {
                    // error occured, abort the DDL
                    SPDLOG_ERROR("Failed to apply DDL statements");
                    redis_ddl.abort_fdw(_fdw_id);
                    assert(0);
                    continue;
                }

                // success, update schema XID if applied, otherwise they may be queued
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Updating redis ddl @ schema XID: {}, db_id: {}", schema_xid, db_id);
                redis_ddl.update_schema_xid(_fdw_id, db_id, schema_xid);
                std::unique_lock db_lock_unique(_db_mutex);
                _db_xid_map[db_id] = schema_xid;

            } catch (Error &e) {
                SPDLOG_ERROR("Springtail exception in DDL thread");
                assert(0); // assert in debug
                e.log_backtrace();
            } catch (...) {
                // handle exception
                SPDLOG_ERROR("Exception in DDL thread");
                assert(0); // assert in debug
            }
        }
    }

    LibPqConnectionPtr
    PgDDLMgr::_connect_primary(uint64_t db_id, const std::string &db_name)
    {
        // get db config and parse it
        std::string host, user, password;
        int port;
        Properties::get_primary_db_config(host, port, user, password);

        // use libpq to connect to the database
        LibPqConnectionPtr conn = std::make_shared<LibPqConnection>();
        conn->connect(host, db_name, user, password, port);

        return conn;
    }

    LibPqConnectionPtr
    PgDDLMgr::_connect_fdw(std::optional<uint64_t> db_id_opt, const std::string &db_name)
    {
        // check if we have a connection in the cache
        LibPqConnectionPtr conn = nullptr;

        if (!db_id_opt.has_value()) {
            conn = std::make_shared<LibPqConnection>();
            conn->connect(_hostname, db_name, _username, _password, _port, false);
            return conn;
        }

        uint64_t db_id = db_id_opt.value();
        conn = _fdw_conn_cache.get(db_id);

        // check if the connection is still valid
        if (conn != nullptr) {
            if (conn->is_connected()) {
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Reusing connection for db_id: {}", db_id);
                return conn;
            }
            _fdw_conn_cache.evict(db_id);
        }

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Establishing connection for db_id: {}", db_id);

        // use libpq to connect to the database
        conn = std::make_shared<LibPqConnection>();
        conn->connect(_hostname, db_name, _username, _password, _port, false);

        // save the connection in the cache
        _fdw_conn_cache.insert(db_id, conn);

        return conn;
    }

    bool
    PgDDLMgr::_update_schemas(RedisDDL &redis,
                              uint64_t db_id,
                              uint64_t schema_xid,
                              const nlohmann::json &ddls)
    {
        // get the database name for the db_id; XXX should see if we can swtich to OID
        std::string db_name = Properties::get_db_name(db_id);
        LibPqConnectionPtr conn = _connect_fdw(db_id, _db_prefix + db_name);

        try {
            std::set<std::string> schemas;

            // generate a DDL statement for each JSON in the transaction
            std::vector<std::string> txn;
            for (const auto &ddl : ddls.at("ddls")) {
                txn.push_back(_gen_sql_from_json(conn, SPRINGTAIL_FDW_SERVER_NAME, ddl, schemas));
            }

            // generate a statement to alter the server options with the schema XID
            txn.push_back(fmt::format("ALTER SERVER {} OPTIONS (SET {} '{}')",
                                      SPRINGTAIL_FDW_SERVER_NAME,
                                      SPRINGTAIL_FDW_SCHEMA_XID_OPTION,
                                      schema_xid));

            // execute the set of statements
            _execute_ddl(conn, db_id, schema_xid, txn);

            return true;

        } catch (Error &e) {
            assert(0); // assert in debug
            e.log_backtrace();
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
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Executing DDL: {}", sql);
            conn->exec(sql);
            conn->clear();
        }

        conn->end_transaction();
    }

    std::string
    PgDDLMgr::_gen_sql_from_json(LibPqConnectionPtr conn,
                                 const std::string &server_name,
                                 const nlohmann::json &ddl,
                                 std::set<std::string> &schemas)
    {
        assert(ddl.is_object());
        assert(ddl.contains("action"));

        auto const &action = ddl.at("action");
        if (action == "create") {
            std::vector<std::tuple<std::string, std::string, bool>> columns;

            // retrieve the column type names
            std::set<uint32_t> type_set;
            for (const auto &col : ddl.at("columns")) {
                type_set.insert(col.at("type").get<uint32_t>());
            }
            auto &&type_map = _query_type_names(conn, type_set);

            // save the column details
            for (const auto &col : ddl.at("columns")) {
                columns.push_back({
                        col.at("name"),
                        type_map.at(col.at("type").get<uint32_t>()),
                        col.at("nullable")
                    });
            }

            // for create table get the schema name
            schemas.insert(ddl.at("schema").get<std::string>());

            // generate the CREATE TABLE statement
            return _gen_fdw_table_sql(conn, server_name, ddl.at("schema"), ddl.at("table"),
                                      ddl.at("tid"), columns);
        }

        else if (action == "rename") {
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

        else if (action == "drop") {
            return fmt::format("DROP FOREIGN TABLE IF EXISTS {}.{};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()));
        }

        else if (action == "col_add") {
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
            auto &&type_map = _query_type_names(conn, { type_oid });
            return fmt::format("ALTER FOREIGN TABLE {}.{} ADD COLUMN {} {} {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               type_map.at(type_oid),
                               constraints);
        }

        else if (action == "col_drop") {
            return fmt::format("ALTER FOREIGN TABLE {}.{} DROP COLUMN {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("column").get<std::string>()));
        }

        else if (action == "col_rename") {
            return fmt::format("ALTER FOREIGN TABLE {}.{} RENAME COLUMN {} TO {};",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("old_name").get<std::string>()),
                               conn->escape_identifier(ddl.at("new_name").get<std::string>()));
        }

        else if (action == "col_nullable") {
            auto &col = ddl.at("column");
            return fmt::format("ALTER FOREIGN TABLE {}.{} ALTER COLUMN {} {} NOT NULL;",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               col.at("nullable").get<bool>() ? "DROP" : "SET");
        }

        else if (action == "create_index") {
            // TODO: do something?
            SPDLOG_ERROR("CREATE INDEX");
            return "";
        }
        else if (action == "drop_index") {
            // TODO: do something?
            SPDLOG_ERROR("DROP INDEX");
            return "";
        }
        else if (action == "ns_create") {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Creating schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return _get_create_schema_with_grants_query(escaped_schema);
        }
        else if (action == "ns_alter") {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Altering schema with JSON: {}", ddl.dump());
            const auto old_schema = conn->escape_identifier(ddl.at("old_name").get<std::string>());
            const auto new_schema = conn->escape_identifier(ddl.at("name").get<std::string>());

            return _get_alter_schema_with_grants_query(old_schema, new_schema);
        }
        else if (action == "ns_drop") {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Dropping schema with JSON: {}", ddl.dump());
            const auto escaped_schema = conn->escape_identifier(ddl.at("name").get<std::string>());
            return fmt::format("DROP SCHEMA IF EXISTS {} CASCADE", escaped_schema);
        }
        else if (action == "set_namespace") {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Set namespace with JSON: {}", ddl.dump());
            const auto schema = conn->escape_identifier(ddl.at("schema").get<std::string>());
            const auto table = conn->escape_identifier(ddl.at("table").get<std::string>());
            const auto old_schema = conn->escape_identifier(ddl.at("old_schema").get<std::string>());

            return fmt::format("ALTER FOREIGN TABLE {}.{} SET SCHEMA {};",
                               old_schema, table, schema);
        }

        // can't currently support other kinds of DDL mutations
        SPDLOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        assert(0);
    }

    std::set<uint32_t>
    PgDDLMgr::_type_map_difference(std::set<uint32_t> &pg_types,
                                   std::map<uint32_t, std::string> &mapped_types)
    {
        std::set<uint32_t> diff;

        // Iterators for map and set
        auto map_it = _type_map.begin();
        auto set_it = pg_types.begin();

        // Loop through both containers
        while (map_it != _type_map.end() && set_it != pg_types.end()) {
            if (map_it->first < *set_it) {
                // Key in map not in set
                ++map_it;
            } else if (map_it->first > *set_it) {
                // Key in set not in map
                diff.insert(*set_it);
                ++set_it;
            } else {
                // Keys are equal, so they exist in both
                mapped_types[map_it->first] = map_it->second;
                ++map_it;
                ++set_it;
            }
        }

        // Add any remaining keys in the set
        while (set_it != pg_types.end()) {
            diff.insert(*set_it);
            ++set_it;
        }

        return diff;
    }

    std::map<uint32_t, std::string>
    PgDDLMgr::_query_type_names(LibPqConnectionPtr conn,
                                std::set<uint32_t> pg_types)
    {
        std::map<uint32_t, std::string> type_map;

        // find the missing type names
        auto &&missing_oids = _type_map_difference(pg_types, type_map);
        if (missing_oids.empty()) {
            // have them all return the map
            return type_map;
        }

        // otherwise query the database for the missing type names
        std::string &&query = fmt::format("SELECT oid, typname FROM pg_type WHERE oid IN ({})",
                                          common::join_string(",", missing_oids.begin(), missing_oids.end()));

        conn->exec(query);

        // extract the type names from the result
        int rows = conn->ntuples();
        for (int i = 0; i < rows; ++i) {
            uint32_t oid = conn->get_int32(i, 0);
            std::string type_name = conn->get_string(i, 1);

            // store the mapping
            type_map[oid] = type_name;
            _type_map[oid] = type_name;
        }
        conn->clear();

        return type_map;
    }

    std::string
    PgDDLMgr::_gen_fdw_table_sql(LibPqConnectionPtr conn,
                                 const std::string &server_name,
                                 const std::string &schema,
                                 const std::string &table,
                                 uint64_t tid,
                                 std::vector<std::tuple<std::string, std::string, bool>> &columns)
    {
        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {}.{} (\n",
                                         conn->escape_identifier(schema),
                                         conn->escape_identifier(table));

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        for (int i = 0; i < columns.size(); i++) {
            const auto &[column_name, type_name, nullable] = columns[i];
            std::string column = fmt::format("{} {} {} {}", conn->escape_identifier(column_name),
                                             type_name, nullable ? "" : "NOT NULL",
                                             (i == columns.size() - 1) ? "" : ",");

            create += column;
        }

        create += fmt::format("\n) SERVER {} OPTIONS (tid '{}');", server_name, tid);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Generated SQL: {}", create);

        return create;
    }

    void
    PgDDLMgr::_create_database(LibPqConnectionPtr conn,
                     const uint64_t db_id,
                     const std::string &db_name)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "Creating DB ID: {}, DB Name: {}", db_id, db_name);

        // drop and create database on fdw
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format("DROP DATABASE IF EXISTS {}", prefixed_name);
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
    PgDDLMgr::_create_schemas(LibPqConnectionPtr conn,
                    const uint64_t db_id,
                    const std::string &db_name)
    {

        RedisDDL redis_ddl;

        // get schemas, parse include, fetch from primary db if necessary
        auto &&schemas = _get_schemas(db_id, db_name);

        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

        // connect to the database on the fdw
        conn = _connect_fdw(db_id, _db_prefix + db_name);

        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);

        // drop and create the fdw extension
        conn->exec(fmt::format("DROP EXTENSION IF EXISTS {} CASCADE", SPRINGTAIL_FDW_EXTENSION));
        conn->clear();

        conn->exec(fmt::format("CREATE EXTENSION {} WITH SCHEMA PUBLIC", SPRINGTAIL_FDW_EXTENSION));
        conn->clear();

        // drop and create the foreign server
        conn->exec(fmt::format("DROP SERVER IF EXISTS {}", SPRINGTAIL_FDW_SERVER_NAME));
        conn->clear();

        conn->exec(fmt::format("CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (id '{}', db_id '{}', db_name '{}', schema_xid '{}')",
                                SPRINGTAIL_FDW_SERVER_NAME, SPRINGTAIL_FDW_EXTENSION, _fdw_id, db_id, prefixed_name, xid));
        conn->clear();

        for (const auto &schema: schemas) {
            // create schema if not exists
            std::string escaped_schema = conn->escape_identifier(schema);
            conn->exec(fmt::format("CREATE SCHEMA IF NOT EXISTS {}", escaped_schema));
            conn->clear();

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

        // set the schema xid in the map
        std::unique_lock db_lock(_db_mutex);
        _db_xid_map[db_id] = xid;
        db_lock.unlock();

        // update redis with the schema xid
        redis_ddl.update_schema_xid(_fdw_id, db_id, xid);

        // close the connection
        conn->disconnect();
    }

    void
    PgDDLMgr::_add_replicated_database(uint64_t db_id)
    {
        nlohmann::json db_config = Properties::get_db_config(db_id);
        std::string db_name = db_config["name"];

        // acquire lock
        std::shared_lock shared_lock(_db_mutex);
        if (_db_xid_map.contains(db_id)) {
            return;
        }
        shared_lock.unlock();

        // verify that the database does not exist before trying to add it
        LibPqConnectionPtr conn = _connect_fdw(std::nullopt, "postgres");
        std::string prefixed_name = conn->escape_string(_db_prefix + db_name);
        conn->exec(fmt::format(VERIFY_DB_EXISTS, prefixed_name));
        if (conn->ntuples() > 0) {
            conn->disconnect();
            return;
        }
        conn->clear();

        // add database
        _create_database(conn, db_id, db_name);
        conn->disconnect();

        _create_schemas(conn, db_id, db_name);
    }

    void
    PgDDLMgr::_remove_replicated_database(uint64_t db_id)
    {
        std::shared_lock shared_lock(_db_mutex);
        if (!_db_xid_map.contains(db_id)) {
            return;
        }
        shared_lock.unlock();

        // read db_config to get database name
        nlohmann::json db_config = Properties::get_db_config(db_id);
        std::string db_name = db_config["name"];

        // drop database
        LibPqConnectionPtr conn = _connect_fdw(std::nullopt, "postgres");
        std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
        std::string drop_db = fmt::format(DROP_DATABASE, prefixed_name);
        conn->exec(drop_db);
        conn->disconnect();

        // remove it from internal storage
        std::unique_lock unique_lock(_db_mutex);
        _db_xid_map.erase(db_id);
    }

    void
    PgDDLMgr::_handle_replicated_dbs_change(const std::string &msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "Handling DB Change Action message: {}", msg);
        uint64_t db_id;
        redis::db_state_change::DBAction action;
        redis::db_state_change::parse_db_action(msg, db_id, action);
        switch (action) {
            case redis::db_state_change::DB_ACTION_ADD:
                _add_replicated_database(db_id);
                break;
            case redis::db_state_change::DB_ACTION_REMOVE:
                _remove_replicated_database(db_id);
                break;
            default:
                SPDLOG_DEBUG_MODULE(LOG_FDW, "Unsupported action: {}", redis::db_state_change::db_action_to_name[action]);
        }
    }

} // namespace springtail::pg_fdw
