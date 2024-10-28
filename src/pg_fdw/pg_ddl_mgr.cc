#include <nlohmann/json.hpp>
#include <libpq-fe.h>

#include <common/redis.hh>
#include <common/redis_ddl.hh>
#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>

#include <pg_repl/exception.hh>
#include <pg_repl/libpq_connection.hh>

#include <xid_mgr/xid_mgr_client.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_ddl_mgr.hh>
#include <pg_fdw/pg_fdw_common.h>

namespace springtail::pg_fdw {

    /** Get a list of all non-system schema names */
    static constexpr char SCHEMA_SELECT[] =
        "SELECT schema_name "
        "FROM information_schema.schemata "
        "WHERE schema_name NOT LIKE 'pg_%' "
        " AND schema_name <> 'information_schema'";

    // static vars for singleton
    PgDDLMgr* PgDDLMgr::_instance {nullptr};
    std::once_flag PgDDLMgr::_init_flag;
    std::once_flag PgDDLMgr::_shutdown_flag;

    void
    PgDDLMgr::startup(const std::string &fdw_id, const std::optional<std::string> &hostname)
    {
        // set fdw id
        _fdw_id = fdw_id;

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

        Json::get_to<std::string>(fdw_config, "fdw_user", _username);
        Json::get_to<std::string>(fdw_config, "password", _password);
        Json::get_to<int>(fdw_config, "port", _port);

        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            _db_prefix = fdw_config.at("db_prefix").get<std::string>();
        }

        // initialize the fdw, setup fdw server, import foreign schemas, etc
        _init_fdw();

        // start the main thread
        _main_thread = std::thread(&PgDDLMgr::_main_thread_fn, this);
    }

    void
    PgDDLMgr::_init() {
        _instance = new PgDDLMgr();
    }

    void
    PgDDLMgr::_shutdown()
    {
        // static method
        if (_instance == nullptr) {
            return;
        }
        _instance->_internal_shutdown();
    }

    void
    PgDDLMgr::_internal_shutdown()
    {
        _shutting_down = true;
    }

    void
    PgDDLMgr::wait_shutdown()
    {
        // join the main thread
        _main_thread.join();

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    std::set<std::string>
    PgDDLMgr::_get_schemas(int db_id, const std::string &db_name)
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
    PgDDLMgr::_init_fdw()
    {
        // get map of dbs id:name from redis
        auto dbs = Properties::get_databases();

        LibPqConnectionPtr conn = _connect_fdw("postgres");

        // go through each db and drop/create the database on the fdw
        for (const auto &[db_id, db_name] : dbs) {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "DB ID: {}, DB Name: {}", db_id, db_name);

            // drop and create database on fdw
            std::string prefixed_name = conn->escape_identifier(_db_prefix + db_name);
            std::string drop_db = fmt::format("DROP DATABASE IF EXISTS {}", prefixed_name);
            std::string create_db = fmt::format("CREATE DATABASE {}", prefixed_name);

            conn->exec(drop_db);
            conn->clear();

            conn->exec(create_db);
            conn->clear();
        }

        // close the connection
        conn->disconnect();

        // go through each db and create the foreign server, connect to each db
        for (const auto &[db_id, db_name] : dbs) {

            // get schemas, parse include, fetch from primary db if necessary
            auto schemas = _get_schemas(db_id, db_name);

            uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

            // connect to the database on the fdw
            conn = _connect_fdw(_db_prefix + db_name);

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
            }

            // import catalog schema
            std::string escaped_schema = conn->escape_identifier(SPRINGTAIL_FDW_CATALOG_SCHEMA);
            conn->exec(fmt::format("CREATE SCHEMA IF NOT EXISTS {}", escaped_schema));
            conn->clear();
            conn->exec(fmt::format("IMPORT FOREIGN SCHEMA {} FROM SERVER {} INTO {}",
                                   escaped_schema, SPRINGTAIL_FDW_SERVER_NAME,
                                   escaped_schema));
            conn->clear();

            // close the connection
            conn->disconnect();
        }
    }

    void
    PgDDLMgr::_main_thread_fn()
    {
        SPDLOG_DEBUG_MODULE(LOG_FDW, "Background worker pid: {}, fdw_id: {}", ::getpid(), _fdw_id);

        // init redis ddl client after springtail_init()
        RedisDDL redis_ddl;

        // move any pending DDLs to the active queue
        redis_ddl.abort_fdw(_fdw_id);

        while (!_shutting_down) {
            try {
                // blocking redis call to get next set of DDL statements
                // XXX we could potentially parallelize updates to different db IDs
                nlohmann::json ddls = redis_ddl.get_next_ddls(_fdw_id);
                if (ddls.empty()) {
                    continue;
                }

                // apply the DDL statements
                if (_update_schemas(redis_ddl, ddls, SPRINGTAIL_FDW_SERVER_NAME)) {
                    // update schema XID if applied, otherwise they may be queued
                    uint64_t schema_xid = ddls.at("xid").get<uint64_t>();
                    SPDLOG_DEBUG_MODULE(LOG_FDW, "Updating redis ddl @ schema XID: {}", schema_xid);
                    redis_ddl.update_schema_xid(_fdw_id, schema_xid);
                } else {
                    SPDLOG_ERROR("Failed to apply DDL statements");
                    assert(0);
                    redis_ddl.abort_fdw(_fdw_id);
                }

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
    PgDDLMgr::_connect_primary(int db_id, const std::string &db_name)
    {
        // get db config and parse it
        auto db_config = Properties::get_primary_db_config();

        std::string host, user, password;
        int port;

        Json::get_to<std::string>(db_config, "host", host);
        Json::get_to<int>(db_config, "port", port);
        Json::get_to<std::string>(db_config, "replication_user", user);
        Json::get_to<std::string>(db_config, "password", password);

        // use libpq to connect to the database
        LibPqConnectionPtr conn = std::make_shared<LibPqConnection>();
        conn->connect(host, db_name, user, password, port);

        return conn;
    }

    LibPqConnectionPtr
    PgDDLMgr::_connect_fdw(const std::string &db_name)
    {
        // use libpq to connect to the database
        LibPqConnectionPtr conn = std::make_shared<LibPqConnection>();
        conn->connect(_hostname, db_name, _username, _password, _port, false);

        return conn;
    }

    bool
    PgDDLMgr::_update_schemas(RedisDDL &redis,
                              const nlohmann::json &ddls,
                              const std::string &server_name)
    {
        // get the db id and schema xid
        uint64_t db_id = ddls.at("db_id").get<uint64_t>();
        uint64_t schema_xid = ddls.at("xid").get<uint64_t>();

        // get the database name for the db_id; XXX should see if we can swtich to OID
        std::string db_name = Properties::get_db_name(db_id);
        LibPqConnectionPtr conn = _connect_fdw(_db_prefix + db_name);

        try {
            // generate a DDL statement for each JSON in the transaction
            std::vector<std::string> txn;
            for (auto ddl : ddls.at("ddls")) {
                txn.push_back(_gen_sql_from_json(conn, server_name, ddl));
            }

            // generate a statement to alter the server options with the schema XID
            txn.push_back(fmt::format("ALTER SERVER {} OPTIONS (SET {} '{}')",
                                      server_name, SPRINGTAIL_FDW_SCHEMA_XID_OPTION,
                                      schema_xid));

            // execute the set of statements
            _execute_ddl(conn, schema_xid, txn);

            // close the connection
            conn->disconnect();

        } catch (Error &e) {
            conn->disconnect();
            assert(0); // assert in debug
            e.log_backtrace();
            return false;
        }

        return true;
    }

    void
    PgDDLMgr::_execute_ddl(LibPqConnectionPtr conn,
                           uint64_t schema_xid,
                           const std::vector<std::string> &txn)
    {
        // get current schema xid
        std::string xid_sql = "SELECT option FROM ("
            "SELECT unnest(srvoptions) AS option "
            "FROM pg_foreign_server "
            "WHERE srvname = '"
            SPRINGTAIL_FDW_SERVER_NAME
            "' "
        ") AS options WHERE option LIKE 'schema_xid%';";

        // get the schema XID from the server options
        conn->exec(xid_sql);

        // extract the schema XID from the result
        uint64_t current_schema_xid = 0;
        std::string option = conn->get_string(0, 0);
        std::string xid_str = option.substr(option.find('=') + 1);
        current_schema_xid = std::stoull(xid_str);

        conn->clear();

        if (current_schema_xid >= schema_xid) {
            SPDLOG_WARN("Schema XID has already been applied: current={}, new={}",
                        current_schema_xid, schema_xid);
            return;
        }

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
                                 nlohmann::json &ddl)
    {
        assert(ddl.is_object());
        assert(ddl.contains("action"));

        auto &action = ddl.at("action");
        if (action == "create") {
            std::vector<std::tuple<std::string, std::string, bool>> columns;

            // retrieve the column type names
            std::set<uint32_t> type_set;
            for (auto &col : ddl.at("columns")) {
                type_set.insert(col.at("type").get<uint32_t>());
            }
            auto &&type_map = _query_type_names(conn, type_set);

            // save the column details
            for (auto &col : ddl.at("columns")) {
                columns.push_back({
                        col.at("name"),
                        type_map.at(col.at("type").get<uint32_t>()),
                        col.at("nullable")
                    });
            }

            // generate the CREATE TABLE statement
            return _gen_fdw_table_sql(conn, server_name, ddl.at("schema"), ddl.at("table"),
                                      ddl.at("tid"), columns);
        }

        if (action == "rename") {
            return fmt::format("ALTER FOREIGN TABLE {} RENAME TO {};",
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("old_table").get<std::string>()));
        }

        if (action == "drop") {
            return fmt::format("DROP FOREIGN TABLE {};",
                               conn->escape_identifier(ddl.at("table").get<std::string>()));
        }

        if (action == "col_add") {
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
            return fmt::format("ALTER FOREIGN TABLE {} ADD COLUMN {} {} {};",
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               type_map.at(type_oid),
                               constraints);
        }

        if (action == "col_drop") {
            return fmt::format("ALTER FOREIGN TABLE {} DROP COLUMN {};",
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("column").get<std::string>()));
        }

        if (action == "col_rename") {
            return fmt::format("ALTER FOREIGN TABLE {} RENAME COLUMN {} TO {};",
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("old_name").get<std::string>()),
                               conn->escape_identifier(ddl.at("new_name").get<std::string>()));
        }

        if (action == "col_nullable") {
            auto &col = ddl.at("column");
            return fmt::format("ALTER FOREIGN TABLE {} ALTER COLUMN {} {} NOT NULL;",
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               col.at("nullable").get<bool>() ? "DROP" : "SET");
        }

        // can't currently support other kinds of DDL mutations
        SPDLOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        assert(0);
    }

    std::map<uint32_t, std::string>
    PgDDLMgr::_query_type_names(LibPqConnectionPtr conn,
                                std::set<uint32_t> pg_types)
    {
        std::map<uint32_t, std::string> type_map;

        std::string &&query = fmt::format("SELECT oid, typname FROM pg_type WHERE oid IN ({})",
                                          common::join_string(",", pg_types.begin(), pg_types.end()));

        conn->exec(query);

        // extract the type names from the result
        int rows = conn->ntuples();
        for (int i = 0; i < rows; ++i) {
            uint32_t oid = conn->get_int32(i, 0);
            std::string type_name = conn->get_string(i, 1);

            // store the mapping
            type_map[oid] = type_name;
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

} // namespace springtail::pg_fdw