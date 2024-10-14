#include <nlohmann/json.hpp>
#include <libpq-fe.h>

#include <common/redis.hh>
#include <common/redis_ddl.hh>
#include <common/common.hh>
#include <common/logging.hh>
#include <common/properties.hh>

#include <pg_fdw/exception.hh>
#include <pg_fdw/pg_ddl_mgr.hh>
#include <pg_fdw/pg_fdw_common.h>

namespace springtail::pg_fdw {

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
        if (!_db_prefix.empty()) {
            // if the FDW is using a prefix, prepend it
            db_name = _db_prefix + db_name;
        }

        // use libpq to connect to the database
        PGconn *conn = PQconnectdb(fmt::format("host={} dbname={} user={} password={} port={}",
                                               _hostname, db_name, _username, _password, _port).c_str());

        if (PQstatus(conn) != CONNECTION_OK) {
            SPDLOG_ERROR("Connection to database failed: {}", PQerrorMessage(conn));
            PQfinish(conn);
            return false;
        }

        try {
            // generate a DDL statement for each JSON in the transaction
            std::vector<std::string> txn;
            txn.push_back("BEGIN");

            for (auto ddl : ddls.at("ddls")) {
                txn.push_back(_gen_sql_from_json(conn, server_name, ddl));
            }

            // generate a statement to alter the server options with the schema XID
            txn.push_back(fmt::format("ALTER SERVER {} OPTIONS (SET {} '{}')",
                                      server_name, SPRINGTAIL_FDW_SCHEMA_XID_OPTION,
                                      schema_xid));

            txn.push_back("COMMIT");

            // execute the set of statements
            _execute_ddl(conn, schema_xid, txn);

            // close the connection
            PQfinish(conn);

        } catch (Error &e) {
            PQexec(conn, "ROLLBACK");
            PQfinish(conn);
            assert(0); // assert in debug
            e.log_backtrace();
            return false;
        }

        return true;
    }

    void
    PgDDLMgr::_execute_ddl(PGconn *conn,
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
        PGresult *res = PQexec(conn, xid_sql.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
            SPDLOG_ERROR("SELECT command failed: {} {}", xid_sql, PQerrorMessage(conn));
            PQclear(res);
            throw FdwError();
        }

        // extract the schema XID from the result
        uint64_t current_schema_xid = 0;
        std::string option = PQgetvalue(res, 0, 0);
        std::string xid_str = option.substr(option.find('=') + 1);
        current_schema_xid = std::stoull(xid_str);

        PQclear(res);

        if (current_schema_xid >= schema_xid) {
            SPDLOG_WARN("Schema XID has already been applied: current={}, new={}",
                        current_schema_xid, schema_xid);
            return;
        }

        // exectute each DDL statement
        for (const auto &sql : txn) {
            SPDLOG_DEBUG_MODULE(LOG_FDW, "Executing DDL: {}", sql);
            res = PQexec(conn, sql.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                SPDLOG_ERROR("DDL command failed: {} error: {}", sql, PQerrorMessage(conn));
                PQclear(res);
                throw FdwError();
            }
            PQclear(res);
        }
    }

    std::string
    PgDDLMgr::_gen_sql_from_json(PGconn *conn,
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
                               ddl.at("table").get<std::string>(),
                               ddl.at("old_table").get<std::string>());
        }

        if (action == "drop") {
            return fmt::format("DROP FOREIGN TABLE {};",
                               ddl.at("table").get<std::string>());
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
                               ddl.at("table").get<std::string>(),
                               col.at("name").get<std::string>(),
                               type_map.at(type_oid),
                               constraints);
        }

        if (action == "col_drop") {
            return fmt::format("ALTER FOREIGN TABLE {} DROP COLUMN {};",
                               ddl.at("table").get<std::string>(),
                               ddl.at("column").get<std::string>());
        }

        if (action == "col_rename") {
            return fmt::format("ALTER FOREIGN TABLE {} RENAME COLUMN {} TO {};",
                               ddl.at("table").get<std::string>(),
                               ddl.at("old_name").get<std::string>(),
                               ddl.at("new_name").get<std::string>());
        }

        if (action == "col_nullable") {
            auto &col = ddl.at("column");
            return fmt::format("ALTER FOREIGN TABLE {} ALTER COLUMN {} {} NOT NULL;",
                               ddl.at("table").get<std::string>(),
                               col.at("name").get<std::string>(),
                               col.at("nullable").get<bool>() ? "DROP" : "SET");
        }

        // can't currently support other kinds of DDL mutations
        SPDLOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        assert(0);
    }

    std::map<uint32_t, std::string>
    PgDDLMgr::_query_type_names(PGconn *conn,
                                std::set<uint32_t> pg_types)
    {
        std::map<uint32_t, std::string> type_map;

        std::string &&query = fmt::format("SELECT oid, typname FROM pg_type WHERE oid IN ({})",
                                          common::join_string(",", pg_types.begin(), pg_types.end()));
        PGresult *res = PQexec(conn, query.c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
            SPDLOG_ERROR("SELECT command failed: {}", PQerrorMessage(conn));
            PQclear(res);
            throw FdwError();
        }

        // extract the type names from the result
        int rows = PQntuples(res);
        for (int i = 0; i < rows; ++i) {
            char *value = PQgetvalue(res, i, 0);
            if (value == nullptr) {
                SPDLOG_ERROR("Retrieving OID {}: {}", i, PQerrorMessage(conn));
                PQclear(res);
                throw FdwError();
            }
            uint32_t oid = std::stoi(value, nullptr, 10);

            value = PQgetvalue(res, i, 1);
            if (value == nullptr) {
                SPDLOG_ERROR("Retrieving type name {}: {}", i, PQerrorMessage(conn));
                PQclear(res);
                throw FdwError();
            }
            std::string type_name(value);

            // store the mapping
            type_map[oid] = type_name;
        }
        PQclear(res);

        return type_map;
    }

    std::string
    PgDDLMgr::_gen_fdw_table_sql(PGconn *conn,
                                 const std::string &server_name,
                                 const std::string &schema,
                                 const std::string &table,
                                 uint64_t tid,
                                 std::vector<std::tuple<std::string, std::string, bool>> &columns)
    {
        char *schema_name = PQescapeIdentifier(conn, schema.c_str(), schema.size());
        char *table_name = PQescapeIdentifier(conn, table.c_str(), table.size());

        // no schema name needed
        std::string create = fmt::format("CREATE FOREIGN TABLE {}.{} (\n",
                                         schema_name, table_name);

        PQfreemem(schema_name);
        PQfreemem(table_name);

        // iterate over the columns, adding each to the create statement
        // name, type, is_nullable, default value
        for (int i = 0; i < columns.size(); i++) {
            const auto &[column_name, type_name, nullable] = columns[i];
            char *column_name_escaped = PQescapeIdentifier(conn, column_name.c_str(), column_name.size());

            std::string column = fmt::format("  {} ", column_name_escaped);

            PQfreemem(column_name_escaped);

            // set the type name
            column += type_name;

            // add nullability and default
            if (!nullable) {
                column += " NOT NULL";
            }

            if (i < columns.size() - 1) {
                column += ",\n";
            }

            create += column;
        }

        create += fmt::format("\n) SERVER {} OPTIONS (tid '{}');", server_name.c_str(), tid);

        SPDLOG_DEBUG_MODULE(LOG_FDW, "Generated SQL: {}", create);

        return create;
    }

} // namespace springtail::pg_fdw