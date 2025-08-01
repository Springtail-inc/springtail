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
#include <pg_fdw/pg_xid_collector.hh>

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

    static constexpr char SYSTEM_TYPES[] =
        "SELECT oid, typname, typcategory "
        "FROM pg_type "
        "WHERE typisdefined = true";

    PgDDLMgr::PgDDLMgr() :
        Singleton<PgDDLMgr>(ServiceId::PgDDLMgrId),
        _fdw_conn_cache(MAX_CONNECTION_CACHE_SIZE)
    {
        _cache_watcher = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
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
        );
        PgXidCollector::get_instance()->start_thread();
    }

    void
    PgDDLMgr::_internal_shutdown()
    {
        _pg_ddl_mgr_thread.join();
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);
        PgXidCollector::shutdown();
    }

    void
    PgDDLMgr::start()
    {
        std::string username = springtail_retreive_argument<std::string>(ServiceId::PgDDLMgrId, "username").value();
        std::string password = springtail_retreive_argument<std::string>(ServiceId::PgDDLMgrId, "password").value();
        std::optional<std::string> hostname = springtail_retreive_argument<std::optional<std::string>>(ServiceId::PgDDLMgrId, "hostname").value();
        // start the ddl main thread
        std::string fdw_id = Properties::get_fdw_id();

        LOG_DEBUG(LOG_FDW, "Starting DDL Mgr with fdw_id: {}, username: {}, password: {}, socket_hostname: {}",
                    fdw_id, username, password, hostname.value_or(""));
        PgDDLMgr::get_instance()->init(fdw_id, username, password, hostname);
        PgDDLMgr::get_instance()->_pg_ddl_mgr_thread = std::thread(&PgDDLMgr::run, PgDDLMgr::get_instance());
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
        std::string fdw_username, fdw_password;
        Json::get_to<std::string>(fdw_config, "fdw_user", fdw_username);
        Json::get_to<std::string>(fdw_config, "password", fdw_password);

        if (fdw_config.contains("db_prefix")) {
            // if the FDW is using a prefix, prepend it
            _db_prefix = fdw_config.at("db_prefix").get<std::string>();
        }

        LOG_DEBUG(LOG_FDW, "FDW ID: {}, Host: {}, Port: {}, Username: {}, FDW Username: {}",
                     _fdw_id, _hostname, _port, _username, fdw_username);

        // add subscribers to pubsub threads
        _db_instance_id = Properties::get_db_instance_id();

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher);

        _init_fdw(username, password);

        _thread_manager = std::make_shared<common::MultiQueueThreadManager>(MAX_THREAD_POOL_SIZE);
        _thread_manager->start();

        LOG_INFO("PgDDLMgr::init() done");
    }

    std::map<uint64_t, std::map<uint64_t, std::pair<std::string, std::string>>>
    PgDDLMgr::_get_usertypes(uint64_t db_id, uint64_t xid)
    {
        // namespace id -> type_id -> <type_name, value_json>
        std::map<uint64_t, std::map<uint64_t, std::pair<std::string, std::string>>> usertype_map;

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
            DCHECK(fields->at(sys_tbl::UserTypes::Data::TYPE)->get_uint8(&row) == constant::USER_TYPE_ENUM);

            // insert into map by namespace_id
            LOG_DEBUG(LOG_FDW, "Adding user type: {}.{} = {}:{}", namespace_id, type_id, type_name, value_json);
            usertype_map[namespace_id][type_id] = std::make_pair(type_name, value_json);
        }

        return usertype_map;
    }

    std::map<uint64_t, std::string>
    PgDDLMgr::_get_schemas(uint64_t db_id, uint64_t xid)
    {
        // get the db config and parse out the included schemas
        auto db_config = Properties::get_db_config(db_id);

        bool all_schemas = false;
        std::set<std::string> schemas;
        std::map<uint64_t, std::string> schema_map;

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
    PgDDLMgr::_init_fdw(const std::string &username, const std::string &password)
    {
        // get map of dbs id:name from redis
        auto dbs = Properties::get_databases();

        // connect to the db to setup the replicated dbs, the fdw user and foreign servers
        LibPqConnectionPtr conn = _connect_fdw(std::nullopt, "template1");

        // see if the fdw user exists, if not create it
        _fdw_username = username;
        conn->exec(fmt::format("SELECT 1 FROM pg_roles WHERE rolname = '{}'", username));
        if (conn->ntuples() == 0) {
            // create the user
            conn->clear();
            conn->exec(fmt::format(CREATE_FDW_USER, username, password));
            conn->clear();
        }

        // Populate system defined types
        conn->exec(SYSTEM_TYPES);
        int rows = conn->ntuples();
        for (int i = 0; i < rows; i++) {
            uint64_t oid = conn->get_int32(i, 0);
            std::string name = conn->escape_identifier(conn->get_string(i, 1));
            std::string category = conn->escape_identifier(conn->get_string(i, 2));
            _type_cache[oid] = std::make_tuple(name, category);
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
        LOG_INFO("PgDDLMgr::_init_fdw() done");
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

        while (!_is_shutting_down()) {
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
                LOG_DEBUG(LOG_FDW, "Reusing connection for db_id: {}", db_id);
                return conn;
            }
            _fdw_conn_cache.evict(db_id);
        }

        LOG_DEBUG(LOG_FDW, "Establishing connection for db_id: {}", db_id);

        // use libpq to connect to the database
        std::vector<std::pair<std::string, std::string>> options = {{"springtail_fdw.ddl_connection", "on"}};
        conn = std::make_shared<LibPqConnection>();
        conn->connect(_hostname, db_name, _username, _password, _port, false, options);

        // save the connection in the cache
        _fdw_conn_cache.insert(db_id, conn);

        return conn;
    }

    bool
    PgDDLMgr::_update_schemas(uint64_t db_id,
                              const std::map<uint64_t, nlohmann::json> &xid_map)
    {
        // get the database name for the db_id; XXX should see if we can swtich to OID
        std::string db_name = Properties::get_db_name(db_id);
        LibPqConnectionPtr conn = _connect_fdw(db_id, _db_prefix + db_name);

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

            return true;

        } catch (Error &e) {
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

        LOG_DEBUG(LOG_FDW, "DDL JSON: {}", ddl.dump(4));

        PartitionInfo partition_info = _get_partition_info(ddl);

        bool is_regular_table = partition_info.parent_table_id() == 0 && partition_info.partition_key().empty();
        bool is_leaf_partitioned_table = partition_info.parent_table_id() > 0 && !partition_info.partition_bound().empty() && partition_info.partition_key().empty();

        bool is_regular_table_type = is_regular_table || is_leaf_partitioned_table;

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
                                                   partition_info, is_regular_table_type,
                                                   [conn](const std::string &name) {
                                                        return conn->escape_identifier(name.c_str());
                                                   });
        }

        else if (action == "rename") { // rename table
            std::string rename = fmt::format("ALTER {} TABLE {}.{} RENAME TO {};",
                                             is_regular_table_type ? "FOREIGN" : "",
                                             conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                             conn->escape_identifier(ddl.at("old_table").get<std::string>()),
                                             conn->escape_identifier(ddl.at("table").get<std::string>()));

            // XXX it's not clear to me that we need to support a schema change here?
            if (ddl.at("schema").get<std::string>() != ddl.at("old_schema").get<std::string>()) {
                return rename + fmt::format("ALTER {} TABLE {}.{} SET SCHEMA {};",
                                            is_regular_table_type ? "FOREIGN" : "",
                                            conn->escape_identifier(ddl.at("old_schema").get<std::string>()),
                                            conn->escape_identifier(ddl.at("table").get<std::string>()),
                                            conn->escape_identifier(ddl.at("schema").get<std::string>()));
            } else {
                return rename;
            }
        }

        else if (action == "drop") {  // drop table
            return fmt::format("DROP {} TABLE IF EXISTS {}.{};",
                               is_regular_table_type ? "FOREIGN" : "",
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
                               is_regular_table_type ? "FOREIGN" : "",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(col.at("name").get<std::string>()),
                               type_name,
                               constraints);
        }

        else if (action == "col_drop") {  // alter table drop column
            return fmt::format("ALTER {} TABLE {}.{} DROP COLUMN {};",
                               is_regular_table_type ? "FOREIGN" : "",
                               conn->escape_identifier(ddl.at("schema").get<std::string>()),
                               conn->escape_identifier(ddl.at("table").get<std::string>()),
                               conn->escape_identifier(ddl.at("column").get<std::string>()));
        }
#endif /* ENABLE_SCHEMA_MUTATES */

        else if (action == "col_rename") {
            return fmt::format("ALTER {} TABLE {}.{} RENAME COLUMN {} TO {};",
                               is_regular_table_type ? "FOREIGN" : "",
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

            return fmt::format("ALTER {} TABLE {}.{} SET SCHEMA {};",
                               is_regular_table_type ? "FOREIGN" : "",
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
        }

        // can't currently support other kinds of DDL mutations
        LOG_ERROR("Bad DDL statement: {}", action.get<std::string>());
        CHECK(false);
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

    std::string
    PgDDLMgr::_get_type_name(int32_t pg_type,
                             uint64_t namespace_id,
                             const std::string &namespace_name,
                             const std::map<uint64_t, std::map<uint64_t,
                             std::pair<std::string, std::string>>> &user_types)
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
    PgDDLMgr::_create_schemas(LibPqConnectionPtr conn,
                    const uint64_t db_id,
                    const std::string &db_name)
    {

        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
        RedisDDL redis_ddl;

        uint64_t xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);

        // get schemas, parse include, fetch from primary db if necessary
        auto &&schemas = _get_schemas(db_id, xid);

        // get user types from system tables
        auto &&user_types = _get_usertypes(db_id, xid);

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
        auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}});
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
        LibPqConnectionPtr conn = _connect_fdw(std::nullopt, "postgres");
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
