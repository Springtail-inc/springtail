#pragma once

#include <common/init.hh>
#include <common/multi_queue_thread_manager.hh>
#include <common/object_cache.hh>

#include <pg_repl/libpq_connection.hh>

namespace springtail::pg_fdw {
    /**
     * @brief DDL Mgr, applies changes from Redis queue
     * to the FDW tables
     */
    class PgDDLMgr final : public Singleton<PgDDLMgr>
    {
            friend class Singleton<PgDDLMgr>;
    public:
        /** Max number of connections to cache */
        static constexpr int MAX_CONNECTION_CACHE_SIZE = 10;
        /** Max number of threads in the thread manager pool */
        static constexpr int MAX_THREAD_POOL_SIZE = 4;
        /** Sync thread check interval in seconds */
        static constexpr int SYNC_INTERVAL_SECONDS = 15;


        /**
         * Start the main thread
         * @param fdw_id FDW ID for this instance
         * @param username username for ddl mgr
         * @param password password for ddl mgr
         * @param proxy_password password for roles created on fdw for proxy
         * @param hostname optional hostname for connection
         */
        void init(const std::string &fdw_id,
                  const std::string &username,
                  const std::string &password,
                  const std::string &proxy_password,
                  const std::optional<std::string> &hostname = std::nullopt);

        /**
         * @brief This function runs the main loop of DDL manager
         */
        void run();

        /**
         * @brief Generate enum alter type sql statement
         * It is public due for testing
         * @param escaped_schema schema name
         * @param escaped_name type name
         * @param old_value_json_str json array of value strings
         * @param new_value_json_str json array of new value strings
         * @param conn connection
         */
        static std::string gen_alter_enum_sql(const std::string &escaped_schema,
                                              const std::string &escaped_name,
                                              const nlohmann::json &from,
                                              const nlohmann::json &to,
                                              const LibPqConnectionPtr conn);

        static void start();

    protected:
        LruObjectCache<uint64_t, LibPqConnection> _fdw_conn_cache;  ///< FDW connections
        std::mutex _fdw_conn_cache_mutex;  ///< mutex for fdw connection cache

        RedisCache::RedisChangeWatcherPtr _cache_watcher;                   ///< redis cache callback object
        std::shared_ptr<common::MultiQueueThreadManager> _thread_manager;   ///< thread manager that processes DDL requests

        std::thread _sync_thread;                   ///< thread for syncing policies and roles
        std::condition_variable _sync_shutdown_cv;  ///< condition variable for shutdown notification
        std::mutex _sync_shutdown_mutex;            ///< mutex for shutdown notification

        std::mutex _pending_ddl_mutex;              ///< mutex for pending DDL statements

        std::string _fdw_id;                       ///< FDW ID

        std::string _hostname;                     ///< hostname
        std::string _username;                     ///< FDW username
        std::string _password;                     ///< FDW password
        std::string _proxy_password;               ///< proxy user password for new roles
        std::string _db_prefix;                    ///< db prefix, may be empty

        uint64_t _db_instance_id;                  ///< database instance id
        int _port;                                 ///< port

        std::shared_mutex _db_mutex;               ///< shared mutex for read/write access to _db_xid_map
        std::map<uint64_t, uint64_t> _db_xid_map;  ///< map of db id to max schema xid (applied)

        std::mutex _db_name_mutex;                 ///< mutex for access to _db_name_map
        std::unordered_map<uint64_t, std::string> _db_name_map;     ///< map of database id to database name

        std::map<uint32_t, std::string> _type_map;  ///< map of PG type OIDs to type names

        std::thread _pg_ddl_mgr_thread;

        /** User type map from namespace_id -> map of type oid -> <type_name, value_json> */
        using UserTypeMap = std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::pair<std::string, std::string>>>;

        /**
         * @brief Type cache
         * Stores the details about the system types
         *
         * The key is the pg_type OID and the value is a tuple of the type name and the type category
         */
        std::unordered_map<uint32_t, std::tuple<std::string, std::string>> _type_cache;

        /**
         * @brief Helper to get type name from pg_type OID
         *
         * @param pg_type pg_type OID
         * @param namespace_id namespace id
         * @param namespace_name namespace name
         * @param user_types map of user types; map: namespace id -> type_oid -> <type_name, value_json>
         * @return type name
         */
        std::string _get_type_name(int32_t pg_type,
                                   uint64_t namespace_id,
                                   const std::string &namespace_name,
                                   const UserTypeMap &user_types);

        /** Private constructor */
        PgDDLMgr();

        /** Private destructor */
        ~PgDDLMgr() override = default;

        /** Function for shutdown */
        void _internal_shutdown() override;

        /** Initialize the FDW */
        void _init_fdw();

        /** Redis callback for database ID changes */
        void _on_database_ids_changed(const std::string &path,
                                      const nlohmann::json &new_value);

        /** Sync thread; sync policy changes, roles, role memberships etc to FDW */
        void _sync_thread_func(int sync_interval_seconds);

        /** Helper to sync policies for a database */
        void _policy_sync_database(LibPqConnectionPtr conn, LibPqConnectionPtr fdw_conn, uint64_t db_id);

        /** Helper to sync roles for a database */
        void _roles_sync_database(LibPqConnectionPtr conn, LibPqConnectionPtr fdw_conn, uint64_t db_id);

        /** Helper to sync role members for a database */
        void _role_member_sync_database(LibPqConnectionPtr conn, LibPqConnectionPtr fdw_conn, uint64_t db_id);

        /** Helper to sync table ownership */
        void _table_owner_sync_database(LibPqConnectionPtr conn, LibPqConnectionPtr fdw_conn, uint64_t db_id);

        /**
         * Method to get the create schema query
         */
        std::string _get_create_schema_with_grants_query(std::string_view schema);

        /**
         * Method to get the alter schema query
         */
        std::string _get_alter_schema_with_grants_query(std::string_view old_schema, std::string_view new_schema);

        /**
         * @brief Helper to generate a create type query
         * @param escaped_schema schema name
         * @param escaped_name type name
         * @param value_json_str json array of value strings
         * @param conn connection
         * @return std::string create type query string
         */
        std::string _get_create_type_query(std::string_view escaped_schema,
                                           std::string_view escaped_name,
                                           std::string_view value_json_str,
                                           LibPqConnectionPtr conn);

        /** Helper to connect to fdw db */
        LibPqConnectionPtr _get_fdw_connection(std::optional<uint64_t> db_id, const std::string &db_name);

        /** Helper to release FDW connection back to the cache */
        void _release_fdw_connection(uint64_t db_id, LibPqConnectionPtr conn);

        /**
         * @brief Helper to apply outstanding DDL changes to the FDW tables.
         * @param db_id The database ID to apply the changes to.
         * @param schema_xid The XID at which the DDL changes were applied.
         * @param ddls A JSON array of DDL statements to apply.
         * @return Status of the operation. True if successful, false otherwise.
         */
        bool _update_schemas(uint64_t db_id,
                             const std::map<uint64_t, nlohmann::json> &xid_map);

        /** Helper to execute ddl statements for this db */
        /**
         * @brief Execute DDL statements for this db
         * @param conn connection
         * @param db_id db id
         * @param sql vector of sql statements
         */
        void _execute_ddl(LibPqConnectionPtr conn,
                          uint64_t db_id,
                          const std::vector<std::string> &sql);

        /**
         * @brief Helper to get schemas from the system tables
         * @param db_id db id
         * @param xid transaction id
         * @return map of schemas <ns_id, schema name>
         */
        std::unordered_map<uint64_t, std::string> _get_schemas(uint64_t db_id, uint64_t xid);

        /**
         * @brief Helper to get user defined types from the system tables
         * @param db_id db id
         * @param xid transaction id
         * @return map of namespace id to map of type_oid to pair <type_name, value_json>
         */
        UserTypeMap
        _get_usertypes(uint64_t db_id, uint64_t xid);

        /**
         * @brief Helper to generate sql statement from json.  Decodes the ddl json.
         * @param conn LibPqConnectionPtr connection
         * @param server_name fdw server name
         * @param ddl json object containing ddl
         * @return std::string sql statement
         */
        std::string _gen_sql_from_json(LibPqConnectionPtr conn,
                                       const std::string &server_name,
                                       const nlohmann::json &ddl);

        /**
         * @brief Function for creating a replicated database
         * @param conn - connection object
         * @param db_id - database id
         * @param db_name - database name
         */
        void _create_database(LibPqConnectionPtr conn,
                         const uint64_t db_id,
                         const std::string &db_name);

        /**
         * @brief Function for creating a replicated database schemas
         * @param db_id - database id
         * @param db_name - database name
         */
        void _create_schemas(const uint64_t db_id,
                             const std::string &db_name);

        /**
         * @brief Function for initializing RLS policies for the tables
         *        scans table names for RLS enabled tables
         * @param db_id - database id
         * @param xid - transaction id (for scanning system table)
         * @param schemas - map of schema id to schema name
         * @param conn - connection object
         */
        void _init_rls(uint64_t db_id,
                       uint64_t xid,
                       const std::unordered_map<uint64_t, std::string> &schemas,
                       LibPqConnectionPtr conn);

        /**
         * @brief Function for adding a comment to the top and intermediary
         *      partition tables
         *
         * @param conn - connection object
         * @param db_id - database id
         * @param schema_name - schema name
         * @param xid - transaction id
         */
        void
        _add_partition_table_comment(LibPqConnectionPtr conn,
                                     const uint64_t db_id,
                                     const std::string &schema_name,
                                     const uint64_t xid);

        /**
         * @brief Function for adding a new replicated database
         * @param db_id - database id
         */
        void _add_replicated_database(uint64_t db_id,
                                      const std::optional<std::string> &db_name_opt = std::nullopt,
                                      bool check_exists = false);


        /**
         * @brief Function for removing an existing replicated database
         * @param db_id - databese id
         */
        void _remove_replicated_database(uint64_t db_id);


        /**
         * @brief Function for executing a SQL command within a savepoint
         * @param fdw_conn connection to the FDW database
         * @param primary_conn connection to the primary database
         * @param savepoint_name name of the savepoint to use
         * @param sql SQL command to execute
         * @param rollback_sql SQL command to rollback in case of failure
         * @return true if successful
         * @return false if failed; rollback_sql is executed and savepoint is rolled back
         */
        bool _execute_in_savepoint(LibPqConnectionPtr fdw_conn,
                                LibPqConnectionPtr primary_conn,
                                const std::string &savepoint_name,
                                const std::string &sql,
                                const std::string &rollback_sql);

        /**
         * @brief Check if a table exists in the FDW database
         * @param fdw_conn connection to the FDW database
         * @param schema_name name of the schema
         * @param table_name name of the table
         * @param table_oid OID of the table
         * @return true if the table exists, false otherwise
         */
        bool _check_table_exists(LibPqConnectionPtr fdw_conn,
                                 const std::string &schema_name,
                                 const std::string &table_name,
                                 uint32_t table_oid);
    };

} // springtail::pg_fdw
