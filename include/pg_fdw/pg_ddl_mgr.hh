#pragma once

#include <vector>
#include <set>
#include <optional>

#include <nlohmann/json.hpp>

#include <common/properties.hh>
#include <common/object_cache.hh>
#include <common/singleton.hh>
#include <common/multi_queue_thread_manager.hh>

#include <redis/redis_ddl.hh>

#include <sys_tbl_mgr/system_tables.hh>
#include <sys_tbl_mgr/table_mgr.hh>

#include <pg_repl/libpq_connection.hh>
#include "common/logging.hh"

namespace springtail::pg_fdw {

    /**
     * @brief DDL Mgr, applies changes from Redis queue
     * to the FDW tables
     */
    class PgDDLMgr final : public Singleton<PgDDLMgr> {
            friend class Singleton<PgDDLMgr>;
    public:
        /** Max number of connections to cache */
        static constexpr int MAX_CONNECTION_CACHE_SIZE = 10;
        /** Max number of threads in the thread manager pool */
        static constexpr int MAX_THREAD_POOL_SIZE = 4;
        /** Policy sync interval in seconds */
        static constexpr int POLICY_SYNC_INTERVAL_SECONDS = 30;

        /**
         * Start the main thread
         * @param fdw_id FDW ID for this instance
         * @param username username for ddl mgr
         * @param password password for ddl mgr
         * @param hostname optional hostname for connection
         */
        void init(const std::string &fdw_id,
                  const std::string &username,
                  const std::string &password,
                  const std::optional<std::string> &hostname = std::nullopt);

        /**
         * @brief This function runs the main loop of DDL manager
         */
        void run();

        /**
         * @brief This function notifies DDL manager to exit the main loop
         */
        void notify_shutdown() {
            _is_shutting_down = true;
            _policy_shutdown_cv.notify_all();
        }

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
    private:
        LruObjectCache<uint64_t, LibPqConnection> _fdw_conn_cache;  ///< FDW connections
        RedisCache::RedisChangeWatcherPtr _cache_watcher;           ///< redis cache callback object
        std::shared_ptr<common::MultiQueueThreadManager> _thread_manager;   ///< thread manager that processes DDL requests

        std::thread _policy_sync_thread;              ///< thread for syncing policies
        std::condition_variable _policy_shutdown_cv;  ///< condition variable for shutdown notification
        std::mutex _policy_shutdown_mutex;            ///< mutex for shutdown notification

        std::string _fdw_id;                       ///< FDW ID

        std::string _hostname;                     ///< hostname
        std::string _username;                     ///< username
        std::string _password;                     ///< password
        std::string _db_prefix;                    ///< db prefix, may be empty
        std::string _fdw_username;                 ///< FDW username
        uint64_t _db_instance_id;                  ///< database instance id
        int _port;                                 ///< port

        std::shared_mutex _db_mutex;               ///< shared mutex for read/write access to _db_xid_map
        std::map<uint64_t, uint64_t> _db_xid_map;  ///< map of db id to max schema xid (applied)

        std::map<uint32_t, std::string> _type_map;  ///< map of PG type OIDs to type names
        std::atomic<bool> _is_shutting_down{false}; ///< shutting down flag

        /** Private constructor */
        PgDDLMgr();
        /** Private destructor */
        ~PgDDLMgr() override = default;

        /** Function for shutdown */
        void _internal_shutdown() override;

        /** Initialize the FDW */
        void _init_fdw(const std::string &username, const std::string &password);

        /** Redis callback for database ID changes */
        void _on_database_ids_changed(const std::string &path,
                                      const nlohmann::json &new_value);

        /** Policy sync thread; sync policy changes to FDW */
        void _policy_sync_thread();

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
        LibPqConnectionPtr _connect_fdw(std::optional<uint64_t> db_id, const std::string &db_name);

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
         * @param schema_xid schema xid associated with ddl changes
         * @param sql vector of sql statements
         * @param schemas set of schemas to for create table (create if not exist)
         */
        void _execute_ddl(LibPqConnectionPtr conn,
                          uint64_t db_id,
                          uint64_t schema_xid,
                          const std::vector<std::string> &sql);

        /**
         * @brief Helper to get schemas from the system tables
         * @param db_id db id
         * @param xid transaction id
         * @return map of schemas <ns_id, schema name>
         */
        std::map<uint64_t, std::string> _get_schemas(uint64_t db_id, uint64_t xid);

        /**
         * @brief Helper to get user defined types from the system tables
         * @param db_id db id
         * @param xid transaction id
         * @return map of namespace id to map of type_id to pair <type_name, value_json>
         */
        std::map<uint64_t, std::map<uint64_t, std::pair<std::string, std::string>>>
        _get_usertypes(uint64_t db_id, uint64_t xid);

        /**
         * @brief Helper to generate sql statement from json.  Decodes the ddl json.
         * @param conn LibPqConnectionPtr connection
         * @param server_name fdw server name
         * @param ddl json object containing ddl
         * @param schemas set of schemas to update (output)
         * @return std::string sql statement
         */
        std::string
        _gen_sql_from_json(LibPqConnectionPtr conn,
                           const std::string &server_name,
                           const nlohmann::json &ddl);

        /** Helper to generate sql for FDW foreign table */
        std::string
        _gen_fdw_table_sql(LibPqConnectionPtr conn,
                           const std::string &server_name,
                           const std::string &schema,
                           const std::string &table,
                           uint64_t tid,
                           const nlohmann::json &columns);

        /**
         * @brief Function for creating a replicated database
         *
         * @param conn - connection object
         * @param db_id - database id
         * @param db_name - database name
         */
        void
        _create_database(LibPqConnectionPtr conn,
                         const uint64_t db_id,
                         const std::string &db_name);

        /**
         * @brief Function for creating a replicated database schemas
         *
         * @param conn - connection object
         * @param db_id - database id
         * @param db_name - database name
         */
        void
        _create_schemas(LibPqConnectionPtr conn,
                        const uint64_t db_id,
                        const std::string &db_name);

        /**
         * @brief Function for adding a new replicated database
         *
         * @param db_id - databese id
         */
        void
        _add_replicated_database(uint64_t db_id);

        /**
         * @brief Function for removing an existing replicated database
         *
         * @param db_id - databese id
         */
        void
        _remove_replicated_database(uint64_t db_id);

    };

    class PgDDLMgrRunner : public ServiceRunner {
    public:
        PgDDLMgrRunner(const std::string &username,
                       const std::string &password,
                       const std::optional<std::string> &hostname) :
            ServiceRunner("PgDDLMgr"),
            _username(username),
            _password(password),
            _hostname(hostname) {}

        bool start() override
        {
            // start the ddl main thread
            std::string fdw_id = Properties::get_fdw_id();

            LOG_DEBUG(LOG_FDW, "Starting DDL Mgr with fdw_id: {}, username: {}, password: {}, socket_hostname: {}",
                        fdw_id, _username, _password, _hostname.value_or(""));
            PgDDLMgr::get_instance()->init(fdw_id, _username, _password, _hostname);
            _pg_ddl_mgr_thread = std::thread(&PgDDLMgr::run, PgDDLMgr::get_instance());
            return true;
        }

        void stop() override
        {
            PgDDLMgr::get_instance()->notify_shutdown();
            _pg_ddl_mgr_thread.join();
            PgDDLMgr::shutdown();
        }

    private:
        std::thread _pg_ddl_mgr_thread;
        std::string _username;                     ///< username
        std::string _password;                     ///< password
        std::optional<std::string> _hostname;      ///< hostname
    };
}
