#pragma once

#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <set>

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <common/redis_ddl.hh>
#include <pg_repl/libpq_connection.hh>


/* These are defined by Thrift imported from xid_mgr_client.h and
 * must be undefined before including postgres.h */
#undef PACKAGE_STRING
#undef PACKAGE_VERSION
#undef UINT64CONST

namespace springtail::pg_fdw {

    /**
     * @brief DDL Mgr, applies changes from Redis queue
     * to the FDW tables
     */
    class PgDDLMgr {
    public:
        /** Get DDL Mgr instance */
        static PgDDLMgr* get_instance() {
            std::call_once(_init_flag, _init);
            return _instance;
        }

        // delete copy constructor
        PgDDLMgr(const PgDDLMgr&) = delete;
        PgDDLMgr& operator=(const PgDDLMgr&) = delete;

        // delete move constructor
        PgDDLMgr(PgDDLMgr&&) = delete;
        PgDDLMgr& operator=(PgDDLMgr&&) = delete;

        /**
         * Shutdown singleton -- initiates shutdown
         * must call wait_shutdown() to complete
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, _shutdown);
        }

        /**
         * Start the main thread
         * @param fdw_id FDW ID for this instance
         * @param username username for ddl mgr
         * @param password password for ddl mgr
         * @param hostname optional hostname for connection
         */
        void startup(const std::string &fdw_id,
                     const std::string &username,
                     const std::string &password,
                     const std::optional<std::string> &hostname = std::nullopt);

        /**
         * @brief Wait for shutdown of the singleton
         */
        void wait_shutdown();

    private:
        static PgDDLMgr* _instance;                ///< singleton instance
        static std::once_flag _init_flag;          ///< initialization flag
        static std::once_flag _shutdown_flag;      ///< shutdown flag

        std::atomic<bool> _shutting_down {false};  ///< shutdown flag, set in shutdown()
        std::thread _main_thread;                  ///< main thread

        std::string _fdw_id;                       ///< FDW ID

        std::string _hostname;                     ///< hostname
        std::string _username;                     ///< username
        std::string _password;                     ///< password
        std::string _db_prefix;                    ///< db prefix, may be empty
        std::string _fdw_username;                 ///< FDW username
        int _port;                                 ///< port

        std::map<uint64_t, std::set<std::string>> _db_schemas;  ///< map of db id to set of schemas

        std::map<uint32_t, std::string> _type_map;  ///< map of PG type OIDs to type names

        /** Private constructor */
        PgDDLMgr() {};

        /** Initialize singleton */
        static void _init();

        /** Shutdown the singleton */
        static void _shutdown();

        /** Internal instance shutdown */
        void _internal_shutdown();

        /** Initialize the FDW */
        void _init_fdw(const std::string &username, const std::string &password);

        /**
         * Main thread entry point; loops checking redis for DDL changes
         */
        void _main_thread_fn();

        /** Helper to connect to fdw db */
        LibPqConnectionPtr _connect_fdw(const std::string &db_name);

        /** Helper to connect to primary db */
        LibPqConnectionPtr _connect_primary(int db_id, const std::string &db_name);

        /** Helper to apply outstanding DDL changes to the FDW tables. Return true if applied */
        bool _update_schemas(RedisDDL &redis, const nlohmann::json &ddls,
                             const std::string &servername);


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
                          const std::vector<std::string> &sql,
                          const std::set<std::string> &schemas);

        /**
         * @brief Helper to get schemas from db config
         * @param db_id db id
         * @param db_name db name
         * @return set of schemas
         */
        std::set<std::string> _get_schemas(int db_id, const std::string &db_name);

        /**
         * @brief Helper to diff oid type set with keys from _type_map
         * @param pg_types set of PG type OIDs
         * @param mapped_types map of PG type OIDs to type names found in _type_map (output)
         * @return set of type OIDs not in _type_map, but in pg_types
         */
        std::set<uint32_t> _type_map_difference(std::set<uint32_t> &pg_types,
            std::map<uint32_t, std::string> &mapped_types);

        /**
         * @brief Helper to convert a set of PG type OIDs to type names via an external SQL query.
         * @param conn LibPqConnectionPtr connection
         * @param pg_types set of PG type OIDs
         * @return map of PG type OIDs to type names
         */
        std::map<uint32_t, std::string>
        _query_type_names(LibPqConnectionPtr conn, std::set<uint32_t> pg_types);

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
                           nlohmann::json &ddl,
                           std::set<std::string> &schemas);

        /** Helper to generate sql for FDW foreign table */
        static std::string
        _gen_fdw_table_sql(LibPqConnectionPtr conn,
                           const std::string &server_name,
                           const std::string &schema,
                           const std::string &table,
                           uint64_t tid,
                           std::vector<std::tuple<std::string, std::string, bool>> &columns);


    };
}