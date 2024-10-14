#pragma once

#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <set>

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <common/redis_ddl.hh>


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
         */
        void startup(const std::string &fdw_id, const std::optional<std::string> &hostname = std::nullopt);

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
        int _port;                                 ///< port

        /** Private constructor */
        PgDDLMgr() {};

        /** Initialize singleton */
        static void _init();

        /** Shutdown the singleton */
        static void _shutdown();

        /** Internal instance shutdown */
        void _internal_shutdown();

        /**
         * Main thread entry point; loops checking redis for DDL changes
         */
        void _main_thread_fn();

        /** Helper to apply outstanding DDL changes to the FDW tables. Return true if applied */
        bool _update_schemas(RedisDDL &redis, const nlohmann::json &ddls,
                             const std::string &servername);


        /** Helper to execute ddl statements for this db */
        void _execute_ddl(PGconn *conn,
                          uint64_t schema_xid,
                          const std::vector<std::string> &sql);


        /** Helper to generate sql from ddl json */
        static std::string
        _gen_sql_from_json(PGconn *conn,
                           const std::string &server_name,
                           nlohmann::json &ddl);

        /** Helper to convert a set of PG type OIDs to type names via an external SQL query. */
        static std::map<uint32_t, std::string>
        _query_type_names(PGconn *conn, std::set<uint32_t> pg_types);

        /** Helper to generate sql for FDW foreign table */
        static std::string
        _gen_fdw_table_sql(PGconn *conn,
                           const std::string &server_name,
                           const std::string &schema,
                           const std::string &table,
                           uint64_t tid,
                           std::vector<std::tuple<std::string, std::string, bool>> &columns);

    };
}