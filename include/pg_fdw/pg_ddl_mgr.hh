#pragma once

#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <set>
#include <optional>

#include <nlohmann/json.hpp>

#include <common/properties.hh>
#include <common/object_cache.hh>
#include <common/singleton.hh>

#include <redis/redis_ddl.hh>
#include <redis/pubsub_thread.hh>

#include <pg_repl/libpq_connection.hh>


/* These are defined by Thrift imported from xid_mgr_client.h and
 * must be undefined before including postgres.h */
#undef PACKAGE_STRING
#undef PACKAGE_VERSION

namespace springtail::pg_fdw {

    /**
     * @brief DDL Mgr, applies changes from Redis queue
     * to the FDW tables
     */
    class PgDDLMgr final : public SingletonWithThread<PgDDLMgr> {
        friend class SingletonWithThread<PgDDLMgr>;
    public:
        /** Max number of connections to cache */
        static constexpr int MAX_CONNECTION_CACHE_SIZE = 10;

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

    private:
        LruObjectCache<uint64_t, LibPqConnection> _fdw_conn_cache;  ///< FDW connections
        PubSubThread _config_sub_thread;           ///< pubsub thread for redis config database

        std::string _fdw_id;                       ///< FDW ID

        std::string _hostname;                     ///< hostname
        std::string _username;                     ///< username
        std::string _password;                     ///< password
        std::string _db_prefix;                    ///< db prefix, may be empty
        std::string _fdw_username;                 ///< FDW username
        uint64_t _db_instance_id;                  ///< database instance id
        int _port;                                 ///< port

        std::shared_mutex _db_mutex;               ///< shared mutex for read/write access to _db_xid_map and _db_schemas maps
        std::map<uint64_t, uint64_t> _db_xid_map;  ///< map of db id to max schema xid (applied)

        std::map<uint64_t, std::set<std::string>> _db_schemas;  ///< map of db id to set of schemas

        std::map<uint32_t, std::string> _type_map;  ///< map of PG type OIDs to type names

        /** Private constructor */
        PgDDLMgr() : _fdw_conn_cache(MAX_CONNECTION_CACHE_SIZE), _config_sub_thread(1, true) {};
        ~PgDDLMgr() {
            _config_sub_thread.shutdown();
        }

        /** Initialize the FDW */
        void _init_fdw(const std::string &username, const std::string &password);

        /**
         * Main thread entry point; loops checking redis for DDL changes
         */
        void _internal_run() override;

        /** Helper to connect to fdw db */
        LibPqConnectionPtr _connect_fdw(std::optional<uint64_t> db_id, const std::string &db_name);

        /** Helper to connect to primary db */
        LibPqConnectionPtr _connect_primary(uint64_t db_id, const std::string &db_name);

        /**
         * @brief Helper to apply outstanding DDL changes to the FDW tables.
         * @param redis RedisDDL instance
         * @param db_id The database ID to apply the changes to.
         * @param schema_xid The XID at which the DDL changes were applied.
         * @param ddls A JSON array of DDL statements to apply.
         * @return Status of the operation. True if successful, false otherwise.
         */
        bool _update_schemas(RedisDDL &redis,
                             uint64_t db_id,
                             uint64_t schema_xid,
                             const nlohmann::json &ddls);


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
        std::set<std::string> _get_schemas(uint64_t db_id, const std::string &db_name);

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
                           const nlohmann::json &ddl,
                           std::set<std::string> &schemas);

        /** Helper to generate sql for FDW foreign table */
        static std::string
        _gen_fdw_table_sql(LibPqConnectionPtr conn,
                           const std::string &server_name,
                           const std::string &schema,
                           const std::string &table,
                           uint64_t tid,
                           std::vector<std::tuple<std::string, std::string, bool>> &columns);

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

        /**
         * @brief Function for handling database change notifications from redis
         *
         * @param msg - message
         */
        void
        _handle_replicated_dbs_change(const std::string &msg);

    };
}