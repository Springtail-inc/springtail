#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <map>
#include <utility>
#include <list>
#include <climits>
#include <unordered_map>
#include <fmt/core.h>

#include <common/logging.hh>
#include <common/service_register.hh>
#include <common/singleton.hh>

#include <redis/db_state_change.hh>
#include <redis/redis_db_tables.hh>
#include <redis/pubsub_thread.hh>

#include <proxy/connection.hh>
#include <proxy/server_session.hh>

namespace springtail::pg_proxy {

    /**
     * @brief Implements a simple pool of database sessions
     */
    class DatabasePool {
    public:
        DatabasePool() = default;

        /**
         * @brief Add session to pool; marked as free
         * @param session  session to add
         */
        void add_session(ServerSessionPtr session);

        /**
         * @brief Get a session from the pool removing that session
         * @param db_id database id
         * @param username username
         * @returns session  session from pool or nullptr if pool is empty
         */
        ServerSessionPtr get_session(uint64_t db_id,
                                     const std::string &username);

        /**
         * @brief Evict all sessions for a given db_id
         * @param db_id database id
         */
        void evict(uint64_t db_id);

        /**
         * @brief Evict a session from the pool
         * @param session ServerSessionPtr session
         */
        void evict(ServerSessionPtr session);

        /**
         * @brief Evict a session from the pool based on the instance
         * @param instance DatabaseInstancePtr instance
         */
        void evict(DatabaseInstancePtr instance);

        /**
         * @brief Get the size of the pool
         * @returns size of the pool
         */
        int size() const {
            std::shared_lock lock(_mutex);
            return count;
        }

        void dump() const {
            std::shared_lock lock(_mutex);
            LOG_DEBUG(LOG_PROXY, "DatabasePool size: {}", count);
            for ([[maybe_unused]] const auto &it : _free_sessions) {
                LOG_DEBUG(LOG_PROXY, "DatabasePool db_id: {} username: {} size: {}", it.first.first, it.first.second, it.second.size());
            }
        }

    private:
        mutable std::shared_mutex _mutex;   ///< mutex for pool
        int count = 0;                      ///< count of sessions in pool

        /** pair<db_id, username> to list of sessions */
        std::map<std::pair<uint64_t, std::string>, std::list<ServerSessionWeakPtr>> _free_sessions;

    };
    using DatabasePoolPtr = std::shared_ptr<DatabasePool>;

    /**
     * @brief Database instance class, contains hostname, port etc.
     */
    class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
    public:
        DatabaseInstance(const Session::Type type,
                         const std::string &hostname,
                         std::string db_prefix="",
                         int port=5432)
            : _type(type), _hostname(hostname),
              _replica_db_prefix(db_prefix),
              _port(port)
        {}

        /**
         * @brief Get hostname
         * @return std::string hostname
         */
        std::string hostname() const { return _hostname; }

        /**
         * @brief Get port
         * @return int port
         */
        int port() const { return _port; }

        /**
         * @brief Get prefix
         * @return std::string prefix
         */
        std::string prefix() const { return _replica_db_prefix; }

        /**
         * @brief Create connection to this instance
         * @return ProxyConnectionPtr
         */
        ProxyConnectionPtr create_connection() {
            return ProxyConnection::create(_hostname, _port);
        }

        /**
         * @brief Dump the instance to string
         */
        std::string to_string() {
            return fmt::format("type={}, hostname={}, port={}", (_type == Session::Type::PRIMARY ? "PRIMARY" : "REPLICA"), _hostname, _port);
        }

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * Virtual to allow override in testing.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters);

    private:
        mutable std::shared_mutex _mutex;    ///< mutex for instance
        Session::Type _type;                 ///< type of instance (primary, replica)
        std::string _hostname;               ///< hostname of instance
        std::string _replica_db_prefix;      ///< prefix to be used for replica database
        int _port;                           ///< port of instance
    };
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;


    /**
     * @brief Interface for primary set and replica set
     */
    class DatabaseSet {
    public:

        explicit DatabaseSet(int max_sessions_per_instance) :
            _max_sessions_per_instance(max_sessions_per_instance),
            _pool(std::make_shared<DatabasePool>())
        {}

        virtual ~DatabaseSet() = default;

        /**
         * @brief Get a free session from the session pool if possible
         * @param db_id database id
         * @param username username
         * @return ServerSessionPtr or nullptr if no sessions available
         */
        ServerSessionPtr get_session(const uint64_t db_id,
                                     const std::string &username) {
            std::shared_lock lock(_mutex);
            return _pool->get_session(db_id, username);
        }

        /**
         * @brief Remove instance from the replica set
         * @param instance database instance
         */
        void remove_instance(DatabaseInstancePtr instance);

        /**
         * @brief Remove database from the replica set
         * @param db_id database id
         */
        void remove_database(uint64_t db_id);

        /**
         * @brief Release session back to pool if space, or deallocate session
         * @param session session to release
         */
        virtual void release_session(ServerSessionPtr session, bool deallocate) = 0;

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters) = 0;

        /**
         * @brief Get size of the free session pool
         * @return int size of pool
         */
        int pool_size() const {
            return _pool->size();
        }

    protected:

        /**
         * @brief Release session back to pool if space, or deallocate session
         * @param session session to release
         * @param deallocate deallocate session
         */
        void _release_session(ServerSessionPtr session, int num_instances, bool deallocate=false);

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param instance DatabaseInstancePtr instance
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr _allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            DatabaseInstancePtr instance);

        /**
         * @brief Get least loaded instance from the _instances_sessions map
         * @return DatabaseInstancePtr or nullptr if no instances
         */
        DatabaseInstancePtr _get_least_loaded_instance();

        /**
         * @brief For testing, retrieve the instance sessions map
         * @return std::map<DatabaseInstancePtr, int>
         */
        std::map<DatabaseInstancePtr, int> _get_instance_sessions() const {
            std::shared_lock lock(_mutex);
            return _instance_sessions;
        }

    private:
        mutable std::shared_mutex _mutex;  ///< mutex for maps

        /** max sessions per instance, assuming roughly distributed evenly */
        int _max_sessions_per_instance;

        /** map of database instances to session ids */
        std::map<DatabaseInstancePtr, std::map<uint64_t, std::list<ServerSessionWeakPtr>>> _sessions;

        /* map of instance to number of sessions */
        std::map<DatabaseInstancePtr, int> _instance_sessions;

        DatabasePoolPtr _pool;  ///< Database pool across all instances
    };
    using DatabaseSetPtr = std::shared_ptr<DatabaseSet>;

    /**
     * Class representing a database replica set, contains a list of
     * database instances for the replica set.
     */
    class DatabaseReplicaSet : public DatabaseSet {
    public:
        explicit DatabaseReplicaSet(int max_sessions_per_instance) :
            DatabaseSet(max_sessions_per_instance)
        {}

        /**
         * @brief Add a replica to the replica set
         * @param replica replica to add
         */
        void add_replica(DatabaseInstancePtr replica)
        {
            std::unique_lock lock(_mutex);
            _replicas.insert(replica);
        }

        /**
         * @brief Remove a replica database instance, and removes
         * all sessions associated with the instance from the pool.
         * @param replica database instance
         */
        void remove_replica(DatabaseInstancePtr replica);

        /**
         * @brief Release session back to the db instance (and associated pool).
         * If the pool is full, the session is deallocated and removed from the internal maps.
         * @param session Session to release
         * @param deallocate bool deallocate session (e.g., connection closed)
         */
        void release_session(ServerSessionPtr session, bool deallocate) override;

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * Allocated sessions are tracked by the _instance_sessions and _db_sessions maps.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @return ServerSessionPtr session
         */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters) override;

    private:
        std::shared_mutex _mutex; ///< mutex for replicas set

        /** list of database instances */
        std::set<DatabaseInstancePtr> _replicas;
    };
    using DatabaseReplicaSetPtr = std::shared_ptr<DatabaseReplicaSet>;

    /**
     * Class representing a database primary, contains a primary
     * and standby database instance.
     * XXX Failover not implemented
     */
    class DatabasePrimarySet : public DatabaseSet {
    public:
        explicit DatabasePrimarySet(int max_sessions_per_instance) :
            DatabaseSet(max_sessions_per_instance)
        {}

        /** Set primary instance */
        void set_primary(DatabaseInstancePtr primary) { _primary = primary; }

        /** Set standby instance */
        void set_standby(DatabaseInstancePtr standby) { _standby = standby; }

        /** Get primary instance */
        DatabaseInstancePtr primary() const { return _primary; }

        /** Get standby instance */
        DatabaseInstancePtr standby() const { return _standby; }

        /**
         * @brief Release session back to the db instance (and associated pool).
         * @param session Session to release
         */
        void release_session(ServerSessionPtr session, bool deallocate) override;

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @return ServerSessionPtr session
         */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters) override;

    private:
        std::shared_mutex _mutex; ///< mutex for primary set

        DatabaseInstancePtr _primary; ///< primary instance
        DatabaseInstancePtr _standby; ///< standby instance XXX not yet implemented
    };
    using DatabasePrimarySetPtr = std::shared_ptr<DatabasePrimarySet>;


    /**
     * @brief Database class for storing all the information pertaining to a replicated database
     */
    class Database {
    public:
        /**
         * @brief Construct a new Database Object from database id and name
         * @param db_id - database id
         * @param db_name - database name
         */
        Database(uint64_t db_id, const std::string &db_name) : _db_name(db_name), _db_id(db_id) {}

        /**
         * @brief Add schema and table
         * @param db_schema - schema
         * @param db_table - table
         */
        void add_schema_table(const std::string &db_schema, const std::string &db_table);

        /**
         * @brief Add schema and tables
         * @param schema_table_pairs - vector of schema and table pairs
         */
        void add_schema_tables(const std::vector<std::pair<std::string, std::string>> &schema_table_pairs);

        /**
         * @brief Remove schema and table
         * @param db_schema - schema
         * @param db_table - table
         */
        void remove_schema_table(const std::string &db_schema, const std::string &db_table);

        /**
         * @brief Verify that the table is replicated
         * @param db_table - table
         * @param db_schema - schema optional
         * @return true - replicated
         * @return false - not replicated
         */
        bool has_table(const std::string &db_table, std::optional<std::string> db_schema) const;

        /**
         * @brief Set database state
         * @param state - state
         */
        void set_state(redis::db_state_change::DBState state) {
            std::unique_lock storage_lock(_db_mutex);
            _state = state;
        }

        /**
         * @brief Get database state
         * @return redis::db_state_change::DBState
         */
        redis::db_state_change::DBState get_state() const {
            std::shared_lock storage_lock(_db_mutex);
            return _state;
        }

        /**
         * @brief Get the database id
         * @return uint64_t - database id
         */
        uint64_t get_db_id() const {
            return _db_id;
        }

        /**
         * @brief Get database name
         * @return std::string - database name
         */
        std::string get_db_name() const {
            return _db_name;
        }
    private:
        std::unordered_multimap<std::string, std::string> _table_map;  ///< maps table name to schema
        mutable std::shared_mutex _db_mutex;            ///< mutex for accessing and modifying database data
        std::string _db_name;                           ///< database name
        uint64_t _db_id;                                ///< database id
        redis::db_state_change::DBState _state;         ///< database state

        /**
         * @brief Helper function to add schema and table to the schema_tables_map (lock must be held)
         * @param db_schema - schema
         * @param db_table  - table
         */
        void _internal_add_schema_table(const std::string &db_schema, const std::string &db_table);
    };
    using DatabasePtr = std::shared_ptr<Database>;

    /**
     * @brief Singleton database manager class. Collects all configuration data from Properties and
     * redis instance.
     * TODO: Missing addition/removal of replica instances (FDWs) via redis pubsub
     */
    class DatabaseMgr final : public Singleton<DatabaseMgr> {
        friend class Singleton<DatabaseMgr>;
    public:

        static constexpr const int POOL_SESSIONS_PER_INSTANCE=5; ///< max sessions per instance

        /**
         * @brief Initialization function
         */
        void init();

        /**
         * @brief Get a name of an arbitrary replicated database for running a user query in UserMgr
         * @return std::optional<std::string> - name of a replicated database if found
         */
        std::optional<std::string> get_any_replicated_db_name() const;

        /**
         * @brief Get database id for given database name
         * @param db_name - database name
         * @return std::optional<uint64_t> - optional database id
         */
        std::optional<uint64_t> get_database_id(const std::string &db_name) const;

        /**
         * @brief Get the database name object
         * @param db_id
         * @return std::optional<std::string>
         */
        std::optional<std::string> get_database_name(const uint64_t db_id) const;

        /**
         * @brief Verifies if the database is in the running state.
         * @param db_id - database id to verify
         * @return true - database is in running state
         * @return false - database is not in the running state
         */
        bool is_database_ready(uint64_t db_id) const;

        /**
         * @brief Check if a database is replicated
         * @param dbname - name of the database
         * @return true - replicated
         * @return false - not replicated
         */
        bool is_database_replicated(const std::string &dbname) const {
            std::shared_lock lock(_db_mutex);
            return _db_name_rep_dbs.contains(dbname);
        }

        /**
         * @brief Set the primary database instance
         * @param instance_id - instance id
         * @param instance - database instance
         */
        void set_primary(uint64_t instance_id, DatabaseInstancePtr instance) {
            _db_instance_id = instance_id;
            _primary_set->set_primary(instance);
        }

        /**
         * @brief Set the secondary database instance
         * @param instance - database instance
         */
        void set_standby(DatabaseInstancePtr instance) {
            _primary_set->set_standby(instance);
        }

        /**
         * @brief Add replica database instance
         * @param instance - database instance
         */
        void add_replica(DatabaseInstancePtr instance) {
            _replica_set->add_replica(instance);
        }

        /**
         * @brief Get primary database set
         * @return DatabasePrimarySet& primary database set reference
         */
        DatabasePrimarySetPtr primary_set() { return _primary_set; }

        /**
         * @brief Get replica database set
         * @return ReplicaDatabaseSet& replica database set reference
         */
        DatabaseReplicaSetPtr replica_set() { return _replica_set; }

        /**
         * @brief Get a server session from pool
         * @param type - session type, primary or replica
         * @param db_id - database id
         * @param username - username
         * @return ServerSessionPtr - server session
         */
        ServerSessionPtr get_pooled_session(const Session::Type type,
                                            const uint64_t db_id,
                                            const std::string &username) {
            if (type == Session::Type::PRIMARY) {
                assert(_primary_set != nullptr);
                return _primary_set->get_session(db_id, username);
            } else if (type == Session::Type::REPLICA) {
                assert(_replica_set != nullptr);
                return _replica_set->get_session(db_id, username);
            }
            assert (0);
            return nullptr;
        }

        /**
         * @brief Allocate a new session from the database set
         * @param type - session type
         * @param db_id - database id
         * @param username - username
         * @param server - proxy server
         * @param user - user
         * @param parameters - startup parameters
         * @return ServerSessionPtr
         */
        ServerSessionPtr allocate_session(const Session::Type type,
                                          const uint64_t db_id,
                                          UserPtr user,
                                          const std::unordered_map<std::string, std::string> &parameters)
        {
            if (type == Session::Type::PRIMARY) {
                CHECK_NE(_primary_set, nullptr);
                return _primary_set->allocate_session(user, db_id, parameters);
            } else if (type == Session::Type::REPLICA) {
                CHECK_NE(_replica_set, nullptr);
                CHECK_NE(db_id, INVALID_DB_ID);
                return _replica_set->allocate_session(user, db_id, parameters);
            }

            DCHECK(false);
            return nullptr;
        }

        /**
         * @brief Verify if the table is replicated for give database and schema
         * @param db_id - database id
         * @param schema - schema name
         * @param table - table name
         * @return true - table is replicated
         * @return false - table is not replicated
         */
        bool is_table_replicated(const uint64_t db_id,
            const std::string &schema,
            const std::string &table) const;

    protected:
        /**
         * @brief Function called by Singleton base class to perform shutdown.
         */
        void _internal_shutdown() override;

    private:
        uint64_t _db_instance_id;           ///< primary database instance id

        RedisCache::RedisChangeWatcherPtr _cache_watcher_db_ids; ///< callback for redis cache db ids
        RedisCache::RedisChangeWatcherPtr _cache_watcher_db_states; ///< callback for redis cache database replica state

        PubSubThread _data_sub_thread;      ///< pubsub thread for redis data database

        DatabasePrimarySetPtr _primary_set; ///< set of primary database and standby database
        DatabaseReplicaSetPtr _replica_set; ///< set of replica databases

        std::map<std::string, DatabasePtr> _db_name_rep_dbs; ///< map of database names to database object
        std::map<uint64_t, DatabasePtr> _db_id_rep_dbs;      ///< map of database ids to database object
        mutable std::shared_mutex _db_mutex;                 ///< shared mutex for the replicated databases maps

        std::string _db_replica_prefix;     ///< prefix to be used for replica database (for testing)

        /**
         * @brief Construct a new Database Mgr object
         */
        DatabaseMgr();

        /**
         * @brief Destroy the Database Mgr object
         */
        ~DatabaseMgr() override = default;

        /**
         * @brief Database schema and table change handling
         * @param msg - message
         */
        void _handle_db_table_change(const std::string &msg);

        /**
         * @brief Initialize pubsub thread for database tables subscriber
         */
        void _init_db_tables_subscriber();

        /**
         * @brief Initialize replicated databases
         */
        void _init_replicated_dbs();

        /**
         * @brief add replicated database
         * @param db_id - database id
         */
        void _add_replicated_database(uint64_t db_id);

        /**
         * @brief remove replicated database
         * @param db_id - database id
         */
        void _remove_replicated_database(uint64_t db_id);
    };

    class DatabaseMgrRunner : public ServiceRunner {
    public:
        DatabaseMgrRunner() : ServiceRunner("DatabaseMgr") {}

        ~DatabaseMgrRunner() override = default;

        bool start() override
        {
            DatabaseMgr::get_instance()->init();
            return true;
        }

        void stop() override
        {
            DatabaseMgr::shutdown();
        }
    };

} // namespace springtail:pg_proxy
