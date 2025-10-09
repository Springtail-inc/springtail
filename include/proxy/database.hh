#pragma once

#include <common/logging.hh>
#include <common/init.hh>
#include <common/constants.hh>

#include <redis/db_state_change.hh>
#include <redis/pubsub_thread.hh>

#include <proxy/server_session.hh>

namespace springtail::pg_proxy {

    /**
     * @brief Class for managing database sessions
     *
     */
    class DatabasePool {
    public:

        struct PoolConfig {
            size_t size_limit{10};           ///< max sessions in pool (0 means no limit)
            size_t timeout_limit{5};         ///< max sessions in pool without expiration (0 means all expire)
            uint64_t expiration_interval{300}; ///< expiration interval in seconds (0 means no expiration)
        };

        /**
         * @brief Construct a new Database Pool object
         * @param config : size_limit - maximum number of sessions in the pool (0 means no max limit on the pool)
         *                 timeout_limit - maximum number of sessions that can be stored in the pool
         *                 without expiration (0 when we want to apply expiration timeout to all stored sessions)
         *                 expiration_interval - session expiration interval in seconds (0 when we do not want for
         *                 sessions to expire)
         */
        explicit DatabasePool(const PoolConfig &config) :
            _size_limit(config.size_limit),
            _timeout_limit(config.timeout_limit),
            _expiration_interval(config.expiration_interval)
        {
            DCHECK((_size_limit == 0 ) || (_size_limit > _timeout_limit));
            // it is invalid to specify a timeout limit, but not specify expiration interval
            DCHECK(!(_timeout_limit > 0 && _expiration_interval == 0));
        }

        /**
         * @brief Destroy the Database Pool object
         *
         */
        ~DatabasePool()
        {
            while (!_cache.empty()) {
                _evict_next();
            }
        }

        /**
         * @brief Add new session to the database pool
         *
         * @param session - new session
         */
        void add_session(ServerSessionPtr session);

        /**
         * @brief Get an existing session from the database pool
         *
         * @param db_id - database id
         * @param username - user name
         * @return ServerSessionPtr - session pointer
         */
        ServerSessionPtr get_session(uint64_t db_id, const std::string &username);

        /**
         * @brief Check if there is a session in the pool for the given database id and user name
         * @param db_id - database id
         * @param username - user name
         * @return true if there is a session
         * @return false if there is no session
         */
        bool has_session(uint64_t db_id, const std::string &username) const;

        /**
         * @brief Evict all sessions from the pool for the given database id
         *
         * @param db_id - database id
         */
        void evict(uint64_t db_id);

        /**
         * @brief This function will evict the oldest expired session when
         *      expiration interval is greater than 0 and the total number of sessions
         *      exceeds timeout limit.
         *
         */
        void evict_expired_sessions();

        /**
         * @brief Get the total number of sessions stored in the pool
         * @return size_t
         */
        size_t size() const
        {
            std::shared_lock lock(_mutex);
            return _cache.size();
        }

        /**
         * @brief Total number of sessions stored in the pool for the given database id and user
         * @param db_id - database id
         * @param username - user name
         * @return size_t - number of sessions
         */
        size_t size(uint64_t db_id, const std::string &username) const;

        /**
         * @brief Get the maximum number of sessions that can be stored in the pool
         * @return size_t - number of sessions
         */
        size_t get_size_limit() const { return _size_limit; }

        /**
         * @brief Get the maximum number of sessions that can be stored in the pool without expiration.
         * @return size_t - number of sessions
         */
        size_t get_timeout_limit() const { return _timeout_limit; }

        /**
         * @brief Shutdown the database pool
         * Evicts all sessions from the pool, calling the eviction callback for each session.
         */
        void shutdown();

    private:
        mutable std::shared_mutex _mutex;                       ///< mutex for pool
        using SessionKey = std::pair<uint64_t, std::string>;    ///< typedef for session key, that is database id and user name pair

        /**
         * @brief Entry datatype for pool cache
         */
        struct SessionEntry {
            SessionKey key;                 ///< session key
            ServerSessionWeakPtr value;     ///< session value
            uint64_t expiration_time;       ///< storage expiration time
        };

        std::map<SessionKey, std::deque<typename std::list<SessionEntry>::iterator>> _lookup; ///< A map that holds the entries.
        std::list<SessionEntry> _cache;     ///< An ordered list of objects for priority removal
        size_t _size_limit;                 ///< Maximum number of sessions that can be stored in cache
        size_t _timeout_limit;              ///< Maximum number of sessions that can be stored in cache without expiration
        uint64_t _expiration_interval;      ///< Expiration interval

        /**
         * @brief Remove entry for the given session key
         * @param key - session key
         */
        void _remove_entry(const SessionKey &key);

        /**
         * @brief Evict next session from the cache.
         */
        void _evict_next();

        /**
         * @brief Insert new session into the cache
         * @param key - session key
         * @param value - session
         */
        void _insert(const SessionKey &key, ServerSessionWeakPtr value);

        /**
         * @brief Get the most recently used session for the given key and user
         * @param key - session key
         * @return ServerSessionPtr - session
         */
        ServerSessionPtr _get(const SessionKey &key);
    };
    using DatabasePoolPtr = std::shared_ptr<DatabasePool>;

    /**
     * @brief Database instance class, contains hostname, port etc.
     */
    class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
    public:
        enum class InstanceState {
            ACTIVE,           // Normal operation, can accept new sessions
            SHUTTING_DOWN,    // No new sessions, waiting for active sessions to complete
        };

        DatabaseInstance(const DatabasePool::PoolConfig &pool_config,
                         const Session::Type type,
                         const std::string &hostname,
                         int port=5432,
                         std::string db_prefix="",
                         std::string replica_id="")
            : _type(type), _hostname(hostname),
              _replica_db_prefix(db_prefix),
              _replica_id(replica_id),
              _port(port)
        {
            _pool = std::make_shared<DatabasePool>(pool_config);
        }

        /**
         * @brief Get hostname
         * @return std::string hostname
         */
        const std::string& hostname() const { return _hostname; }

        /**
         * @brief Get port
         * @return int port
         */
        int port() const { return _port; }

        /**
         * @brief Get prefix
         * @return std::string prefix
         */
        const std::string& prefix() const { return _replica_db_prefix; }

        /**
         * @brief Get replica id
         * @return std::string replica id
         */
        const std::string& replica_id() const { return _replica_id; }

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
            return fmt::format("type={}, hostname={}, port={}, replica_id={}", (_type == Session::Type::PRIMARY ? "PRIMARY" : "REPLICA"), _hostname, _port, _replica_id);
        }

        /**
         * @brief Is the instance active
         * @return true if active, false if shutting down
         */
        bool is_active() const {
            return _state.load() == InstanceState::ACTIVE;
        }

        /**
         * @brief Initiate shutdown of the instance.
         * No new sessions will be allocated.  Shutdown the pool.
         */
        void initiate_shutdown();

        /**
         * @brief Register a callback to be called when the instance is fully shutdown
         * @param callback function to call
         */
        void register_shutdown_callback(std::function<void(DatabaseInstancePtr)> callback) {
            _shutdown_callback = callback;
        }

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * Virtual to allow override in testing.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param database std::string database name
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database);

        /**
         * @brief Get the database pool
         * @return DatabasePoolPtr
         */
        DatabasePoolPtr get_pool() { return _pool; }

        /**
         * @brief Remove session from active sessions map
         * Called from ServerSession destructor
         * @param session_id session id
         */
        void remove_session(uint64_t session_id);

        /**
         * @brief Get the number of all sessions
         * @return int number of all sessions
         */
        int all_session_count() {
            std::shared_lock lock(_active_sessions_mutex);
            return _active_sessions.size();
        }

        /**
         * @brief Get the number of active sessions (not in pool)
         * @return int number of active sessions
         */
        int active_session_count() {
            std::shared_lock lock(_active_sessions_mutex);
            return _active_sessions.size() - _pool->size();
        }

    private:
        std::atomic<InstanceState> _state{InstanceState::ACTIVE}; ///< state of instance
        DatabasePoolPtr _pool{nullptr};      ///< free connections pool for this instance
        Session::Type _type;                 ///< type of instance (primary, replica)
        std::string _hostname;               ///< hostname of instance
        std::string _replica_db_prefix;      ///< prefix to be used for replica database
        std::string _replica_id;             ///< unique id used for identifying replica (fdw)
        int _port;                           ///< port of instance

        /** map of active sessions by id, includes all allocated sessions */
        std::unordered_map<uint64_t, ServerSessionWeakPtr> _active_sessions;
        std::shared_mutex _active_sessions_mutex;   ///< mutex for active sessions map

        /** shutdown callback */
        std::function<void(DatabaseInstancePtr)> _shutdown_callback{nullptr};
    };
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;


    /*
     * @brief Interface for primary set and replica set
     */
    class DatabaseInstanceSet {
    public:

        explicit DatabaseInstanceSet() = default;

        virtual ~DatabaseInstanceSet() = default;

        /**
         * @brief Try and get free session from the session pool if possible
         * @param db_id database id
         * @param username username
         * @return ServerSessionPtr or nullptr if no sessions available;
         *         if nullptr, allocate_session() must be called to create a new session.
         */
        ServerSessionPtr get_pooled_session(uint64_t db_id,
                                            const std::string &username);

        /**
         * @brief Remove database from the instance set
         * @param db_id database id
         */
        virtual void remove_database(uint64_t db_id);

        /**
         * @brief Release session back to pool if space, or deallocate session
         * @param session session to release
         */
        virtual void release_session(ServerSessionPtr session, bool deallocate) = 0;

        /**
         * @brief Release session from the free pools that are expired
         */
         virtual void release_expired_sessions() = 0;

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param database std::string database name
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) = 0;

        /**
         * @brief Add a database instance to the database set
         * @param instance database instance
         */
        void add_instance(DatabaseInstancePtr instance) {
            std::unique_lock lock(_base_mutex);
            _add_instance(instance, lock);
        }

        /**
         * @brief Initiate shutdown of a database instance.  No new sessions will be allocated
         * @param instance database instance
         */
        void shutdown_instance(DatabaseInstancePtr instance)
        {
            std::unique_lock lock(_base_mutex);
            instance->initiate_shutdown();
            _active_instances.erase(instance);
            _shutdown_pending_instances.insert(instance);
        }

    protected:
        /**
         * @brief Add a database instance to the database set
         * @param instance database instance
         * @param lock unique_lock on _base_mutex
         * Note: caller must hold the lock
         */
        void _add_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock)
        {
            DCHECK(lock.owns_lock());
            instance->register_shutdown_callback([this, instance](DatabaseInstancePtr) {
                std::unique_lock lock(_base_mutex);
                _remove_instance(instance, lock);
            });
        }

        /**
         * @brief Remove instance from the replica set
         * @param instance database instance
         */
         void _remove_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock);

        /**
         * @brief Release session back to pool if space, or deallocate session
         * @param session session to release
         * @param deallocate deallocate session
         */
        void _release_session(ServerSessionPtr session, bool deallocate=false);

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param instance DatabaseInstancePtr instance
         * @param database std::string database name
         * @return ServerSessionPtr session
         */
        virtual ServerSessionPtr _allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            DatabaseInstancePtr instance,
            const std::string &database);

        /**
         * @brief Get least loaded instance from the _instances_sessions map
         * @return DatabaseInstancePtr or nullptr if no instances
         */
        DatabaseInstancePtr _get_least_loaded_instance();

    protected:
        /** mutex for maps */
        mutable std::shared_mutex _base_mutex;

        /** set of active instances */
        std::set<DatabaseInstancePtr> _active_instances;

        /** set of shutting down instances */
        std::set<DatabaseInstancePtr> _shutdown_pending_instances;

        /** map of database id to database instance */
        std::unordered_multimap<uint64_t, DatabaseInstancePtr> _db_instances;
    };
    using DatabaseInstanceSetPtr = std::shared_ptr<DatabaseInstanceSet>;

    /**
     * Class representing a database replica set, contains a list of
     * database instances for the replica set.
     */
    class DatabaseReplicaSet : public DatabaseInstanceSet {
    public:
        explicit DatabaseReplicaSet(const DatabasePool::PoolConfig& pool_config) :
            _pool_config(pool_config)
        {}

        /**
         * @brief Add a replica to the replica set
         * @param replica_id fdw_id to add
         */
        void add_replica(const std::string &replica_id);

        /**
         * @brief Release session back to the db instance (and associated pool).
         * If the pool is full, the session is deallocated and removed from the internal maps.
         * @param session Session to release
         * @param deallocate bool deallocate session (e.g., connection closed)
         */
        void release_session(ServerSessionPtr session, bool deallocate) override;

        /**
         * @brief Release session from the free pools that are expired
         */
        void release_expired_sessions() override;

        /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * Allocated sessions are tracked by the _instance_sessions and _db_sessions maps.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param database std::string database name
         * @return ServerSessionPtr session
         */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) override;

        /**
         * @brief Initiate shutdown of a replica instance.  No new sessions will be allocated
         * @param replica_id replica id
         */
        void initiate_replica_shutdown(const std::string &replica_id);

    protected:
        /** pool config used for each instance */
        DatabasePool::PoolConfig _pool_config;
    };
    using DatabaseReplicaSetPtr = std::shared_ptr<DatabaseReplicaSet>;

    /**
     * Class representing a database primary, contains a primary
     * and standby database instance.
     * XXX Failover not implemented
     */
    class DatabasePrimarySet : public DatabaseInstanceSet {
    public:
        explicit DatabasePrimarySet() :
            DatabaseInstanceSet()
        {}

        /** Set primary instance */
        void
        set_primary(DatabaseInstancePtr primary)
        {
            std::unique_lock lock(_base_mutex);
            _primary = primary;
        }

        /** Set standby instance */
        void
        set_standby(DatabaseInstancePtr standby)
        {
            std::unique_lock lock(_base_mutex);
            _standby = standby;
        }

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
         * @brief Release session from the free pools that are expired
         */
         void release_expired_sessions() override;

         /**
         * @brief Allocate a session from the db instance.  Creates a new session.
         * @param user UserPtr user
         * @param db_id uint64_t database id
         * @param parameters std::unordered_map<std::string, std::string> parameters
         * @param database std::string database name
         * @return ServerSessionPtr session
         */
        ServerSessionPtr allocate_session(UserPtr user,
            uint64_t db_id,
            const std::unordered_map<std::string, std::string> &parameters,
            const std::string &database) override;

    private:
        DatabaseInstancePtr _primary{nullptr}; ///< primary instance
        DatabaseInstancePtr _standby{nullptr}; ///< standby instance XXX not yet implemented
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
     */
    class DatabaseMgr final : public Singleton<DatabaseMgr>
    {
        friend class Singleton<DatabaseMgr>;

    public:
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
         * @param replica_id - replica id (fdw id)
         */
        void add_replica(const std::string &replica_id) {
            _replica_set->add_replica(replica_id);
        }

        /**
         * @brief Get primary database set
         * @return DatabasePrimarySetPtr primary database set reference
         */
        DatabasePrimarySetPtr primary_set() { return _primary_set; }

        /**
         * @brief Get replica database set
         * @return ReplicaDatabaseSetPtr replica database set reference
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
                return _primary_set->get_pooled_session(db_id, username);
            } else if (type == Session::Type::REPLICA) {
                assert(_replica_set != nullptr);
                return _replica_set->get_pooled_session(db_id, username);
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
         * @param database - database name
         * @return ServerSessionPtr
         */
        ServerSessionPtr allocate_session(const Session::Type type,
                                          const uint64_t db_id,
                                          UserPtr user,
                                          const std::unordered_map<std::string, std::string> &parameters,
                                          const std::string &database)
        {
            if (type == Session::Type::PRIMARY) {
                CHECK_NE(_primary_set, nullptr);
                return _primary_set->allocate_session(user, db_id, parameters, database);
            } else if (type == Session::Type::REPLICA) {
                CHECK_NE(_replica_set, nullptr);
                CHECK_NE(db_id, constant::INVALID_DB_ID);
                return _replica_set->allocate_session(user, db_id, parameters, database);
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

        /**
         * @brief Function called by Singleton base class to run thread.
         *
         */
        void _internal_run() override;

    private:
        uint64_t _db_instance_id;           ///< primary database instance id

        RedisCache::RedisChangeWatcherPtr _cache_watcher_db_ids;    ///< callback for redis cache db ids
        RedisCache::RedisChangeWatcherPtr _cache_watcher_db_states; ///< callback for redis cache database replica state
        RedisCache::RedisChangeWatcherPtr _cache_watcher_fdws;      ///< callback for redis cache foreign data wrapper state

        PubSubThread _data_sub_thread;      ///< pubsub thread for redis data database

        DatabasePrimarySetPtr _primary_set{nullptr}; ///< set of primary database and standby database
        DatabaseReplicaSetPtr _replica_set{nullptr}; ///< set of replica databases

        std::map<std::string, DatabasePtr> _db_name_rep_dbs; ///< map of database names to database object
        std::map<uint64_t, DatabasePtr> _db_id_rep_dbs;      ///< map of database ids to database object
        mutable std::shared_mutex _db_mutex;                 ///< shared mutex for the replicated databases maps

        std::string _db_replica_prefix;         ///< prefix to be used for replica database (for testing)
        DatabasePool::PoolConfig _pool_config;  ///< pool configuration

        /**
         * @brief Construct a new Database Mgr object
         */
        DatabaseMgr();

        /**
         * @brief Destroy the Database Mgr object
         */
        ~DatabaseMgr() override = default;

        /**
         * @brief Initialization function
         */
        void _init();

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

        /**
         * @brief Handle changes to the list of replicated databases
         * @param new_value - new value
         */
        void _redis_dbs_change_cb(const nlohmann::json &new_value);

        /**
         * @brief Handle changes to the state of a replicated database
         * @param path - path of the change
         * @param new_value - new value
         */
        void _redis_db_state_change_cb(const std::string &path, const nlohmann::json &new_value);

        /**
         * @brief Handle changes to the list of FDWs
         * @param path - path of the change
         * @param new_value - new value
         */
        void _redis_fdw_change_cb(const std::string &path, const nlohmann::json &new_value);
    };

} // namespace springtail:pg_proxy
