#pragma once

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
         */
        ~DatabasePool() {
            try { // for sonarqube
                shutdown();
            } catch (const std::exception &e) {
                LOG_ERROR("Error shutting down database pool: {}", e.what());
            }
        }

        /**
         * @brief Add new session to the database pool
         * @param session - new session
         */
        void add_session(ServerSessionPtr session);

        /**
         * @brief Get an existing session from the database pool
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
        void evict_db(uint64_t db_id);

        /**
         * @brief This function will evict the oldest expired session when
         *      expiration interval is greater than 0 and the total number of sessions
         *      exceeds timeout limit.
         *
         */
        void evict_expired_sessions();

        /**
         * @brief Evict the session with the given id from the pool
         * @param session_id - session id
         */
        void evict_session(uint64_t session_id);

        /**
         * @brief Get the total number of sessions stored in the pool
         * @return size_t
         */
        size_t size() const
        {
            std::shared_lock lock(_mutex);
            return _lru.size();
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

        /**
         * @brief Dump the current state of the database pool
         */
        void dump();

    private:
        mutable std::shared_mutex _mutex;                       ///< mutex for pool
        using SessionKey = std::pair<uint64_t, std::string>;    ///< typedef for session key, that is database id and user name pair

        /**
         * @brief Entry datatype for pool cache
         */
        struct SessionEntry {
            SessionKey key;                 ///< session key
            ServerSessionPtr value;         ///< session value
            uint64_t expiration_time;       ///< storage expiration time
        };

        /** Lookup map for session entries by key {db_id, username} */
        std::map<SessionKey, std::deque<typename std::list<SessionEntry>::iterator>> _lookup;

        /** Map for session entries by session id */
        std::unordered_map<uint64_t, std::list<SessionEntry>::iterator> _session_id_map;

        /** Ordered list of session entries for priority removal */
        std::list<SessionEntry> _lru;

        size_t _size_limit;                 ///< Maximum number of sessions that can be stored in cache
        size_t _timeout_limit;              ///< Maximum number of sessions that can be stored in cache without expiration
        uint64_t _expiration_interval;      ///< Expiration interval

        /**
         * @brief Remove entry for the given session key
         * @param key - session key
         * @return ServerSessionPtr - removed session pointer
         */
        ServerSessionPtr _remove_entry(const SessionKey &key);

        /**
         * @brief Evict next session from the cache.
         * @return ServerSessionPtr - evicted session pointer
         */
        ServerSessionPtr _evict_next();
    };
    using DatabasePoolPtr = std::shared_ptr<DatabasePool>;

    /**
     * @brief Database instance class, contains hostname, port etc.
     */
    class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
    public:
        enum class State {
            NONE,             // Instance not known
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
         * @brief Get JSON representation of instance; used by admin server
         * @return nlohmann::json JSON object representing instance
         */
        nlohmann::json to_json() const {
            std::shared_lock lock(_active_sessions_mutex);
            nlohmann::json j = {
                {"state", (is_active() ? "ACTIVE" : "SHUTTING_DOWN")},
                {"type", (_type == Session::Type::PRIMARY ? "PRIMARY" : "REPLICA")},
                {"hostname", _hostname},
                {"port", _port},
                {"replica_id", _replica_id},
                {"active_sessions", static_cast<int>(_active_sessions.size())},
                {"pooled_sessions", static_cast<int>(_pool->size())}
            };
            return j;
        }

        /**
         * @brief Is the instance active
         * @return true if active, false if shutting down
         */
        bool is_active() const {
            return _state.load() == State::ACTIVE;
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

        /**
         * @brief Get the database pool size
         * @return int number of pooled sessions
         */
        int pooled_session_count() {
            return _pool->size();
        }

        /**
         * @brief Does the pool have a session for the given db_id and user
         * @param db_id database id
         * @param username username
         * @return true
         * @return false
         */
        bool has_pooled_session(uint64_t db_id, const std::string &username) {
            return _pool->has_session(db_id, username);
        }

        /**
         * @brief Get a session from the pool for the given db_id and user
         * @param db_id database id
         * @param username username
         * @return ServerSessionPtr session or nullptr if no session available
         */
        ServerSessionPtr get_pooled_session(uint64_t db_id, const std::string &username) {
            auto session = _pool->get_session(db_id, username);
            DCHECK_GE(_active_sessions.size(), _pool->size());
            return session;
        }

        /**
         * @brief Release expired sessions from the pool
         */
        void release_expired_sessions() {
            _pool->evict_expired_sessions();
        }

        /**
         * @brief Evict all sessions for the given database id from the pool
         * @param db_id database id
         */
        void evict_pooled_sessions(uint64_t db_id) {
            _pool->evict_db(db_id);
        }

        /**
         * @brief Release session back to pool, or deallocate session
         * On deallocate, the session is removed from the pool, if it was there,
         * the session destructor will call back into remove_session() which
         * will remove it from the active sessions map.
         * @param session session to release
         * @param deallocate deallocate session, if false add back to pool,
         *                   otherwise session will ultimately be destroyed
         */
        void release_session(ServerSessionPtr session, bool deallocate=false)
        {
            if (deallocate) {
                _pool->evict_session(session->id());
            } else {
                _pool->add_session(session);
            }
            if (_active_sessions.size() < _pool->size()) {
                LOG_ERROR("Active sessions {} less than pooled sessions {}",
                          _active_sessions.size(), _pool->size());
                dump();
            }
            DCHECK_GE(_active_sessions.size(), _pool->size());
        }

        /**
         * @brief Dump the current state of the database instance sessions
         */
        void dump();

        /**
         * @brief Get the current state of the instance
         * @return DatabaseInstance::State
         */
        State state() const {
            return _state.load();
        }

    protected: // for testing
        /** map of active sessions by id, includes all allocated sessions (incl. pooled) */
        std::unordered_map<uint64_t, ServerSessionWeakPtr> _active_sessions;
        mutable std::shared_mutex _active_sessions_mutex;   ///< mutex for active sessions map

        /**
         * @brief Add session to active sessions map
         * Called from allocate_session() and can be called from test code
         * @param session session to add
         */
        void _add_session(ServerSessionPtr session) {
            std::unique_lock lock(_active_sessions_mutex);
            _active_sessions[session->id()] = session;
            DCHECK_GE(_active_sessions.size(), _pool->size());
        }

    private:
        std::atomic<State> _state{State::ACTIVE}; ///< state of instance
        DatabasePoolPtr _pool{nullptr};      ///< free connections pool for this instance
        Session::Type _type;                 ///< type of instance (primary, replica)
        std::string _hostname;               ///< hostname of instance
        std::string _replica_db_prefix;      ///< prefix to be used for replica database
        std::string _replica_id;             ///< unique id used for identifying replica (fdw)
        int _port;                           ///< port of instance

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
         * @brief Get JSON representation of instance set; used by admin server
         * @return nlohmann::json
         */
        virtual nlohmann::json to_json() const {
            nlohmann::json j = nlohmann::json::array();
            std::shared_lock lock(_base_mutex);
            for (const auto &instance: _active_instances) {
                j.push_back(instance->to_json());
            }
            for (const auto &instance: _shutdown_pending_instances) {
                j.push_back(instance->to_json());
            }
            return j;
        }

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
            _shutdown_instance(instance, lock);
        }

    protected:
        /**
         * @brief Add a database instance to the instance set
         * @param instance database instance
         * @param lock unique_lock on _base_mutex
         * Note: caller must hold the lock
         */
        void _add_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock)
        {
            DCHECK(lock.owns_lock());
            DCHECK(instance->is_active());
            _active_instances.insert(instance);
            lock.unlock();
            instance->register_shutdown_callback([this, instance](DatabaseInstancePtr) {
                std::unique_lock lock(_base_mutex);
                _remove_instance(instance, lock);
            });
        }

        /**
         * @brief Remove instance from the instance set; must be in _shutdown_pending_instances
         * @param instance database instance
         * @param lock unique_lock on _base_mutex
         * Note: caller must hold the lock
         */
        void _remove_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock);

        /**
         * @brief Shutdown instance; move it from active to shutdown_pending
         * @param instance database instance
         * @param lock unique_lock on _base_mutex
         * Note: caller must hold the lock
         */
        void _shutdown_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock)
        {
            DCHECK(lock.owns_lock());
            _active_instances.erase(instance);
            _shutdown_pending_instances.insert(instance);
            lock.unlock();
            // don't hold lock while initiating shutdown as it may call back into us
            instance->initiate_shutdown();
        }

        /**
         * @brief Release session back to pool if space, or deallocate session
         * @param session session to release
         * @param deallocate deallocate session
         * @param lock unique_lock on _base_mutex
         * Note: caller must hold the lock
         */
        void _release_session(ServerSessionPtr session, bool deallocate);

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

    protected:
        /** mutex for maps */
        mutable std::shared_mutex _base_mutex;

        /** set of active instances */
        std::set<DatabaseInstancePtr> _active_instances;

        /** set of shutting down instances */
        std::set<DatabaseInstancePtr> _shutdown_pending_instances;
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

        /**
         * @brief Get the replica instance object
         * @param replica_id replica id string
         * @return DatabaseInstancePtr
         */
        DatabaseInstancePtr get_replica_instance(const std::string &replica_id) const;

        /**
         * @brief Get the current state of the instance
         * @return DatabaseInstance::State
         */
        DatabaseInstance::State get_replica_state(const std::string &replica_id) const {
            auto instance = get_replica_instance(replica_id);
            if (instance) {
                return instance->state();
            }
            return DatabaseInstance::State::NONE;
        }

        /**
         * @brief Update the set of running database ids for the given replica.
         *
         * @param replica_id - replica id
         * @param new_db_ids - new set of database ids
         */
        virtual void update_fdw_db_ids(const std::string &replica_id, std::set<uint64_t> new_db_ids);

        /**
         * @brief Remove database for all replicas
         *
         * @param db_id - database id
         */
        void remove_database(uint64_t db_id) override;

        /**
         * @brief Get JSON representation of replica set; used by admin server
         * @return nlohmann::json
         */
        nlohmann::json to_json() const override;

    protected:
        /**
         * @brief Get least loaded instance from the _instances_sessions map
         * @return DatabaseInstancePtr or nullptr if no instances
         */
        DatabaseInstancePtr _get_least_loaded_instance(uint64_t db_id);

        /** pool config used for each instance */
        DatabasePool::PoolConfig _pool_config;

        std::map<std::string, std::set<uint64_t>>  _fdw_dbs; ///< map of FDW ids to the list of databases active on this FDW
        mutable std::shared_mutex _fdw_mutex;                ///< mutex for access to _fdw_dbs
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

        /**
         * @brief Get JSON representation of primary/standby set
         * @return nlohmann::json
         */
        nlohmann::json to_json() const override {
            return _primary->to_json();
        }

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
        void add_replica(const std::string &replica_id);

        /**
         * @brief Remove replica database instance
         * @param replica_id - replica id (fdw id)
         */
        void remove_replica(const std::string &replica_id);

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

        /**
         * @brief Get JSON representation of database manager data
         * @return nlohmann::json
         */
        nlohmann::json to_json() const;

        /**
         * @brief Add replicated database
         * public for testing
         * @param db_object - database object
         */
        void add_database(DatabasePtr db_object) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Added database (id, name): ({}, {})", db_object->get_db_id(), db_object->get_db_name());
            std::unique_lock<std::shared_mutex> database_lock(_db_mutex);
            _db_name_rep_dbs.insert(std::pair<std::string, DatabasePtr>(db_object->get_db_name(), db_object));
            _db_id_rep_dbs.insert(std::pair<uint64_t, DatabasePtr>(db_object->get_db_id(), db_object));
        }

    protected:
        /**
         * @brief Function called by Singleton base class to perform shutdown.
         */
        void _internal_shutdown() override;

        /**
         * @brief Function called by Singleton base class to prior to thread shutdown.
         */
        void _internal_thread_shutdown() override;

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
        RedisCache::RedisChangeWatcherPtr _cache_watcher_fdw_dbs;   ///< callback for redis cache FDW dbs

        PubSubThread _data_sub_thread;      ///< pubsub thread for redis data database

        DatabasePrimarySetPtr _primary_set{nullptr}; ///< set of primary database and standby database
        DatabaseReplicaSetPtr _replica_set{nullptr}; ///< set of replica databases

        std::map<std::string, DatabasePtr> _db_name_rep_dbs; ///< map of database names to database object
        std::map<uint64_t, DatabasePtr> _db_id_rep_dbs;      ///< map of database ids to database object
        mutable std::shared_mutex _db_mutex;                 ///< shared mutex for the replicated databases maps

        std::string _db_replica_prefix;         ///< prefix to be used for replica database (for testing)
        DatabasePool::PoolConfig _pool_config;  ///< pool configuration

        std::condition_variable _shutdown_cv;    ///< condition variable for shutdown
        std::mutex _shutdown_mutex;              ///< mutex for shutdown

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
         * @param new_value - new value
         */
        void _redis_fdw_change_cb(const nlohmann::json &new_value);

        /**
         * @brief Register watcher for the given fdw id
         *
         * @param fdw_id - FDW id
         */
        void _register_fdw_dbs_cache_watcher(const std::string &fdw_id);

        /**
         * @brief Deregister watcher for the given fdw id
         *
         * @param fdw_id - FDW id
         */
        void _deregister_fdw_dbs_cache_watcher(const std::string &fdw_id);
    };
    using DatabaseMgrPtr = std::shared_ptr<DatabaseMgr>;

} // namespace springtail:pg_proxy
