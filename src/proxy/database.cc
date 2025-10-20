#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>

#include <proxy/client_session.hh>
#include <proxy/database.hh>
#include <proxy/exception.hh>

#include <redis/redis_db_tables.hh>

namespace springtail::pg_proxy
{

    /*********** Database Pool *************/
    void
    DatabasePool::add_session(ServerSessionPtr session)
    {
        SessionKey key = std::make_pair(session->database_id(), session->username());
        LOG_INFO("Adding session to pool: db_id={}, user={}, session_id={}",
                 key.first, key.second, session->id());

        SessionPtr released_session = nullptr;
        std::unique_lock lock(_mutex);

        // evict the next session if we are at the size limit
        if (_size_limit != 0  && _lru.size() == _size_limit) {
            released_session = _evict_next();
        }

        // find the deque for this key, or create a new one
        auto it = _lookup.find(key);
        if (it == _lookup.end()) {
            auto result = _lookup.emplace(key, std::deque<typename std::list<SessionEntry>::iterator>());
            DCHECK(result.second);
            it = result.first;
        }

        // compute expiration time
        uint64_t expiration_time = std::numeric_limits<uint64_t>::max();
        if (_expiration_interval != 0) {
            uint64_t now = common::get_time_in_millis() / 1000;
            expiration_time = now + _expiration_interval;
        }

        // insert new entry at the front of the lru list
        SessionEntry entry = {key, session, expiration_time};
        _lru.emplace_front(entry);
        auto lru_it = _lru.begin();

        // insert into deque for this key
        it->second.push_front(lru_it);

        // insert into session id map
        _session_id_map[session->id()] = lru_it;
        lock.unlock();

        // release the evicted session after unlocking
        if (released_session) {
            released_session.reset();
        }
    }

    ServerSessionPtr
    DatabasePool::get_session(uint64_t db_id, const std::string &username)
    {
        SessionKey key = std::make_pair(db_id, username);
        std::unique_lock lock(_mutex);

        // find the deque for this key
        auto it = _lookup.find(key);
        if (it == _lookup.end() || it->second.empty()) {
            return nullptr;
        }

        // get the first entry from the deque
        DCHECK(!(it->second.empty()));
        auto &&lru_it = it->second.front();
        SessionEntry &entry = *(lru_it);

        // remove from session id map
        _session_id_map.erase(entry.value->id());

        // keep reference to session to return
        auto session = entry.value;

        // remove from lru list and deque
        _lru.erase(lru_it);
        it->second.pop_front();

        return session;
    }

    void
    DatabasePool::evict_db(uint64_t db_id)
    {
        std::vector<ServerSessionPtr> evicted_sessions;
        std::unique_lock lock(_mutex);

        // find matching db id entries
        auto db_it = _lookup.lower_bound({db_id, ""});
        while (db_it != _lookup.end() && db_it->first.first == db_id) {
            auto &deque = db_it->second;

            // evict all entries for this db_id from the deque
            for (auto &lru_it: deque) {
                evicted_sessions.push_back(lru_it->value);
                _session_id_map.erase(lru_it->value->id());
                _lru.erase(lru_it);
            }
            deque.clear();

            // erase the map entry
            db_it = _lookup.erase(db_it);
        }
        lock.unlock();

        // release sessions after unlocking
        evicted_sessions.clear();
    }

    void
    DatabasePool::evict_expired_sessions()
    {
        std::unique_lock lock(_mutex);
        // cleanup based on the lru entry timestamp
        if (_expiration_interval == 0) {
            return;
        }

        // go through the lru list and remove expired entries
        std::vector<ServerSessionPtr> evicted_sessions;
        uint64_t now = common::get_time_in_millis() / 1000;
        while (_lru.size() > _timeout_limit) {
            // get last entry, check expiration time
            SessionEntry &lru_entry = _lru.back();
            if (lru_entry.expiration_time > now) {
                break;
            }

            // remove the entry
            auto session = _remove_entry(lru_entry.key);
            DCHECK_NE(session, nullptr);
            evicted_sessions.push_back(std::move(session));
        }
        lock.unlock();

        // release sessions after unlocking
        evicted_sessions.clear();
    }

    void
    DatabasePool::evict_session(uint64_t session_id)
    {
        std::unique_lock lock(_mutex);

        // find the session by id
        auto it = _session_id_map.find(session_id);
        if (it == _session_id_map.end()) {
            return;
        }
        _session_id_map.erase(it);

        // find the deque for this key
        auto lru_it = it->second;
        SessionEntry &entry = *(lru_it);
        auto lookup_it = _lookup.find(entry.key);
        DCHECK(!(lookup_it == _lookup.end()));

        // remove the entry from the lookup, need to find in the deque
        auto &deque = lookup_it->second;
        auto deque_it = std::find(deque.begin(), deque.end(), lru_it);
        DCHECK(!(deque_it == deque.end()));

        // remove from deque
        deque.erase(deque_it);
        if (deque.empty()) {
            _lookup.erase(lookup_it);
        }

        // keep reference to session to release after unlocking
        auto session = entry.value;

        // remove from lru list
        _lru.erase(lru_it);

        lock.unlock();

        // release session after unlocking
        session.reset();
    }

    size_t
    DatabasePool::size(uint64_t db_id, const std::string &username) const
    {
        SessionKey key = std::make_pair(db_id, username);
        std::shared_lock lock(_mutex);
        auto it = _lookup.find(key);
        if (it != _lookup.end()) {
            return it->second.size();
        }
        return 0;
    }

    ServerSessionPtr
    DatabasePool::_remove_entry(const SessionKey &key)
    {
        // remove from lookup map
        auto it = _lookup.find(key);
        if (it == _lookup.end()) {
            return nullptr;
        }

        // check that deque for this key is not empty
        DCHECK(!(it->second.empty()));
        auto &&lru_it = it->second.back();
        // remove entry from _lookup
        it->second.pop_back();

        if (it->second.empty()) {
            _lookup.erase(it);
        }

        // remove from session_id map
        auto &entry = *lru_it;
        _session_id_map.erase(entry.value->id());

        // keep reference to session to return
        // we do this to avoid deallocating while holding the lock
        auto session = entry.value;

        // remove from lru lru list
        _lru.erase(lru_it);

        return session;
    }

    ServerSessionPtr
    DatabasePool::_evict_next()
    {
        // get last entry
        if (_lru.empty()) {
            return nullptr;
        }

        SessionEntry &lru_entry = _lru.back();
        return _remove_entry(lru_entry.key);
    }

    void
    DatabasePool::shutdown()
    {
        std::vector<ServerSessionPtr> evicted_sessions;
        std::unique_lock lock(_mutex);
        while (!_lru.empty()) {
            auto session = _evict_next();
            if (session) {
                evicted_sessions.push_back(std::move(session));
            }
        }
        lock.unlock();
        evicted_sessions.clear();
    }

    bool
    DatabasePool::has_session(uint64_t db_id, const std::string &username) const
    {
        SessionKey key = std::make_pair(db_id, username);
        std::shared_lock lock(_mutex);
        return _lookup.contains(key);
    }

    void
    DatabasePool::dump()
    {
        std::shared_lock lock(_mutex);
        LOG_INFO("DatabasePool dump: size_limit={}, expiration_interval={}, timeout_limit={}, current_size={}",
                 _size_limit, _expiration_interval, _timeout_limit, _lru.size());
        for (const auto &entry : _lru) {
            LOG_INFO("  Session[id={}, db_id={}, user={}, exp_time={}]",
                     entry.value->id(), entry.key.first, entry.key.second, entry.expiration_time);
        }
    }

    /*********** Database Instance Set *************/

    void
    DatabaseInstanceSet::_remove_instance(DatabaseInstancePtr instance, std::unique_lock<std::shared_mutex> &lock)
    {
        // remove an inactive instance from the set
        // called from shutdown callback in DatabaseInstance
        LOG_INFO("Removing inactive instance: [hostname:{} R:{}]", instance->hostname(), instance->replica_id());

        DCHECK(lock.owns_lock());
        DCHECK_EQ(instance->is_active(), false);

        // remove from internal maps/sets
        _shutdown_pending_instances.erase(instance);
    }

    void
    DatabaseInstanceSet::remove_database(uint64_t db_id)
    {
        std::unique_lock lock(_base_mutex);

        // evict pooled sessions for this db_id
        for (const auto &db_instance : _active_instances) {
            db_instance->evict_pooled_sessions(db_id);
        }
    }

    void
    DatabaseInstanceSet::_release_session(ServerSessionPtr session,
                                          bool deallocate)
    {
        // no lock is held
        auto instance = session->get_instance();

        // deallocate if connection is closed or database id is invalid, or instance is not active
        if (session->is_connection_closed() ||
            session->database_id() == constant::INVALID_DB_ID ||
            instance->is_active() == false ||
            !_active_instances.contains(instance)) {
            deallocate = true;
        }

        LOG_INFO("Session being released: [S:{:d}], deallocate: {}", session->id(), deallocate);
        instance->release_session(session, deallocate);

        return;
    }

    ServerSessionPtr
    DatabaseInstanceSet::_allocate_session(UserPtr user,
        uint64_t db_id,
        const std::unordered_map<std::string, std::string> &parameters,
        DatabaseInstancePtr instance,
        const std::string &database)
    {
        // create a new session from instance
        auto session = instance->allocate_session(user, db_id, parameters, database);

        return session;
    }

    DatabaseInstancePtr
    DatabaseInstanceSet::_get_least_loaded_instance()
    {
        std::shared_lock lock(_base_mutex);

        if (_active_instances.empty()) {
            return nullptr;
        }

        // find the instance with the least number of sessions
        DatabaseInstancePtr instance = nullptr;
        int min_active_sessions = INT_MAX;
        int min_total_sessions = INT_MAX;

        for (auto &it : _active_instances) {
            int num_active_sessions = it->active_session_count();
            int num_sessions = it->all_session_count();

            if (num_active_sessions < min_active_sessions) {
                // first look at only active sessions
                min_total_sessions = num_sessions;
                min_active_sessions = num_active_sessions;
                instance = it;
            } else if (num_active_sessions == min_active_sessions &&
                       num_sessions < min_total_sessions) {
                // break tie by total sessions
                min_total_sessions = num_sessions;
                min_active_sessions = num_active_sessions;
                instance = it;
            }
        }

        return instance;
    }

    ServerSessionPtr
    DatabaseInstanceSet::get_pooled_session(uint64_t db_id,
                                            const std::string &username)
    {
        // called from dbmgr->get_pooled_session()
        // get a free session from the pool if possible
        std::shared_lock lock(_base_mutex);
        if (_active_instances.empty()) {
            return nullptr;
        }

        // track instances that have a session for this db_id and user in the pool
        int min_sessions = std::numeric_limits<int>::max();
        DatabaseInstancePtr selected_instance = nullptr;

        // first look for a free pooled session
        for (const auto &db_instance : _active_instances) {
            if (!db_instance->is_active()) {
                continue;
            }

            // check if a user for the db id exists in the pool
            if (!db_instance->has_pooled_session(db_id, username)) {
                continue;
            }

            // get number of active sessions for this instance
            if (db_instance->active_session_count() < min_sessions) {
                min_sessions = db_instance->active_session_count();
                selected_instance = db_instance;
            }
        }

        if (selected_instance != nullptr) {
            return selected_instance->get_pooled_session(db_id, username);
        }

        return nullptr;
    }

    /*********** Database Replica Set *************/

    void
    DatabaseReplicaSet::initiate_replica_shutdown(const std::string &replica_id)
    {
        DatabaseInstancePtr replica = nullptr;

        std::unique_lock lock(_base_mutex);

        // find the instance, a little heavy handed but doesn't happen often
        // better than keeping another map that needs to be kept in sync
        for (auto &it : _active_instances) {
            if (it->replica_id() == replica_id) {
                replica = it;
                break;
            }
        }

        if (replica == nullptr) {
            LOG_ERROR("Could not find replica instance with replica id {}", replica_id);
            DCHECK_NE(replica, nullptr);
            return;
        }

        // mark instance as shutting down; releases the lock
        _shutdown_instance(replica, lock);
    }

    void
    DatabaseReplicaSet::add_replica(const std::string &replica_id)
    {
        // create new instance; fetch config from properties (redis)
        nlohmann::json fdw_config = Properties::get_fdw_config(replica_id);
        auto host = Json::get<std::string>(fdw_config, "host");
        auto port = Json::get<uint16_t>(fdw_config, "port");
        auto db_prefix = Json::get<std::string>(fdw_config, "db_prefix");
        auto state = Json::get<std::string>(fdw_config, "state");

        if (!host.has_value() || !port.has_value() ||
            !state.has_value() || state.value() != Properties::FDW_STATE_RUNNING) {
            return;
        }

        // add replica
        if (!db_prefix.has_value()) {
            db_prefix = "";
        }

        LOG_INFO("Added replica instance with replica id {}", replica_id);

        std::unique_lock lock(_base_mutex);

        // check if replica with this id already exists
        for (const auto &it : _active_instances) {
            if (it->replica_id() == replica_id) {
                LOG_WARN("Replica instance with replica id {} already exists, ignoring add", replica_id);
                return;
            }
        }

        auto instance = std::make_shared<DatabaseInstance>(
            _pool_config,
            Session::Type::REPLICA,
            host.value(),
            port.value(),
            db_prefix.value(),
            replica_id);

        DatabaseInstanceSet::_add_instance(instance, lock);
    }

    void
    DatabaseReplicaSet::release_session(ServerSessionPtr session, bool deallocate)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "Replica session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::REPLICA);

        DatabaseInstanceSet::_release_session(session, deallocate);
    }

    void
    DatabaseReplicaSet::release_expired_sessions()
    {
        std::shared_lock lock(_base_mutex);
        // iterate over all replicas and evict expired sessions
        for (auto instance : _active_instances) {
            instance->release_expired_sessions();
        }
    }

    ServerSessionPtr
    DatabaseReplicaSet::allocate_session(UserPtr user,
                                         uint64_t db_id,
                                         const std::unordered_map<std::string, std::string> &parameters,
                                         const std::string &database)
    {
        std::shared_lock lock(_base_mutex);

        auto instance = _get_least_loaded_instance();
        if (instance == nullptr) {
            return nullptr;
        }

        return _allocate_session(user, db_id, parameters, instance, database);
    }

    DatabaseInstancePtr
    DatabaseReplicaSet::get_replica_instance(const std::string &replica_id) const
    {
        std::shared_lock lock(_base_mutex);
        for (auto &it : _active_instances) {
            if (it->replica_id() == replica_id) {
                return it;
            }
        }
        return nullptr;
    }

    /*********** Database Primary Set *************/

    void
    DatabasePrimarySet::release_session(ServerSessionPtr session, bool deallocate)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG2, "Primary Session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::PRIMARY);

        std::shared_lock lock(_base_mutex);

        // check if primary instance is still alive, if so try to add to pool
        if (session->get_instance() != _primary) {
            deallocate = true;
        }

        _release_session(session, deallocate);
    }

    void
    DatabasePrimarySet::release_expired_sessions()
    {
        std::shared_lock lock(_base_mutex);
        if (_primary != nullptr) {
            _primary->release_expired_sessions();
        }
        if (_standby != nullptr) {
            _standby->release_expired_sessions();
        }
    }

    ServerSessionPtr
    DatabasePrimarySet::allocate_session(UserPtr user,
                                         uint64_t db_id,
                                         const std::unordered_map<std::string, std::string> &parameters,
                                         const std::string &database)
    {
        std::shared_lock lock(_base_mutex);

        // get primary instance
        auto instance = _primary;
        if (instance == nullptr) {
            return nullptr;
        }

        // allocate session
        auto session = _allocate_session(user, db_id, parameters, instance, database);

        return session;
    }

    /*********** Database Instance *************/

    ServerSessionPtr
    DatabaseInstance::allocate_session(UserPtr user,
                                       uint64_t db_id,
                                       const std::unordered_map<std::string, std::string> &parameters,
                                       const std::string &database)
    {
        auto db_name = DatabaseMgr::get_instance()->get_database_name(db_id);
        // create a new session; this is a blocking activity as it requires creating a connection
        auto session = ServerSession::create(user, db_name.value_or(database), prefix(), shared_from_this(), _type, parameters);

        // add to active sessions map; removed in ServerSession destructor
        _add_session(session);

        return session;
    }

    void
    DatabaseInstance::remove_session(uint64_t session_id)
    {
        LOG_INFO("[DB:{}] Removing session S:{}", to_string(), session_id);

        // callback from ServerSession destructor
        std::unique_lock lock(_active_sessions_mutex);
        _active_sessions.erase(session_id);

        DCHECK_GE(_active_sessions.size(), _pool->size());

        // if shutting down and no more active sessions, call shutdown callback
        if (_state.load() == InstanceState::SHUTTING_DOWN && _active_sessions.empty()) {
            lock.unlock();
            LOG_INFO("[DB:{}] All sessions completed, shutting down instance", to_string());
            if (_shutdown_callback) {
                _shutdown_callback(shared_from_this());
            }
        }
    }

    void
    DatabaseInstance::initiate_shutdown()
    {
        // atomic test and set _state to SHUTTING_DOWN
        InstanceState expected = InstanceState::ACTIVE;
        if (!_state.compare_exchange_strong(expected, InstanceState::SHUTTING_DOWN)) {
            return; // already shutting down or inactive
        }

        _state.store(InstanceState::SHUTTING_DOWN);
        _pool->shutdown();

        std::unique_lock lock(_active_sessions_mutex);
        LOG_INFO("[DB:{}] Shutting down, waiting for {} active sessions", to_string(), _active_sessions.size());

        // go through active sessions and notify client sessions of failover
        for (auto it = _active_sessions.begin(); it != _active_sessions.end(); ++it) {
            auto &session_weak = it->second;
            auto session = session_weak.lock();
            if (session != nullptr) {
                auto client_session = session->get_client_session();
                if (client_session != nullptr) {
                    // enqueue a failover notification to the client session, this is non-blocking
                    client_session->queue_failover_notification();
                }
            } else {
                LOG_INFO("[DB:{}] Session expired during shutdown", to_string());
                // session already expired, remove from map
                it = _active_sessions.erase(it);
            }
        }

        if (_active_sessions.empty()) {
            LOG_INFO("[DB:{}] No active sessions, shutting down instance", to_string());
            lock.unlock();
            if (_shutdown_callback) {
                // defined in DatabaseInstanceSet::_add_instance()
                _shutdown_callback(shared_from_this());
            }
            return;
        }
    }

    void
    DatabaseInstance::dump()
    {
        std::shared_lock lock(_active_sessions_mutex);
        LOG_INFO("DatabaseInstance dump: [DB:{}] Active sessions: {}, Pooled sessions: {}",
                 to_string(), _active_sessions.size(), _pool->size());
        for (auto &[id, session_weak] : _active_sessions) {
            auto session = session_weak.lock();
            if (session != nullptr) {
                LOG_INFO("  Session[id={}, user={}, db_id={}, type={}]",
                         session->id(), session->username(), session->database_id(),
                         session->type() == Session::Type::PRIMARY ? "PRIMARY" : "REPLICA");
            } else {
                LOG_INFO("  Session[id={}] expired", id);
            }
        }
        _pool->dump();
    }

    /*********** Database *************/

    void
    Database::add_schema_tables(const std::vector<std::pair<std::string, std::string>> &schema_table_pairs)
    {
        std::unique_lock lock(_db_mutex);

        for (const auto &[schema, table]: schema_table_pairs) {
            _internal_add_schema_table(schema, table);
        }
    }


    void
    Database::add_schema_table(const std::string &db_schema, const std::string &db_table)
    {
        std::unique_lock lock(_db_mutex);
        _internal_add_schema_table(db_schema, db_table);
    }

    void
    Database::_internal_add_schema_table(const std::string &db_schema, const std::string &db_table)
    {
        // lock must be held
        _table_map.insert({db_table, db_schema});
    }

    void
    Database::remove_schema_table(const std::string &db_schema, const std::string &db_table)
    {
        std::unique_lock lock(_db_mutex);

        auto range = _table_map.equal_range(db_table);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == db_schema) {
                _table_map.erase(it);
                return;
            }
        }
    }

    bool
    Database::has_table(const std::string &db_table,
                        std::optional<std::string> db_schema) const
    {
        std::shared_lock lock(_db_mutex);

        auto range = _table_map.equal_range(db_table);
        for (auto it = range.first; it != range.second; ++it) {
            // if no schema is provided, check if the table exists
            if (!db_schema.has_value()) {
                return true;
            }

            // if we have a schema then use it
            if (it->second == db_schema.value()) {
                return true;
            }
        }

        return false;
    }

    /*********** Database Mgr *************/

    DatabaseMgr::DatabaseMgr() :
        Singleton<DatabaseMgr>(ServiceId::DatabaseMgrId),
        _data_sub_thread(1, false),
        _primary_set(std::make_shared<DatabasePrimarySet>())
    {
        // get pool parameters
        nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);

        size_t pool_size_limit = Json::get_or<size_t>(json, "pool_size_limit", 0);
        size_t pool_timeout_limit = Json::get_or<size_t>(json, "pool_timeout_limit", 0);
        uint64_t pool_expiration_interval = Json::get_or<size_t>(json, "pool_expiration_interval_secs", 0);
        if (pool_timeout_limit != 0 && pool_expiration_interval == 0) {
            LOG_ERROR("Pool timeout limit is set to {} while expiration interval is not defined", pool_timeout_limit);
            throw ProxyServerError();
        }

        _pool_config = {pool_size_limit, pool_timeout_limit, pool_expiration_interval};

        // create replica set with pool config
        _replica_set = std::make_shared<DatabaseReplicaSet>(_pool_config);

        // redis cache watcher to handle changes to the list of replicated databases
        _cache_watcher_db_ids = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Replicated databases: {}", new_value.dump(4));
                CHECK_EQ(path, Properties::DATABASE_IDS_PATH);
                _redis_dbs_change_cb(new_value);
            }
        );

        // redis cache watcher to handle changes to the list of replicated database states
        _cache_watcher_db_states = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Replicated database state change; path: {}, state: {}",
                    path, new_value.dump(4));
                CHECK(path.starts_with(Properties::DATABASE_STATE_PATH));
                _redis_db_state_change_cb(path, new_value);
            }
        );

        // redis cache watcher to handle changes to the list of FDWs and their states
        _cache_watcher_fdws = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "FDW configurations changed: {}", new_value.dump(4));
                CHECK(path.starts_with(Properties::FDW_CONFIG_PATH));
                _redis_fdw_change_cb(path, new_value);
            }
        );

        _init();
    }

    void
    DatabaseMgr::_redis_dbs_change_cb(const nlohmann::json &new_value)
    {
        // get a vector of old database ids from _log_mgrs
        std::shared_lock<std::shared_mutex> lock(_db_mutex);
        auto keys = std::views::keys(_db_id_rep_dbs);
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

    void
    DatabaseMgr::_redis_db_state_change_cb(const std::string &path,
                                           const nlohmann::json &new_value)
    {
        // extract database id from path
        std::vector<std::string> path_parts;
        common::split_string("/", path, path_parts);
        DCHECK_EQ(path_parts.size(), 2);
        uint64_t db_id = stoull(path_parts[1]);

        // if we ever get in here, this means that this database will be deleted
        if (new_value.type() == nlohmann::json::value_t::null) {
            return;
        }

        // extract state
        DCHECK(new_value.type() == nlohmann::json::value_t::string);
        std::string state_str = new_value.get<std::string>();
        redis::db_state_change::DBState state = redis::db_state_change::db_state_map[state_str];

        // shared lock for lookup
        std::shared_lock lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return;
        }

        iter->second->set_state(state);
    }

    void
    DatabaseMgr::_redis_fdw_change_cb(const std::string &path,
                                      const nlohmann::json &new_value)
    {
        // extract database id from path
        std::vector<std::string> path_parts;
        common::split_string("/", path, path_parts);
        DCHECK_EQ(path_parts.size(), 2);
        auto replica_id = path_parts[1];

        DCHECK(new_value.type() != nlohmann::json::value_t::null);

        // extract fdw state
        // see: https://www.notion.so/springtail-hq/Database-Schema-1273ea3f343c8098bf95d78b6a3741aa
        // states: initialize->running->draining->stopped
        auto state = Json::get<std::string>(new_value, "state");

        if (state == Properties::FDW_STATE_DRAINING) {
            // initiate shutdown of the replica instance
            _replica_set->initiate_replica_shutdown(replica_id);
            return;
        }

        if (state == Properties::FDW_STATE_RUNNING) {
            // add the replica instance if not already present
            add_replica(replica_id);
            return;
        }
    }

    void
    DatabaseMgr::_init()
    {
        // add primary
        uint64_t primary_instance_id = Properties::get_db_instance_id();
        std::string host, user, password;
        int port;
        Properties::get_primary_db_config(host, port, user, password);
        set_primary(primary_instance_id, std::make_shared<DatabaseInstance>(_pool_config, Session::Type::PRIMARY, host, port));

        // iterate through fdws and add replicas
        std::vector<std::string> fdw_id_list = Properties::get_fdw_ids();
        for (const auto & fdw_id: fdw_id_list) {
            add_replica(fdw_id);
        }

        // add redis cache watchers
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher_db_ids);
        redis_cache->add_callback(Properties::FDW_CONFIG_PATH, _cache_watcher_fdws);

        _init_replicated_dbs();

        std::string db_table_change_channel = fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, _db_instance_id);
        _data_sub_thread.add_subscriber(db_table_change_channel,
            [this]() {
                this->_init_db_tables_subscriber();
            },
            [this](const std::string &msg) {
                _handle_db_table_change(msg);
            });

        // start redis subscriber thread
        _data_sub_thread.start();
        start_thread();
    }

    void DatabaseMgr::_handle_db_table_change(const std::string &msg)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Received DB table change: {}", msg);
        uint64_t db_id;
        std::string action;
        std::string schema;
        std::string table;
        RedisDbTables::decode_pubsub_msg(msg, db_id, action, schema, table);

        // find the database object
        std::shared_lock db_lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return;
        }

        // update the schema table map in the database object
        if (action == "add") {
            iter->second->add_schema_table(schema, table);
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Added schema: {}, table: {} to database {}", schema, table, db_id);
        } else if (action == "remove") {
            iter->second->remove_schema_table(schema, table);
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Removed schema: {}, table: {} from database {}", schema, table, db_id);
        } else {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Unsupported action: {}", action);
        }
    }

    void
    DatabaseMgr::_init_db_tables_subscriber()
    {
        // get all schemas and tables from redis
        std::shared_lock db_lock(_db_mutex);
        for (const auto &[db_id, db_object]: _db_id_rep_dbs) {
            std::vector<std::pair<std::string, std::string>> schema_table_pairs;
            // blocking redis call, but we are in init, so should be ok
            RedisDbTables::get_tables(_db_instance_id, db_id, schema_table_pairs);
            db_object->add_schema_tables(schema_table_pairs);
        }
    }

    void
    DatabaseMgr::_init_replicated_dbs()
    {
        std::map<uint64_t, std::string> db_list = Properties::get_databases();
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();

        std::unique_lock db_lock(_db_mutex);
        for (const auto& [db_id, db_name]: db_list) {
            // create database object and insert it into the maps
            DatabasePtr db_object = std::make_shared<Database>(db_id, db_name);
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Added database (id, name): ({}, {})", db_id, db_name);
            _db_name_rep_dbs.insert(std::pair<std::string, DatabasePtr>(db_name, db_object));
            _db_id_rep_dbs.insert(std::pair<uint64_t, DatabasePtr>(db_id, db_object));

            // subscribe to state change notifications per database
            redis_cache->add_callback(
                std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(db_id),
                _cache_watcher_db_states);

            // initialize state
            redis::db_state_change::DBState db_state = redis::db_state_change::get_db_state(db_id);
            db_object->set_state(db_state);

        }
    }

    void
    DatabaseMgr::_add_replicated_database(uint64_t db_id)
    {
        // look at the properties to find the database config
        std::map<uint64_t, std::string> db_list = Properties::get_databases();
        auto iter = db_list.find(db_id);
        if (iter == db_list.end()) {
            return;
        }

        // create new database object
        const std::string &db_name = iter->second;
        DatabasePtr db_object = std::make_shared<Database>(db_id, db_name);

        // add state change notification callback
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(
            std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(db_id),
            _cache_watcher_db_states);

        // set database state
        redis::db_state_change::DBState db_state = redis::db_state_change::get_db_state(db_id);
        db_object->set_state(db_state);

        // update replicated database maps
        std::unique_lock db_lock(_db_mutex);
        _db_name_rep_dbs.insert(std::pair<std::string, DatabasePtr>(db_name, db_object));
        _db_id_rep_dbs.insert(std::pair<uint64_t, DatabasePtr>(db_id, db_object));
        db_lock.unlock();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Added database (id, name): ({}, {})", db_id, db_name);

        // update database schemas and tables
        std::vector<std::pair<std::string, std::string>> schema_table_pairs;
        RedisDbTables::get_tables(_db_instance_id, db_id, schema_table_pairs);
        db_object->add_schema_tables(schema_table_pairs);
    }

    void
    DatabaseMgr::_remove_replicated_database(uint64_t db_id)
    {
        std::unique_lock db_lock(_db_mutex);

        // find in the database map
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return;
        }

        // remove state change notification callback
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(
            std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(db_id),
            _cache_watcher_db_states);

        // get the database name and remove from maps
        std::string db_name = iter->second->get_db_name();
        _db_id_rep_dbs.erase(db_id);
        _db_name_rep_dbs.erase(db_name);

        db_lock.unlock();

        // remove from replica set
        _replica_set->remove_database(db_id);

        // remove from primary set
        _primary_set->remove_database(db_id);

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG1, "Removed database (id, name): ({}, {})", db_id, db_name);
    }

    std::optional<std::string>
    DatabaseMgr::get_any_replicated_db_name() const
    {
        std::shared_lock lock(_db_mutex);

        auto iter = _db_name_rep_dbs.begin();
        if (iter != _db_name_rep_dbs.end()) {
            return iter->first;
        }
        return std::nullopt;
    }

    std::optional<uint64_t>
    DatabaseMgr::get_database_id(const std::string &db_name) const
    {
        std::shared_lock lock(_db_mutex);

        auto iter = _db_name_rep_dbs.find(db_name);
        if (iter != _db_name_rep_dbs.end()) {
            return iter->second->get_db_id();
        }
        return std::nullopt;
    }

    std::optional<std::string>
    DatabaseMgr::get_database_name(const uint64_t db_id) const
    {
        std::shared_lock lock(_db_mutex);

        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter != _db_id_rep_dbs.end()) {
            return iter->second->get_db_name();
        }
        return std::nullopt;
    }

    bool
    DatabaseMgr::is_table_replicated(const uint64_t db_id,
                                     const std::string &schema,
                                     const std::string &table) const
    {
        // if invalid db id, db is not replicated, so return false
        if (db_id == constant::INVALID_DB_ID) {
            return false;
        }

        // get the database object by db_id
        std::shared_lock lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return false;
        }
        DatabasePtr db_object = iter->second;
        lock.unlock();

        return db_object->has_table(table, schema.empty() ? std::nullopt : std::optional<std::string>{schema});
    }

    bool
    DatabaseMgr::is_database_ready(uint64_t db_id) const
    {
        if (db_id == constant::INVALID_DB_ID) {
            return false;
        }

        std::shared_lock lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return false;
        }
        DatabasePtr db_object = iter->second;
        lock.unlock();

        // this may be a blocking redis call, so we unlock above
        return (db_object->get_state() == redis::db_state_change::DB_STATE_RUNNING);
    }

    void
    DatabaseMgr::_internal_shutdown()
    {
        // deregister callback for add/remove replicated database
        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->remove_callback(Properties::DATABASE_IDS_PATH, _cache_watcher_db_ids);

        // deregister callbacks for replicated database state change
        std::shared_lock<std::shared_mutex> lock(_db_mutex);
        for (auto &[db_id, rep_db]: _db_id_rep_dbs) {
            redis_cache->remove_callback(
                std::string(Properties::DATABASE_STATE_PATH) + "/" + std::to_string(db_id),
                _cache_watcher_db_states);
        }
        lock.unlock();

        _data_sub_thread.shutdown();
    }

    void
    DatabaseMgr::_internal_run()
    {
        while (!_is_shutting_down()) {
            if (_primary_set != nullptr) {
                _primary_set->release_expired_sessions();
            }
            if (_replica_set != nullptr) {
                _replica_set->release_expired_sessions();
            }
            sleep(5);
        }
    }

} // namespace springtail::pg_proxy
