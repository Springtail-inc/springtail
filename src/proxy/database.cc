#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>

#include <proxy/client_session.hh>
#include <proxy/database.hh>
#include <proxy/exception.hh>
#include <proxy/logging.hh>

#include <redis/redis_db_tables.hh>

namespace springtail::pg_proxy
{

    /*********** Database Pool *************/
    void
    DatabasePool::add_session(ServerSessionPtr session)
    {
        SessionKey key = std::make_pair(session->database_id(), session->username());
        std::unique_lock lock(_mutex);
        _insert(key, session);
    }

    ServerSessionPtr
    DatabasePool::get_session(uint64_t db_id, const std::string &username)
    {
        SessionKey key = std::make_pair(db_id, username);
        std::unique_lock lock(_mutex);
        return _get(key);
    }

    void
    DatabasePool::evict(uint64_t db_id)
    {
        std::unique_lock lock(_mutex);
        auto db_it = _lookup.lower_bound({db_id, ""});
        while (db_it != _lookup.end() && db_it->first.first == db_id) {
            auto &queue = db_it->second;
            for (auto &cache_it: queue) {
                _cache.erase(cache_it);
            }
            queue.clear();
            db_it = _lookup.erase(db_it);
        }
    }

    void
    DatabasePool::evict_expired_sessions()
    {
        std::unique_lock lock(_mutex);
        uint64_t now = common::get_time_in_millis() / 1000;
        // cleanup based on the cache entry timestamp
        if (_expiration_interval != 0) {
            while (_cache.size() > _timeout_limit) {
                // get last entry
                SessionEntry &cache_entry = _cache.back();

                if (cache_entry.expiration_time < now) {
                    _remove_entry(cache_entry.key);
                } else {
                    break;
                }
            }
        }
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

    void
    DatabasePool::_remove_entry(const SessionKey &key)
    {
        // remove from hashmap
        auto it = _lookup.find(key);
        // check that _lookup map has the key
        DCHECK(it != _lookup.end());
        // check that deque for this key is not empty
        DCHECK(!(it->second.empty()));
        // check that the last element in deque points to the last element in cache
        DCHECK(it->second.back() == (--_cache.end()));
        // remove entry from _lookup
        it->second.pop_back();

        // remove entry from cache
        _cache.pop_back();
    }

    void
    DatabasePool::_evict_next()
    {
        // get last entry
        SessionEntry &cache_entry = _cache.back();
        _remove_entry(cache_entry.key);
    }

    void
    DatabasePool::_insert(const SessionKey &key, ServerSessionWeakPtr value)
    {
        uint64_t now = common::get_time_in_millis() / 1000;
        if (!_cache.empty()) {
            if (_size_limit != 0  && _cache.size() == _size_limit) {
                _evict_next();
            }
        }

        auto it = _lookup.find(key);
        if (it == _lookup.end()) {
            auto result = _lookup.emplace(key, std::deque<typename std::list<SessionEntry>::iterator>());
            DCHECK(result.second);
            it = result.first;
        }
        uint64_t expiration_time = std::numeric_limits<uint64_t>::max();
        if (_expiration_interval != 0) {
            expiration_time = now + _expiration_interval;
        }
        SessionEntry entry = {key, value, expiration_time};
        _cache.emplace_front(entry);
        it->second.push_front(_cache.begin());
    }

    ServerSessionPtr
    DatabasePool::_get(const SessionKey &key)
    {
        auto it = _lookup.find(key);
        if (it == _lookup.end() || it->second.empty()) {
            return nullptr;
        }
        auto &&cache_it = it->second.front();
        SessionEntry &entry = *(cache_it);
        _cache.erase(cache_it);
        it->second.pop_front();
        return entry.value.lock();
    }

    /*********** Database Set *************/

    void
    DatabaseSet::_remove_instance(DatabaseInstancePtr instance)
    {
        // remove from sessions map
        auto instance_it = _sessions.find(instance);
        if (instance_it != _sessions.end()) {
            _sessions.erase(instance_it);
        }

        // remove from instance sessions map
        auto session_it = _instance_sessions.find(instance);
        if (session_it != _instance_sessions.end()) {
            _instance_sessions.erase(session_it);
        }

        // XXX Not handling removing instance with in-use sessions
        // those should be handled by the session release
    }

    void
    DatabaseSet::remove_database(uint64_t db_id)
    {
        std::unique_lock lock(_base_mutex);

        // remove from the session map, iterate over instances
        for (auto instance_it = _sessions.begin(); instance_it != _sessions.end();) {
            // evict database from the pool
            instance_it->first->get_pool()->evict(db_id);

            // cleanup the rest
            auto& db_map = instance_it->second;

            // Find the database ID in the inner map and remove
            auto db_it = db_map.find(db_id);
            int num_sessions = db_it->second.size();
            if (db_it != db_map.end()) {
                db_map.erase(db_it);
            }

            // If the db map is empty after removal, erase the instance entry
            if (db_map.empty()) {
                // first remove the instance from the instance sessions map (count)
                auto session_itr = _instance_sessions.find(instance_it->first);
                if (session_itr != _instance_sessions.end()) {
                    _instance_sessions.erase(session_itr);
                }

                // then remove the instance from the sessions map
                instance_it = _sessions.erase(instance_it);
            } else {
                // decrement the session count for the instance
                auto session_itr = _instance_sessions.find(instance_it->first);
                if (session_itr != _instance_sessions.end()) {
                    session_itr->second -= num_sessions;
                }
                // move to the next instance
                ++instance_it;
            }
        }
    }

    void
    DatabaseSet::_release_session(ServerSessionPtr session,
                                  int num_instances,
                                  bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Session being released: [S:{:d}]", session->id());

        // deallocate if connection is closed or database id is invalid
        if (session->is_connection_closed() || session->database_id() == constant::INVALID_DB_ID) {
            deallocate = true;
        }

        if (!deallocate) {
            DatabasePoolPtr pool = session->get_instance()->get_pool();
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Adding session back to pool: [S:{:d}]", session->id());
            pool->add_session(session);
            return;
        }

        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Deallocating session: [S:{:d}]", session->id());

        // otherwise, remove from the internal maps and deallocate
        auto instance_it = _sessions.find(session->get_instance());
        if (instance_it != _sessions.end()) {
            // remove from the db session map; get the list of sessions for the db_id
            auto db_it = instance_it->second.find(session->database_id());

            // remove from list if it exists
            if (db_it != instance_it->second.end()) {
                auto &list = db_it->second;
                list.remove_if([session](const ServerSessionWeakPtr &s) {
                    auto s_ptr = s.lock();
                    return (s_ptr == nullptr) || (session == s_ptr);
                });
            }
        }

        // update count in _instance_sessions map
        auto session_itr = _instance_sessions.find(session->get_instance());
        if (session_itr != _instance_sessions.end()) {
            session_itr->second--;
        }

        session.reset();
    }

    ServerSessionPtr
    DatabaseSet::_allocate_session(UserPtr user,
                                   uint64_t db_id,
                                   const std::unordered_map<std::string, std::string> &parameters,
                                   DatabaseInstancePtr instance,
                                   const std::string &database)
    {
        // create a new session from instance
        auto session = instance->allocate_session(user, db_id, parameters, database);

        // add session to instance map
        _sessions[instance][db_id].push_back(session);

        // incr count of sessions for instance
        _instance_sessions[instance]++;

        return session;
    }

    DatabaseInstancePtr
    DatabaseSet::_get_least_loaded_instance()
    {
        std::shared_lock lock(_base_mutex);

        if (_instance_sessions.empty()) {
            return nullptr;
        }

        // find the instance with the least number of sessions
        DatabaseInstancePtr instance = nullptr;
        int min_sessions = INT_MAX;
        for (auto &it : _instance_sessions) {
            int num_sessions = it.second;
            if (num_sessions < min_sessions) {
                min_sessions = num_sessions;
                instance = it.first;
            }
        }

        return instance;
    }

    /*********** Database Replica Set *************/

    void
    DatabaseReplicaSet::remove_replica(DatabaseInstancePtr replica)
    {
        std::unique_lock lock(_base_mutex);

        // Remove from list of replicas
        _replicas.erase(replica);

        // remove from the database set
        DatabaseSet::_remove_instance(replica);
    }

    void
    DatabaseReplicaSet::release_session(ServerSessionPtr session, bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Replica session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::REPLICA);

        std::shared_lock lock(_base_mutex);

        // check if replica instance is still alive, if so try to add back to pool
        if (_replicas.find(session->get_instance()) == _replicas.end()) {
            deallocate = true;
        }

        DatabaseSet::_release_session(session, _replicas.size(), deallocate);
    }

    void
    DatabaseReplicaSet::release_expired_sessions()
    {
        std::shared_lock lock(_base_mutex);
        for (auto replica : _replicas) {
            auto pool = replica->get_pool();
            pool->evict_expired_sessions();
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
        if (instance == nullptr && !_replicas.empty()) {
            instance = *_replicas.begin();
        }

        if (instance == nullptr) {
            return nullptr;
        }

        return _allocate_session(user, db_id, parameters, instance, database);
    }

    /*********** Database Primary Set *************/

    void
    DatabasePrimarySet::release_session(ServerSessionPtr session, bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Primary Session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::PRIMARY);

        std::shared_lock lock(_base_mutex);

        // check if primary instance is still alive, if so try to add to pool
        if (session->get_instance() != _primary) {
            deallocate = true;
        }

        DatabaseSet::_release_session(session, 1, deallocate);
    }

    void
    DatabasePrimarySet::release_expired_sessions()
    {
        std::shared_lock lock(_base_mutex);
        if (_primary != nullptr) {
            _primary->get_pool()->evict_expired_sessions();
        }
        if (_standby != nullptr) {
            _standby->get_pool()->evict_expired_sessions();
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
        auto session = DatabaseSet::_allocate_session(user, db_id, parameters, instance, database);

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
        return ServerSession::create(user, db_name.value_or(database), prefix(), shared_from_this(), _type, parameters);
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
        _data_sub_thread(1, false),
        _primary_set(std::make_shared<DatabasePrimarySet>(POOL_SESSIONS_PER_INSTANCE)),
        _replica_set(std::make_shared<DatabaseReplicaSet>(POOL_SESSIONS_PER_INSTANCE))
    {
        _cache_watcher_db_ids = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PROXY,"Replicated databases: {}", new_value.dump(4));
                CHECK_EQ(path, Properties::DATABASE_IDS_PATH);
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
        );
        _cache_watcher_db_states = std::make_shared<RedisCache::RedisChangeWatcher>(
            [this](const std::string &path, const nlohmann::json &new_value) -> void {
                LOG_DEBUG(LOG_PROXY,"Replicated database state change; path: {}, state: {}",
                    path, new_value.dump(4));
                CHECK(path.starts_with(Properties::DATABASE_STATE_PATH));
                // extract database id
                std::vector<std::string> path_parts;
                common::split_string("/", path, path_parts);
                CHECK_EQ(path_parts.size(), 2);
                uint64_t db_id = stoull(path_parts[1]);

                // if we ever get in here, this means that this database will be deleted
                if (new_value.type() == nlohmann::json::value_t::null) {
                    return;
                }

                // extract state
                CHECK(new_value.type() == nlohmann::json::value_t::string);
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
        );
        _init();
    }

    void
    DatabaseMgr::_init()
    {
        nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
        size_t pool_size_limit = Json::get_or<size_t>(json, "pool_size_limit", 0);
        size_t pool_timeout_limit = Json::get_or<size_t>(json, "pool_timeout_limit", 0);
        uint64_t pool_expiration_interval = Json::get_or<size_t>(json, "pool_expiration_interval_secs", 0);
        if (pool_timeout_limit != 0 && pool_expiration_interval == 0) {
            LOG_ERROR("Pool timeout limit is set to {} while expiration interval is not defined", pool_timeout_limit);
            throw ProxyServerError();
        }

        // add primary
        uint64_t primary_instance_id = Properties::get_db_instance_id();
        std::string host, user, password;
        int port;
        Properties::get_primary_db_config(host, port, user, password);
        set_primary(primary_instance_id, std::make_shared<DatabaseInstance>(pool_size_limit, pool_timeout_limit, pool_expiration_interval, Session::Type::PRIMARY, host, "", port));

        std::vector<std::string> fdw_id_list = Properties::get_fdw_ids();
        for (const auto & fdw_id: fdw_id_list) {
            nlohmann::json fdw_config = Properties::get_fdw_config(fdw_id);
            auto host = Json::get<std::string>(fdw_config, "host");
            auto port = Json::get<uint16_t>(fdw_config, "port");
            auto db_prefix = Json::get<std::string>(fdw_config, "db_prefix");
            if (host.has_value() && port.has_value()) {
                // add replica
                if (!db_prefix.has_value()) {
                    db_prefix = "";
                }
                add_replica(std::make_shared<DatabaseInstance>(pool_size_limit, pool_timeout_limit, pool_expiration_interval, Session::Type::REPLICA, host.value(), db_prefix.value(), port.value()));
            } else {
                LOG_ERROR("Could not find the value for replica database {} either host or port", fdw_id);
                throw ProxyServerError();
            }
        }

        std::shared_ptr<RedisCache> redis_cache = Properties::get_instance()->get_cache();
        redis_cache->add_callback(Properties::DATABASE_IDS_PATH, _cache_watcher_db_ids);

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
        LOG_DEBUG(LOG_PROXY, "Received DB table change: {}", msg);
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
            LOG_DEBUG(LOG_PROXY, "Added schema: {}, table: {} to database {}", schema, table, db_id);
        } else if (action == "remove") {
            iter->second->remove_schema_table(schema, table);
            LOG_DEBUG(LOG_PROXY, "Removed schema: {}, table: {} from database {}", schema, table, db_id);
        } else {
            LOG_DEBUG(LOG_PROXY, "Unsupported action: {}", action);
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
            LOG_DEBUG(LOG_PROXY, "Added database (id, name): ({}, {})", db_id, db_name);
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

        LOG_DEBUG(LOG_PROXY, "Added database (id, name): ({}, {})", db_id, db_name);

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
        for (auto [db_id, rep_db]: _db_id_rep_dbs) {
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
