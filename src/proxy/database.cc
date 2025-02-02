#include <mutex>
#include <shared_mutex>
#include <memory>
#include <format>
#include <unordered_map>

#include <common/counter.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>

#include <proxy/database.hh>
#include <proxy/exception.hh>
#include <proxy/session.hh>
#include <proxy/client_session.hh>
#include <proxy/server_session.hh>
#include <proxy/logging.hh>

namespace springtail::pg_proxy
{

    /*********** Database Pool *************/

    void
    DatabasePool::evict(uint64_t db_id)
    {
        std::unique_lock lock(_mutex);

        // remove from free session map for whole db_id
        auto db_it = _free_sessions.lower_bound({db_id, ""});
        while (db_it != _free_sessions.end() && db_it->first.first == db_id) {
            auto list = db_it->second;
            count -= list.size();
            db_it = _free_sessions.erase(db_it);
        }
    }

    void
    DatabasePool::evict(ServerSessionPtr session)
    {
        std::unique_lock lock(_mutex);

        // remove from db session map
        uint64_t db_id = session->database_id();
        std::string username = session->username();

        // lookup db_id and username in free session map
        auto db_it = _free_sessions.find({db_id, username});
        auto list = db_it->second;

        // remove session from list if it exists, also prune any null sessions
        auto start_count = list.size();
        list.remove_if([session](const ServerSessionWeakPtr &s) {
            return (s.lock() == session) || (s.lock() == nullptr);
        });

        // update count based on the difference in list size
        count -= (start_count - list.size());

        // remove db_id and username if list is empty
        if (list.empty()) {
            _free_sessions.erase(db_it);
        }
    }

    void
    DatabasePool::evict(DatabaseInstancePtr instance)
    {
        std::unique_lock lock(_mutex);

        // remove from free sessions map for all sessions that have an instance that matches
        auto db_it = _free_sessions.begin();
        while (db_it != _free_sessions.end()) {
            auto &list = db_it->second;
            auto size = list.size();

            list.remove_if([instance](const ServerSessionWeakPtr &s) {
                auto session = s.lock();
                if ((session != nullptr) && (session->get_instance() == instance)) {
                    PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Evicting session from pool: [S:{:d}], db_id: {}, username: {}",
                                session->id(), session->database_id(), session->username());
                    PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Instance: {}", session->get_instance()->to_string());
                } else {
                    PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Session not evicted from pool: [S:{:d}]", session->id());
                    PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Instance: {}", session->get_instance()->to_string());
                }
                return (session == nullptr) || (session->get_instance() == instance);
            });

            // update count based on the difference in list size
            count -= (size - list.size());

            // remove db_id and username if list is empty
            if (list.empty()) {
                db_it = _free_sessions.erase(db_it);
            } else {
                db_it++;
            }
        }
    }

    void
    DatabasePool::add_session(ServerSessionPtr session)
    {
        uint64_t db_id = session->database_id();
        std::string username = session->username();

        std::unique_lock lock(_mutex);
        // look up db_id and username in session map
        auto it = _free_sessions.find({db_id, username});
        if (it != _free_sessions.end()) {
            it->second.push_front(session);
        } else {
            std::list<ServerSessionWeakPtr> sessions;
            sessions.push_front(session);
            _free_sessions[{db_id, username}] = sessions;
        }
        count++;
    }

    ServerSessionPtr
    DatabasePool::get_session(uint64_t db_id,
                              const std::string &username)
    {
        std::unique_lock lock(_mutex);

        // look up db_id and username in session map
        auto it = _free_sessions.find({db_id, username});
        if (it == _free_sessions.end()) {
            return nullptr;
        }

        if (it->second.empty()) {
            return nullptr;
        }

        ServerSessionPtr session = nullptr;
        while (session == nullptr && !it->second.empty()) {
            session = it->second.front().lock();
            it->second.pop_front();
            count--;
        }

        if (session != nullptr) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Session retrieved from pool: [S:{:d}]", session->id());
            assert(session->database_id() == db_id);
            assert(session->username() == username);
        }

        return session;
    }

    /*********** Database Set *************/

    void
    DatabaseSet::remove_instance(DatabaseInstancePtr instance)
    {
        std::unique_lock lock(_mutex);

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

        // evict from pool
        _pool->evict(instance);

        // XXX Not handling removing instance with in-use sessions
        // those should be handled by the session release
    }

    void
    DatabaseSet::remove_database(uint64_t db_id)
    {
        std::unique_lock lock(_mutex);

        // remove from the session map, iterate over instances
        for (auto instance_it = _sessions.begin(); instance_it != _sessions.end();) {
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

        // evict from the pool
        _pool->evict(db_id);
    }

    void
    DatabaseSet::_release_session(ServerSessionPtr session,
                                  int num_instances,
                                  bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Session being released: [S:{:d}]", session->id());

        // deallocate if connection is closed
        if (session->is_connection_closed()) {
            deallocate = true;
        }

        std::unique_lock lock(_mutex);

        // if not deallocating the session then add it back to the pool
        if (!deallocate && (num_instances * _max_sessions_per_instance > _pool->size())) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Adding session back to pool: [S:{:d}]", session->id());
            _pool->add_session(session);
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
                                   DatabaseInstancePtr instance)
    {
        // create a new session from instance
        auto session = instance->allocate_session(user, db_id, parameters);

        std::unique_lock lock(_mutex); // lock after getting the session, since it is blocking

        // add session to instance map
        _sessions[instance][db_id].push_back(session);

        // incr count of sessions for instance
        _instance_sessions[instance]++;

        return session;
    }

    DatabaseInstancePtr
    DatabaseSet::_get_least_loaded_instance()
    {
        std::shared_lock lock(_mutex);

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
        std::unique_lock lock(_mutex);

        // Remove from list of replicas
        _replicas.erase(replica);

        lock.unlock();

        // remove from the database set
        DatabaseSet::remove_instance(replica);
    }

    void
    DatabaseReplicaSet::release_session(ServerSessionPtr session, bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Replica session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::REPLICA);

        std::shared_lock lock(_mutex);

        // check if replica instance is still alive, if so try to add back to pool
        if (_replicas.find(session->get_instance()) == _replicas.end()) {
            deallocate = true;
        }

        DatabaseSet::_release_session(session, _replicas.size(), deallocate);
    }


    ServerSessionPtr
    DatabaseReplicaSet::allocate_session(UserPtr user,
                                         uint64_t db_id,
                                         const std::unordered_map<std::string, std::string> &parameters)
    {
        std::shared_lock lock(_mutex);

        auto instance = _get_least_loaded_instance();
        if (instance == nullptr && !_replicas.empty()) {
            instance = *_replicas.begin();
        }
        lock.unlock();

        if (instance == nullptr) {
            return nullptr;
        }

        return _allocate_session(user, db_id, parameters, instance);
    }

    /*********** Database Primary Set *************/

    void
    DatabasePrimarySet::release_session(ServerSessionPtr session, bool deallocate)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Primary Session released: [S:{:d}]", session->id());
        assert(session->type() == Session::Type::PRIMARY);

        std::shared_lock lock(_mutex);

        // check if primary instance is still alive, if so try to add to pool
        if (session->get_instance() != _primary) {
            deallocate = true;
        }

        DatabaseSet::_release_session(session, 1, deallocate);
    }

    ServerSessionPtr
    DatabasePrimarySet::allocate_session(UserPtr user,
                                         uint64_t db_id,
                                         const std::unordered_map<std::string, std::string> &parameters)
    {
        std::shared_lock lock(_mutex);

        // get primary instance
        auto instance = _primary;
        if (instance == nullptr) {
            return nullptr;
        }
        lock.unlock();

        // allocate session
        auto session = DatabaseSet::_allocate_session(user, db_id, parameters, instance);

        return session;
    }

    /*********** Database Instance *************/

    ServerSessionPtr
    DatabaseInstance::allocate_session(UserPtr user,
                                       uint64_t db_id,
                                       const std::unordered_map<std::string, std::string> &parameters)
    {
        auto db_name = DatabaseMgr::get_instance()->get_database_name(db_id);
        if (!db_name.has_value()) {
            return nullptr;
        }

        // create a new session; this is a blocking activity as it requires creating a connection
        return ServerSession::create(user, db_name.value(), prefix(), shared_from_this(), _type, parameters);
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

        // lookup schema in schema_tables map
        auto schema_it = _schema_tables_map.find(db_schema);
        if (schema_it == _schema_tables_map.end()) {
            // doesn't exist, create new entry
            std::set<std::string> empty_schema;
            _schema_tables_map.insert(std::pair(db_schema, empty_schema));
            schema_it = _schema_tables_map.find(db_schema);
        }

        auto &schema = schema_it->second;
        schema.insert(db_table);
    }

    void
    Database::remove_schema_table(const std::string &db_schema, const std::string &db_table)
    {
        std::unique_lock lock(_db_mutex);

        auto schema_it = _schema_tables_map.find(db_schema);
        if (schema_it == _schema_tables_map.end()) {
            // doesn't exist
            return;
        }

        auto &schema = schema_it->second;
        schema.erase(db_table);
        if (schema.empty()) {
            // remove schema if schema is now empty
            _schema_tables_map.erase(db_schema);
        }
    }

    bool
    Database::has_schema_table(const std::string &db_schema, const std::string &db_table) const
    {
        std::shared_lock lock(_db_mutex);

        // lookup schema
        auto schema_it = _schema_tables_map.find(db_schema);
        if (schema_it == _schema_tables_map.end()) {
            return false;
        }

        // lookup table
        auto &schema = schema_it->second;
        if (schema.contains(db_table)) {
            return true;
        }

        return false;
    }

    /*********** Database Mgr *************/

    void
    DatabaseMgr::init()
    {
        // add primary
        uint64_t primary_instance_id = Properties::get_db_instance_id();
        std::string host, user, password;
        int port;
        Properties::get_primary_db_config(host, port, user, password);
        set_primary(primary_instance_id, std::make_shared<DatabaseInstance>(Session::Type::PRIMARY, host, "", port));

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
                add_replica(std::make_shared<DatabaseInstance>(Session::Type::REPLICA, host.value(), db_prefix.value(), port.value()));
            } else {
                SPDLOG_ERROR("Could not find the value for replica database {} either host or port", fdw_id);
                throw ProxyServerError();
            }
        }

        // add subscribers to pubsub threads
        std::string db_change_channel = fmt::format(redis::PUBSUB_DB_CONFIG_CHANGES, _db_instance_id);
        _config_sub_thread.add_subscriber(db_change_channel,
            [this]() {
                this->_init_replicated_dbs_subscriber();
            },
            [this](const std::string &msg) {
                _handle_replicated_dbs_change(msg);
            });

        std::string state_change_channel = fmt::format(redis::PUBSUB_DB_STATE_CHANGES, _db_instance_id);
        _config_sub_thread.add_subscriber(state_change_channel,
            [this]() {
                this->_init_db_states_subscriber();
            },
            [this](const std::string &msg) {
                _handle_db_state_change(msg);
            });

        std::string db_table_change_channel = fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, _db_instance_id);
        _data_sub_thread.add_subscriber(db_table_change_channel,
            [this]() {
                this->_init_db_tables_subscriber();
            },
            [this](const std::string &msg) {
                _handle_db_table_change(msg);
            });

        // start redis subscriber threads
        _config_sub_thread.start();
        _data_sub_thread.start();
    }

    void
    DatabaseMgr::_handle_db_state_change(const std::string &msg)
    {
        uint64_t db_id;

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Received state change: {}", msg);

        redis::db_state_change::DBState state;
        redis::db_state_change::parse_db_state_change(msg, db_id, state);

        // shared lock for lookup
        std::shared_lock lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return;
        }

        iter->second->set_state(state);
    }

    void DatabaseMgr::_init_db_states_subscriber()
    {
        // refresh all database states; shared lock for lookup
        std::shared_lock lock(_db_mutex);
        for (const auto &[db_id, db_object]: _db_id_rep_dbs) {
            // this is a blocking redis call, but we are in init, so should be ok
            redis::db_state_change::DBState db_state = redis::db_state_change::get_db_state(db_id);
            db_object->set_state(db_state);
        }
    }

    void DatabaseMgr::_handle_db_table_change(const std::string &msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Received DB table change: {}", msg);
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
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Added schema: {}, table: {} to database {}", schema, table, db_id);
        } else if (action == "remove") {
            iter->second->remove_schema_table(schema, table);
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Removed schema: {}, table: {} from database {}", schema, table, db_id);
        } else {
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Unsupported action: {}", action);
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
    DatabaseMgr::_handle_replicated_dbs_change(const std::string &msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Handling DB Change Action message: {}", msg);
        uint64_t db_id;
        redis::db_state_change::DBAction action;
        redis::db_state_change::parse_db_action(msg, db_id, action);
        switch (action) {
            case redis::db_state_change::DB_ACTION_ADD:
                _add_replicated_database(db_id);
                break;
            case redis::db_state_change::DB_ACTION_REMOVE:
                _remove_replicated_database(db_id);
                break;
            default:
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Unsupported action: {}", redis::db_state_change::db_action_to_name[action]);
        }
    }

    void
    DatabaseMgr::_init_replicated_dbs_subscriber()
    {
        std::map<uint64_t, std::string> db_list = Properties::get_databases();

        std::unique_lock db_lock(_db_mutex);
        for (const auto& [db_id, db_name]: db_list) {
            // create database object and insert it into the maps
            DatabasePtr db_object = std::make_shared<Database>(db_id, db_name);
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Added database (id, name): ({}, {})", db_id, db_name);
            _db_name_rep_dbs.insert(std::pair<std::string, DatabasePtr>(db_name, db_object));
            _db_id_rep_dbs.insert(std::pair<uint64_t, DatabasePtr>(db_id, db_object));
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

        // set database state
        redis::db_state_change::DBState db_state = redis::db_state_change::get_db_state(db_id);
        db_object->set_state(db_state);

        // update replicated database maps
        std::unique_lock db_lock(_db_mutex);
        _db_name_rep_dbs.insert(std::pair<std::string, DatabasePtr>(db_name, db_object));
        _db_id_rep_dbs.insert(std::pair<uint64_t, DatabasePtr>(db_id, db_object));
        db_lock.unlock();

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Added database (id, name): ({}, {})", db_id, db_name);

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
                                     const std::string &default_schema,
                                     const std::string &schema,
                                     const std::string &table) const
    {
        // get the database object by db_id
        std::shared_lock lock(_db_mutex);
        auto iter = _db_id_rep_dbs.find(db_id);
        if (iter == _db_id_rep_dbs.end()) {
            return false;
        }
        DatabasePtr db_object = iter->second;
        lock.unlock();

        return db_object->has_schema_table((schema.empty()) ? default_schema : schema, table);
    }

    bool
    DatabaseMgr::is_database_ready(uint64_t db_id) const
    {
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
        _config_sub_thread.shutdown();
        _data_sub_thread.shutdown();
    }

} // namespace springtail::pg_proxy
