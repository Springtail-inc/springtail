#include <mutex>
#include <shared_mutex>
#include <memory>
#include <format>

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

namespace springtail::pg_proxy {

    ServerSessionPtr
    DatabaseInstance::evict_session() {
        std::unique_lock lock(_mutex);
        return _internal_evict_session();
    }

    DatabasePoolPtr
    DatabaseInstance::get_pool(const uint64_t db_id,
                               const std::string &username) const
    {
        std::shared_lock lock(_mutex);
        // get size of pool based on dbname and username
        auto it = _sessions.find({db_id, username});
        if (it == _sessions.end()) {
            return nullptr;
        }
        return it->second;
    }

    /**
     * @brief Get a free session from the db instance (and associated pool).
     * Removes session from LRU list (so it can't be evicted), incr active count.
     * @param db_id database id
     * @param username username
     * @return ServerSessionPtr
     */
    ServerSessionPtr
    DatabaseInstance::get_session(const uint64_t db_id,
                                    const std::string &username)
    {
        std::unique_lock lock(_mutex);
        return _internal_get_session(db_id, username);
    }

    void
    DatabaseInstance::release_session(ServerSessionPtr session)
    {
        std::unique_lock lock(_mutex);
        auto it = _sessions.find({session->database_id(), session->username()});
        if (it == _sessions.end()) {
            SPDLOG_WARN("Database pool not found for: {}:{}", session->database(), session->username());
            return;
        }

        it->second->release_session(session);
        _active_sessions--;
        assert(_active_sessions >= 0);
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Session released: {:d}, active={}", session->id(), _active_sessions);
    }

    void
    DatabaseInstance::delete_session(ServerSessionPtr session)
    {
        std::unique_lock lock(_mutex);

        auto it = _sessions.find({session->database_id(), session->username()});
        if (it != _sessions.end()) {
            it->second->delete_session(session);
        }

        // find session in LRU list and remove
        _sessions_lru.remove(session);
    }

    ServerSessionPtr
    DatabaseInstance::allocate_session(ProxyServerPtr server,
                                       UserPtr user,
                                       const std::string &database)
    {
        std::unique_lock lock(_mutex);

        auto db_id = DatabaseMgr::get_instance()->get_database_id(database);
        if (!db_id.has_value()) {
            return nullptr;
        }

        // try to get a free one first
        ServerSessionPtr session = _internal_get_session(db_id.value(), user->username());
        if (session != nullptr) {
            return session;
        }

        // if no free sessions, see if eviction is required
        if (_active_sessions >= _max_sessions) {
            session = _internal_evict_session();
            if (session != nullptr) {
                session->shutdown();
            } else {
                assert(0); // XXX need to handle this case, need to wait for a session to be released
            }
        }

        // we will create a new session in a moment, however creating a session requires creating a connection
        // which is a blocking activity. So we will release the lock and create the session.
        // however before releasing the lock we need to reserve space for the new session in the pool

        // create a db pool if one doesn't exist
        auto it = _sessions.find({db_id.value(), user->username()});
        DatabasePoolPtr pool;
        if (it == _sessions.end()) {
            pool = std::make_shared<DatabasePool>();
            _sessions[{db_id.value(), user->username()}] = pool;
        } else {
            pool = it->second;
        }

        // incr active count on pool
        pool->reserve_session();

        // incr active count on instance
        _active_sessions++;

        // unlock and then create the new session
        lock.unlock();

        // create a new session
        session = ServerSession::create(server, user, database, shared_from_this(), _type);

        return session;
    }

    ServerSessionPtr
    DatabaseInstance::_internal_get_session(const uint64_t db_id,
                                            const std::string &username)
    {
        // lock must be held
        auto it = _sessions.find({db_id, username});
        if (it == _sessions.end()) {
            return nullptr;
        }
        ServerSessionPtr session = it->second->get_session();
        if (session == nullptr) {
            return nullptr;
        }

        // find the session in LRU list and remove it
        _sessions_lru.remove(session);
        _active_sessions++;
        return session;
    }

    ServerSessionPtr
    DatabaseInstance::_internal_evict_session()
    {
        // lock must be held
        if (_sessions_lru.empty()) {
            return nullptr;
        }

        ServerSessionPtr session = _sessions_lru.back();
        _sessions_lru.pop_back();

        // find the session in the pool and release it
        auto it = _sessions.find({session->database_id(), session->username()});
        assert (it != _sessions.end());
        if (it != _sessions.end()) {
            it->second->delete_session(session);
        }

        return session;
    }

    void DatabaseMgr::init()
    {
        // add primary
        uint64_t primary_instance_id = Properties::get_db_instance_id();
        std::string host, user, password;
        int port;
        Properties::get_primary_db_config(host, port, user, password);
        set_primary(primary_instance_id, std::make_shared<DatabaseInstance>(Session::Type::PRIMARY, host, port));

        std::vector<std::string> fdw_id_list = Properties::get_fdw_ids();
        for (const auto & fdw_id: fdw_id_list) {
            nlohmann::json fdw_config = Properties::get_fdw_config(fdw_id);
            auto host = Json::get<std::string>(fdw_config, "host");
            auto port = Json::get<uint16_t>(fdw_config, "port");
            if (host.has_value() && port.has_value()) {
                // add replica
                add_replica(std::make_shared<DatabaseInstance>(Session::Type::REPLICA, host.value(), port.value()));
            } else {
                SPDLOG_ERROR("Could not find the value for replica database {} either host or port", fdw_id);
                throw ProxyServerError();
            }
        }

        // add replicated databases
        std::map<uint64_t, std::string> db_list = Properties::get_databases();
        for (const auto& db_pair: db_list) {
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Database (id, name): ({}, {})", get<0>(db_pair), get<1>(db_pair));
            add_replicated_database(std::get<0>(db_pair), std::get<1>(db_pair));
        }

        Counter init_counter(2);

        // add subscribers to pubsub threads
        std::string state_change_channel = fmt::format(redis::PUBSUB_DB_STATE_CHANGES, _db_instance_id);
        _config_sub_thread.add_subscriber(state_change_channel,
            [this, &init_counter]() {
                this->_init_db_states_subscriber();
                init_counter.decrement();
            },
            [this](const std::string &msg) {
                _handle_db_state_change(msg);
            });
        std::string db_table_change_channel = fmt::format(redis::PUBSUB_DB_TABLE_CHANGES, _db_instance_id);
        _data_sub_thread.add_subscriber(db_table_change_channel,
            [this, &init_counter]() {
                this->_init_db_tables_subscriber();
                init_counter.decrement();
            },
            [this](const std::string &msg) {
                _handle_db_table_change(msg);
            });

        // start redis subscriber threads
        _config_sub_thread.start();
        _data_sub_thread.start();

        init_counter.wait();
    }

    void DatabaseMgr::_handle_db_state_change(const std::string &msg)
    {
        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Received state change: {}", msg);
        uint64_t db_id;
        redis::db_state_change::DBState state;
        redis::db_state_change::parse_db_state_change(msg, db_id, state);
        std::unique_lock db_state_lock(_db_state_mutex);
        _replicated_database_states[db_id] = state;
    }

    void DatabaseMgr::_init_db_states_subscriber()
    {
        // refresh all database states
        std::shared_lock db_name_lock(_db_mutex);
        std::unique_lock db_state_lock(_db_state_mutex);
        for (const auto &[db_name, db_id]: _replicated_databases) {
            redis::db_state_change::DBState db_state = redis::db_state_change::get_db_state(db_id);
            _replicated_database_states[db_id] = db_state;
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
        if (action == "add") {
            _schema_tables.add_item(db_id, schema, table);
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Added schema: {}, table: {} to database {}", schema, table, db_id);
        } else if (action == "remove") {
            _schema_tables.remove_item(db_id, schema, table);
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Removed schema: {}, table: {} from database {}", schema, table, db_id);
        }
    }

    void DatabaseMgr::_init_db_tables_subscriber()
    {
        // get all schemas and tables from redis
        std::shared_lock db_name_lock(_db_mutex);
        for (const auto &[db_name, db_id]: _replicated_databases) {
            std::vector<std::pair<std::string, std::string>> schema_table_pairs;
            RedisDbTables::get_tables(_db_instance_id, db_id, schema_table_pairs);
            for (const auto &[schema, table]: schema_table_pairs) {
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Found schema: {}, table : {}", schema, table);
                _schema_tables.add_item(db_id, schema, table);
            }
        }
    }

    void DatabaseMgr::_internal_shutdown()
    {
        _config_sub_thread.shutdown();
        _data_sub_thread.shutdown();
    }

} // namespace springtail::pg_proxy
