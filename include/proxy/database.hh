#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <set>
#include <map>
#include <utility>
#include <list>
#include <climits>

#include <common/logging.hh>

#include <proxy/connection.hh>
#include <proxy/server_session.hh>

namespace springtail {
namespace pg_proxy {

    class DatabasePool {
    public:
        DatabasePool() {}

        /**
         * @brief Add session to pool; marked as free
         * @param session  session to add
         */
        void add_session(ServerSessionPtr session) {
            std::unique_lock lock(_mutex);
            _free_sessions.push_back(session);
        }

        /**
         * @brief Delete a session from pool permanently
         * @param session  session to delete
         */
        void delete_session(ServerSessionPtr session) {
            std::unique_lock lock(_mutex);
            // find session in lru list and remove
            _free_sessions.remove(session);
        }

        /**
         * @brief Get a free session from the pool.  Removes from free list. Increments active count.
         * @returns session or nullptr if pool is empty
         */
        ServerSessionPtr get_session() {
            std::unique_lock lock(_mutex);
            if (_free_sessions.empty()) {
                return nullptr;
            }

            ServerSessionPtr session = _free_sessions.front();
            _free_sessions.pop_front();
            _active_count++;

            return session;
        }

        /**
         * @brief Release a session back into the pool.  Marks as free. Decrements active count.
         * @param session  session to release
         */
        void release_session(ServerSessionPtr session) {
            std::unique_lock lock(_mutex);
            _free_sessions.push_back(session);
            _active_count--;
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Session released: {:d}, active={}", session->id(), _active_count);
        }

        /**
         * @brief Get the size of the pool
         * @returns size of the pool
         */
        int total_count() const {
            std::shared_lock lock(_mutex);
            return _free_sessions.size() + _active_count;
        }

        /**
         * @brief Get the number of active sessions
         * @returns number of active sessions
         */
        int active_count() const {
            std::shared_lock lock(_mutex);
            return _active_count;
        }

        /**
         * @brief Get the number of free sessions
         * @return int number of free sessions
         */
        int free_count() const {
            std::shared_lock lock(_mutex);
            return _free_sessions.size();
        }

        void reserve_session() {
            std::unique_lock lock(_mutex);
            _active_count++;
        }

    private:
        mutable std::shared_mutex _mutex;
        int _active_count = 0;
        std::list<ServerSessionPtr> _free_sessions;

    };
    using DatabasePoolPtr = std::shared_ptr<DatabasePool>;

    /** Database instance class, contains hostname, port etc. */
    class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
    public:
        DatabaseInstance(const Session::Type type,
                         const std::string &hostname,
                         int port=5432,
                         int max_sessions=100)
            : _type(type), _hostname(hostname), _port(port), _max_sessions(max_sessions)
        {}

        /** get hostname */
        const std::string &hostname() const { return _hostname; }

        /** get port */
        int port() const { return _port; }

        /** create connection to this instance */
        ProxyConnectionPtr create_connection() {
            return ProxyConnection::create(_hostname, _port);
        }

        /**
         * @brief Get the least recently used session from the db instance.
         * Note: returns the LRU session, but doesn't close the session or remove it from pool.
         * @returns session or nullptr if LRU list is empty
         */
        ServerSessionPtr evict_session();

        /**
         * @brief Get the size of the pool for a given dbname, username
         * @param db_id database id
         * @param username username
         * @returns size of the pool
         */
        DatabasePoolPtr get_pool(const uint64_t db_id,
                                 const std::string &username) const;

        /**
         * @brief Get a free session from the db instance (and associated pool).
         * Removes session from LRU list (so it can't be evicted), incr active count.
         * @param db_id database id
         * @param username username
         * @return ServerSessionPtr
         */
        ServerSessionPtr get_session(const uint64_t db_id,
                                     const std::string &username);

        /**
         * @brief Release session back to the db instance (and associated pool).
         * Marks as free, decr active count.
         * @param session Session to release
         */
        void release_session(ServerSessionPtr session);

        /**
         * @brief Delete a session from the db instance (and associated pool).
         * @param session Session to remove
         */
        void delete_session(ServerSessionPtr session);

        /**
         * @brief Allocate a session from the db instance.  Creates a new session if necessary.  Evicts a session if necessary.
         * @param server ProxyServerPtr server object
         * @param user UserPtr user
         * @param dbname std::string database name
         * @return ServerSessionPtr session
         */
        ServerSessionPtr allocate_session(ProxyServerPtr server,
                                          UserPtr user,
                                          const std::string &dbname);


        /**
         * @brief Get total count of sessions associated with this instance.
         * @return int total sessions: sum of lru list and active sessions
         */
        int total_count() const {
            std::shared_lock lock(_mutex);
            return _sessions_lru.size() + _active_sessions;
        }

        /**
         * @brief Get count of active (in use) sessions associated with this instance.
         * @return int number of active sessions
         */
        int active_count() const {
            std::shared_lock lock(_mutex);
            return _active_sessions;
        }

    private:
        mutable std::shared_mutex _mutex;
        Session::Type _type;
        std::string _hostname;
        int _port;
        int _max_sessions;
        int _active_sessions=0;

        /** map of dbname, username to database pool */
        std::map<std::pair<uint64_t, std::string>, DatabasePoolPtr> _sessions;

        /** lru list of sessions that are not in use */
        std::list<ServerSessionPtr> _sessions_lru;

        /** Internal call to get a session, assumes lock is held */
        ServerSessionPtr _internal_get_session(const uint64_t db_id,
                                               const std::string &username);

        /** Internal call to evict a session, assumes lock is held */
        ServerSessionPtr _internal_evict_session();

    };
    using DatabaseInstancePtr = std::shared_ptr<DatabaseInstance>;

    /**
     * Class representing a database replica set, contains a list of
     * database instances for the replica set.
     */
    class DatabaseReplicaSet {
    public:
        DatabaseReplicaSet() = default;

        /**
         * @brief Add a replica to the replica set
         * @param replica replica to add
         */
        void add_replica(DatabaseInstancePtr replica) {
            std::unique_lock lock(_mutex);
            _replicas.push_back(replica);
        }

        /**
         * @brief Does a replica exist for this user and database?
         * @param db_id database id
         * @param username username
         * @return true if any replica exists with a session for this user and database
         * @return false if replica does not exist
         */
        bool pool_exists(const uint64_t db_id,
                            const std::string &username)
        {
            std::shared_lock lock(_mutex);
            for (auto &replica : _replicas) {
                DatabasePoolPtr pool = replica->get_pool(db_id, username);
                if (pool != nullptr) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @brief Get a replica from the replica set.  Returns a session from the replica
         * @returns session or nullptr if no sessions available
         */
        DatabaseInstancePtr get_replica(const uint64_t db_id,
                                        const std::string &username)
        {
            std::unique_lock lock(_mutex);
            // XXX make this smarter in the future

            // find the replica with the least number of connections
            int min_sessions = INT_MAX;
            int min_sessions_alloced = INT_MAX;
            DatabaseInstancePtr min_instance = nullptr;
            DatabaseInstancePtr min_alloced_instance = nullptr;
            for (auto &replica : _replicas) {
                int count = replica->total_count();
                if (count < min_sessions) {
                    min_sessions = count;
                    min_instance = replica;
                }
                if (count < min_sessions_alloced) {
                    DatabasePoolPtr pool = replica->get_pool(db_id, username);
                    if (pool != nullptr && pool->free_count() > 0) {
                        min_alloced_instance = replica;
                        min_sessions_alloced = count;
                    }
                }
            }

            // prefer replica with existing session
            if (min_alloced_instance != nullptr) {
                // get session from instance; else fall through to get random session
                return min_alloced_instance;
            }

            // next try to get a session from the replica with the least number of connections
            if (min_instance != nullptr) {
                return min_instance;
            }

            // if we got here, ultimately need to evict from LRU list, pick an instance at random
            // XXX could keep an LRU list of instances to evict from
            int idx = rand() % _replicas.size();
            return _replicas[idx];
        }

    private:
        mutable std::shared_mutex _mutex;
        std::vector<DatabaseInstancePtr> _replicas;
    };

    /**
     * Class representing a database primary, contains a primary
     * and standby database instance.
     * @param primary primary database instance
     * @param standby standby database instance (nullptr ok)
     * @param max_sessions maximum number of sessions for the primary
     */
    class DatabasePrimarySet {
    public:
        DatabasePrimarySet() = default;

        /** Set primary instance */
        void set_primary(DatabaseInstancePtr primary) { _primary = primary; }

        /** Set standby instance */
        void set_standby(DatabaseInstancePtr standby) { _standby = standby; }

        /** Get primary instance */
        DatabaseInstancePtr primary() const { return _primary; }

        /** Get standby instance */
        DatabaseInstancePtr standby() const { return _standby; }

        /**
         * @brief Does a replica exist for this user and database?
         * @param db_id database id
         * @param username username
         * @return true if any replica exists with a session for this user and database
         * @return false if replica does not exist
         */
        bool pool_exists(const uint64_t db_id,
                         const std::string &username)
        {
            DatabasePoolPtr pool = _primary->get_pool(db_id, username);
            if (pool != nullptr) {
                return true;
            }
            return false;
        }

    private:
        DatabaseInstancePtr _primary;
        DatabaseInstancePtr _standby;
    };

    // TODO: add class documentation
    class DatabaseSchemaTableStore {
    public:
        DatabaseSchemaTableStore() = default;

        void add_item(const uint64_t db_id, const std::string &db_schema, const std::string &db_table) {
            auto db_it = _storage.find(db_id);
            if (db_it == _storage.end()) {
                std::map<std::string, std::set<std::string>> empty_db;
                _storage.insert(std::pair(db_id, empty_db));
                db_it = _storage.find(db_id);
            }
            std::map<std::string, std::set<std::string>> &db = db_it->second;
            auto schema_it = db.find(db_schema);
            if (schema_it == db.end()) {
                std::set<std::string> empty_schema;
                db.insert(std::pair(db_schema, empty_schema));
                schema_it = db.find(db_schema);
            }
            std::set<std::string> &schema = schema_it->second;
            schema.insert(db_table);
        }

        void remove_item(const uint64_t db_id, const std::string &db_schema, const std::string &db_table) {
            auto db_it = _storage.find(db_id);
            if (db_it == _storage.end()) {
                return;
            }
            std::map<std::string, std::set<std::string>> &db = db_it->second;
            auto schema_it = db.find(db_schema);
            if (schema_it == db.end()) {
                return;
            }
            std::set<std::string> &schema = schema_it->second;
            schema.erase(db_table);
        }

        bool has_item(const uint64_t db_id, const std::string &db_schema, const std::string &db_table) {
            auto db_it = _storage.find(db_id);
            if (db_it == _storage.end()) {
                return false;
            }
            std::map<std::string, std::set<std::string>> &db = db_it->second;
            auto schema_it = db.find(db_schema);
            if (schema_it == db.end()) {
                return false;
            }
            std::set<std::string> &schema = schema_it->second;
            if (schema.contains(db_table)) {
                return true;
            }
            return false;
        }
    private:
        std::map<uint64_t, std::map<std::string, std::set<std::string>>> _storage;
    };

} // namespace springtail
} // namespace pg_proxy
