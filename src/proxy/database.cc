#include <mutex>
#include <shared_mutex>
#include <memory>

#include <common/logging.hh>

#include <proxy/database.hh>
#include <proxy/session.hh>
#include <proxy/client_session.hh>
#include <proxy/server_session.hh>

namespace springtail {

    ServerSessionPtr
    DatabaseInstance::evict_session() {
        std::unique_lock lock(_mutex);
        return _internal_evict_session();
    }

    DatabasePoolPtr
    DatabaseInstance::get_pool(const std::string &dbname,
                                const std::string &username) const
    {
        std::shared_lock lock(_mutex);
        // get size of pool based on dbname and username
        auto it = _sessions.find({dbname, username});
        if (it != _sessions.end()) {
            return it->second;
        }
        return nullptr;
    }

    /**
     * @brief Get a free session from the db instance (and associated pool).
     * Removes session from LRU list (so it can't be evicted), incr active count.
     * @param dbname database name
     * @param username username
     * @return ServerSessionPtr
     */
    ServerSessionPtr
    DatabaseInstance::get_session(const std::string &dbname,
                                    const std::string &username)
    {
        std::unique_lock lock(_mutex);
        return _internal_get_session(dbname, username);
    }

    void
    DatabaseInstance::release_session(ServerSessionPtr session)
    {
        std::unique_lock lock(_mutex);
        auto it = _sessions.find({session->database(), session->username()});
        if (it != _sessions.end()) {
            it->second->release_session(session);
            _active_sessions--;
        }
    }

    void
    DatabaseInstance::delete_session(ServerSessionPtr session)
    {
        std::unique_lock lock(_mutex);

        auto it = _sessions.find({session->database(), session->username()});
        if (it != _sessions.end()) {
            it->second->delete_session(session);
        }

        // find session in LRU list and remove
        _sessions_lru.remove(session);
    }

    ServerSessionPtr
    DatabaseInstance::allocate_session(ProxyServerPtr server,
                                       UserPtr user,
                                       const std::string &database,
                                       Session::Type type)
    {
        std::unique_lock lock(_mutex);

        // try to get a free one first
        ServerSessionPtr session = _internal_get_session(database, user->username());
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
        auto it = _sessions.find({database, user->username()});
        DatabasePoolPtr pool;
        if (it == _sessions.end()) {
            pool = std::make_shared<DatabasePool>();
            _sessions[{session->database(), session->username()}] = pool;
        } else {
            pool = it->second;
        }

        // incr active count on pool
        pool->incr_active_count();

        // incr active count on instance
        _active_sessions++;

        // unlock and then create the new session
        lock.unlock();

        // create a new session
        session = ServerSession::create(server, user, database, shared_from_this(), type);

        return session;
    }

    ServerSessionPtr
    DatabaseInstance::_internal_get_session(const std::string &dbname,
                                            const std::string &username)
    {
        // lock must be held
        auto it = _sessions.find({dbname, username});
        if (it != _sessions.end()) {
            ServerSessionPtr session = it->second->get_session();
            if (session != nullptr) {
                // find the session in LRU list and remove it
                _sessions_lru.remove(session);
                _active_sessions++;
                return session;
            }
        }
        return nullptr;
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
        auto it = _sessions.find({session->database(), session->username()});
        assert (it != _sessions.end());
        if (it != _sessions.end()) {
            it->second->delete_session(session);
        }

        return session;
    }

} // namespace springtail