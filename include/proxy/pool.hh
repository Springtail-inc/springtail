#pragma once

#include <vector>
#include <set>
#include <queue>
#include <memory>
#include <mutex>

#include <proxy/session.hh>
#include <proxy/server_session.hh>
#include <proxy/client_session.hh>

namespace springtail {
    /**
     * @brief Pool of client and server sessions.
     * One pool class per user:database pair.
     */
    class Pool {
    public:
        Pool() {}

        void add_client_session(ClientSessionPtr client_session) {
            std::unique_lock<std::mutex> lock(_mutex);
            _client_sessions.insert(client_session);
        }

        void add_server_session(ServerSessionPtr server_session) {
            std::unique_lock<std::mutex> lock(_mutex);
            _server_sessions_free.push(server_session);
        }

        ServerSessionPtr get_server_session(ClientSessionPtr client_session) {
            std::unique_lock<std::mutex> lock(_mutex);
            if (_server_sessions_free.empty()) {
                _client_sessions_waiting.push(client_session);
                return nullptr;
            }

            ServerSessionPtr server_session = _server_sessions_free.front();
            _server_sessions_free.pop();
            _server_sessions_in_use.insert(server_session);
            return server_session;
        }

        void release_server_session(ServerSessionPtr server_session) {
            std::unique_lock<std::mutex> lock(_mutex);
            if (!_client_sessions_waiting.empty()) {
                ClientSessionPtr client_session = _client_sessions_waiting.front();
                _client_sessions_waiting.pop();
                client_session->notify_server_available(server_session);
            } else {
                _server_sessions_in_use.erase(server_session);
                _server_sessions_free.push(server_session);
            }
        }

    private:
        std::mutex _mutex;

        std::set<ClientSessionPtr> _client_sessions;            ///< All client sessions
        std::queue<ClientSessionPtr> _client_sessions_waiting;  ///< Client sessions waiting for a server session

        std::set<ServerSessionPtr> _server_sessions_in_use;     ///< Server sessions in use by a client
        std::queue<ServerSessionPtr> _server_sessions_free;     ///< Free server sessions
    };
}