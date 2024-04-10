#pragma once

#include <memory>
#include <string>
#include <map>
#include <set>

#include <common/thread_pool.hh>

#include <proxy/connection.hh>
#include <proxy/request_handler.hh>
#include <proxy/client_session.hh>

namespace springtail {
    class ProxyServer : public std::enable_shared_from_this<ProxyServer> {
    public:
        ProxyServer(const std::string &address,
                    int port,
                    int thread_pool_size = 16);

        void run();

        void signal(ProxyConnectionPtr connection);

        void shutdown(ClientSession *session);

    private:
        int _socket;

        int _pipe[2]; // 0 - read; 1 - write

        ProxyRequestHandlerPtr _request_handler;

        ThreadPool<ClientSession> _thread_pool;

        std::map<int, ClientSessionPtr> _sessions;

        std::mutex _waiting_sessions_mutex;
        std::set<int> _waiting_sessions;

        void _do_accept();
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

}