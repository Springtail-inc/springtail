#pragma once

#include <memory>
#include <string>

#include <proxy/connection.hh>
#include <proxy/request_handler.hh>

namespace springtail {
    class ProxyServer {
    public:
        ProxyServer(const std::string &address,
                    int port);

        void run();

    private:
        void _handle_connection(ProxyConnectionPtr connection);

        int _socket;

        ProxyRequestHandlerPtr _request_handler;
    };
    using ProxyServerPtr = std::shared_ptr<ProxyServer>;

}