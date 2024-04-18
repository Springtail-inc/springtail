#pragma once

#include <memory>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include <proxy/buffer.hh>

namespace springtail {
    /** Connection object */
    class ProxyConnection : public std::enable_shared_from_this<ProxyConnection> {
    public:
        ProxyConnection(int socket, struct sockaddr_in &addr)
          : _socket(socket),
            _addr(addr)
        {}

        ssize_t read(char *buffer, int size, int at_least = 0);
        ssize_t read(ProxyBuffer &buffer, int at_least = 0);
        ssize_t read_fully(ProxyBuffer &buffer, int size);
        ssize_t write(const char *buffer, int size, bool more = false);

        void close() {
            if (!_closed) {
                ::close(_socket);
                _closed = true;
            }
        }

        int get_socket() {
            return _socket;
        }

        bool closed() {
            return _closed;
        }

        std::string get_endpoint() {
            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &_addr.sin_addr, ip, INET_ADDRSTRLEN);
            return std::string(ip) + ":" + std::to_string(ntohs(_addr.sin_port));
        }

        /** factory method to create a connection */
        static std::shared_ptr<ProxyConnection>
        create(const std::string &hostname, int port);

    private:
        int _socket;
        struct sockaddr_in _addr;
        bool _closed = false;
    };
    using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;
}