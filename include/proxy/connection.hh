#pragma once

#include <memory>
#include <atomic>

#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <openssl/ssl.h>

#include <proxy/buffer.hh>

namespace springtail {
    /** Connection object */
    class ProxyConnection : public std::enable_shared_from_this<ProxyConnection> {
    public:
        ProxyConnection(int socket, struct sockaddr_in &addr)
          : _socket(socket),
            _addr(addr)
        {}

        ~ProxyConnection() {
            SPDLOG_DEBUG("Destroying connection to {}", _socket);
            close();
        }

        ssize_t read(char *buffer, int size, int at_least = 0);
        ssize_t read(ProxyBuffer &buffer, int at_least = 0);
        ssize_t read(ProxyBuffer &buffer, int max_size, int at_least);
        ssize_t read_fully(ProxyBuffer &buffer, int size);
        ssize_t write(const char *buffer, int size, bool more = false);

        void close() {
            if (!_closed.test_and_set()) {
                // free ssl object
                if (_ssl) {
                    ::SSL_shutdown(_ssl);
                    ::SSL_free(_ssl);
                }
                ::close(_socket);
            }
        }

        int get_socket() {
            return _socket;
        }

        bool closed() {
            return _closed.test();
        }

        std::string endpoint() {
            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &_addr.sin_addr, ip, INET_ADDRSTRLEN);
            return std::string(ip) + ":" + std::to_string(ntohs(_addr.sin_port));
        }

        int setup_SSL(SSL *ssl) {
            _ssl = ssl;
            int rc = ::SSL_set_fd(ssl, _socket);
            if (rc < 0) {
                SPDLOG_ERROR("Error setting SSL fd\n");
                return -1;
            }

            int flags = fcntl(_socket, F_GETFL, 0);
            rc = fcntl(_socket, F_SETFL, flags | O_NONBLOCK);
            if (rc < 0) {
                SPDLOG_ERROR("Error setting socket to non-blocking: {}", strerror(errno));
                return -1;
            }

            return 0;
        }

        // do ssl accept set non-blocking return code
        int SSL_accept() {
            // set socket to non-blocking
            int rc = ::SSL_accept(_ssl);
            if (rc == 1) {
                // set socket back to blocking
                int flags = fcntl(_socket, F_GETFL, 0);
                rc = fcntl(_socket, F_SETFL, flags & ~O_NONBLOCK);
            }
            return rc;
        }

        int SSL_get_error(int rc) {
            return ::SSL_get_error(_ssl, rc);
        }

        /** factory method to create a connection */
        static std::shared_ptr<ProxyConnection> create(const std::string &hostname, int port);

    private:
        int _socket;
        struct sockaddr_in _addr;
        std::atomic_flag _closed = ATOMIC_FLAG_INIT;
        SSL *_ssl = nullptr;
    };
    using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;
}