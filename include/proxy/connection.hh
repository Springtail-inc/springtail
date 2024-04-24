#pragma once

#include <memory>
#include <atomic>

#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

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
                if (_ssl != nullptr && _ssl_enabled) {
                    ::SSL_shutdown(_ssl);
                }
                if (_ssl != nullptr) {
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

        /** Get endpoint of connection */
        std::string endpoint() {
            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &_addr.sin_addr, ip, INET_ADDRSTRLEN);
            return std::string(ip) + ":" + std::to_string(ntohs(_addr.sin_port));
        }

        /** Setup SSL for the connection socket 0 - success */
        int setup_SSL(SSL *ssl) {
            _ssl = ssl;
            int rc = ::SSL_set_fd(ssl, _socket);
            if (rc < 0) {
                SPDLOG_ERROR("Error setting SSL fd\n");
                return -1;
            }

            // set socket to non-blocking while we do the SSL handshake
            // it is set back to blocking after the handshake (after ssl_accept)
            int flags = fcntl(_socket, F_GETFL, 0);
            rc = fcntl(_socket, F_SETFL, flags | O_NONBLOCK);
            if (rc < 0) {
                SPDLOG_ERROR("Error setting socket to non-blocking: {}", strerror(errno));
                return -1;
            }

#if SPDLOG_ACTIVE_LEVEL==SPDLOG_ACTIVE_DEBUG
            // set the connection object in the SSL ex data for debugging
            SSL_set_ex_data(ssl, 0, this);
#endif

            return 0;
        }

        /** Accept with SSL 1 is success -- converts socket to non-blocking */
        int SSL_accept() {
            // set socket to non-blocking
            int rc = ::SSL_accept(_ssl);
            if (rc == 1) {
                // success
                _ssl_enabled = true;

                // set socket back to blocking
                int flags = fcntl(_socket, F_GETFL, 0);
                int r = fcntl(_socket, F_SETFL, flags & ~O_NONBLOCK);
                if (r < 0) {
                    SPDLOG_ERROR("Error setting socket to blocking: {}", strerror(errno));
                    return -1;
                }
            }
            return rc;
        }

        /** Connect with SSL 1 is success -- converts socket to non-blocking */
        int SSL_connect() {
            int rc = ::SSL_connect(_ssl);
            if (rc == 1) {
                // success
                _ssl_enabled = true;

                // set socket back to blocking
                int flags = fcntl(_socket, F_GETFL, 0);
                int r = fcntl(_socket, F_SETFL, flags & ~O_NONBLOCK);
                if (r < 0) {
                    SPDLOG_ERROR("Error setting socket to blocking: {}", strerror(errno));
                    return -1;
                }
            }
            return rc;
        }

        /** Get SSL error code */
        int SSL_get_error(int rc) {
            return ::SSL_get_error(_ssl, rc);
        }

        /** Does connection have pending data */
        bool has_pending() {
            if (_ssl_enabled) {
                if (::SSL_pending(_ssl) > 0) {
                    return true;
                }
            }

            // check socket if we want to see if more data is available
            // call poll() which is a function call
            struct pollfd pfd = { get_socket(), POLLIN, 0 };
            int n = poll(&pfd, 1, 0);
            if (n > 0 && pfd.revents & POLLIN) {
                return true;
            }

            return false;
        }

        /** factory method to create a connection */
        static std::shared_ptr<ProxyConnection> create(const std::string &hostname, int port);

    private:
        int _socket;
        struct sockaddr_in _addr;
        std::atomic_flag _closed = ATOMIC_FLAG_INIT;
        SSL *_ssl = nullptr;
        bool _ssl_enabled = false;

        ssize_t _ssl_read(char *buffer, int max_size, int at_least);
        ssize_t _ssl_write(const char *buffer, int size);

        ssize_t _read(char *buffer, int size, int at_least);
        ssize_t _write(const char *buffer, int size, bool more);
    };
    using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;
}