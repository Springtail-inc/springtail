#pragma once

#include <memory>
#include <atomic>

#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

#include <openssl/ssl.h>

namespace springtail {
namespace pg_proxy {

    /** Connection object */
    class ProxyConnection : public std::enable_shared_from_this<ProxyConnection> {
    public:
        ProxyConnection(int socket, struct sockaddr_in &addr)
          : _socket(socket),
            _addr(addr)
        {}

        ~ProxyConnection() {
            SPDLOG_DEBUG_MODULE(LOG_PROXY, "Destroying connection to {}", _socket);
            close();
        }

        ssize_t read(char *buffer, int max_size, int at_least = 0);
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
        void setup_SSL(SSL *ssl);

        /** Accept with SSL 0 is success -- converts socket to non-blocking
         *  throws IOError on exception
         */
        int SSL_accept();

        /** Connect with SSL 0 is success -- converts socket to non-blocking
         *  throws IOError on exception
         */
        int SSL_connect();

        /** Get SSL error code */
        int SSL_get_error(int rc) {
            return ::SSL_get_error(_ssl, rc);
        }

        /** Does connection have pending data */
        bool has_pending();

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

        /** Set connection to non-blocking */
        void _set_non_blocking();

        /** Set connection to blocking */
        void _set_blocking();

        void _handle_ssl_error(int rc);
    };
    using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;
} // namespace pg_proxy
} // namespace springtail
