#pragma once

#include <memory>
#include <atomic>
#include <set>
#include <vector>

#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

#include <openssl/ssl.h>

#include <proxy/logging.hh>

namespace springtail::pg_proxy {

    /** Connection object */
    class ProxyConnection : public std::enable_shared_from_this<ProxyConnection> {
    public:
        using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;

        ProxyConnection(int socket);

        ~ProxyConnection() {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Destroying connection to {}", _socket);
            close();
        }

        /**
         * @brief External read method, internally calls _read or _ssl_read
         * @param buffer buffer to read into
         * @param max_size maximum size to read
         * @param at_least minimum bytes to read
         * @return ssize_t number of bytes read (at least at_least)
         */
        ssize_t read(char *buffer, int max_size, int at_least = 0);

        /**
         * @brief External write method, internally calls _write or _ssl_write
         * @param buffer buffer to write
         * @param size size of buffer
         * @param more true if more data is coming (ignored for SSL connection)
         * @return ssize_t number of bytes written
         */
        ssize_t write(const char *buffer, int size, bool more = false);

        /** Close the connection */
        void close();

        /** Get the socket file descriptor */
        int get_socket() {
            return _socket;
        }

        /** Is the connection closed */
        bool closed() {
            return _closed.test();
        }

        /** Get endpoint of connection */
        std::string endpoint() {
            return _endpoint;
        }

        /** Setup SSL for the connection socket 0 - success */
        void setup_SSL(SSL *ssl);

        /**
         * @brief Accept with SSL
         * @returns 0 is success -- converts socket to non-blocking
         * @throws IOError on exception
         */
        int SSL_accept();

        /**
         * @brief Connect with SSL
         * @returns 0 is success -- converts socket to non-blocking
         * @throws IOError on exception
         */
        int SSL_connect();

        /** Get SSL error code */
        int SSL_get_error(int rc) {
            return ::SSL_get_error(_ssl, rc);
        }

        /** Does connection have pending data */
        bool has_pending();

        /** factory method to create a connection */
        static ProxyConnectionPtr create(const std::string &hostname, int port);

        /** Do any of the connections have pending data, mark their fds */
        static bool has_pending(std::vector<ProxyConnectionPtr> connections, std::set<int> &fds);

    private:
        int _socket;              ///< socket file descriptor
        std::atomic_flag _closed = ATOMIC_FLAG_INIT; ///< true if connection is closed
        SSL *_ssl = nullptr;       ///< SSL object
        bool _ssl_enabled = false; ///< true if SSL is enabled
        std::string _endpoint;     ///< endpoint name of connection

        /** Helper to read at_least bytes from ssl connection */
        ssize_t _ssl_read(char *buffer, int max_size, int at_least);

        /** Helper to write buffer to ssl connection */
        ssize_t _ssl_write(const char *buffer, int size);

        /** Helper to read at_least bytes from normal connection */
        ssize_t _read(char *buffer, int size, int at_least);

        /** Helper to write buffer to normal connection */
        ssize_t _write(const char *buffer, int size, bool more);

        /** Set connection to non-blocking */
        void _set_non_blocking();

        /** Set connection to blocking */
        void _set_blocking();

        /** Get peer hostname:service */
        std::string _get_peer_name();

        /**
         * @brief Print appropriate SSL error message and close connection
         * @throws ProxySSLConnectionError
         */
        void _handle_ssl_error(int rc);
    };
    using ProxyConnectionPtr = std::shared_ptr<ProxyConnection>;
} // namespace springtail::pg_proxy
