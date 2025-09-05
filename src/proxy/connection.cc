#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <unistd.h>

#include <common/logging.hh>
#include <proxy/exception.hh>
#include <proxy/connection.hh>

#ifndef MSG_MORE
#define MSG_MORE 0 // not defined for OSX
#endif

namespace springtail::pg_proxy {

    ProxyConnection::ProxyConnection(int socket)
        : _socket(socket), _endpoint(_get_peer_name())
    {}


    std::string
    ProxyConnection::_get_peer_name()
    {
        char host[NI_MAXHOST];
        char service[NI_MAXSERV];
        struct sockaddr_storage addr;
        socklen_t len = sizeof(addr);

        if (getpeername(_socket, (struct sockaddr *)&addr, &len) == -1) {
            LOG_ERROR("Error getting peer name: {}", strerror(errno));
            return "unknown";
        }

        int rc = getnameinfo((struct sockaddr *)&addr, len, host, sizeof(host), service, sizeof(service), NI_NUMERICHOST | NI_NUMERICSERV);
        if (rc != 0) {
            LOG_ERROR("Error getting name info: {}", gai_strerror(rc));
            return "unknown";
        }

        return std::string(host) + ":" + std::string(service);
    }

    void
    ProxyConnection::get_peer_address(struct sockaddr_storage *addr, socklen_t *len)
    {
        if (getpeername(_socket, (struct sockaddr *)addr, len) == -1) {
            LOG_ERROR("Error getting peer name: {}", strerror(errno));
            *len = 0;
        }
    }

    void
    ProxyConnection::close()
    {
        if (_closed.test_and_set()) {
            // already closed
            return;
        }

        // free ssl object
        if (_ssl != nullptr && _ssl_enabled) {
            ::SSL_shutdown(_ssl);
        }
        if (_ssl != nullptr) {
            ::SSL_free(_ssl);
        }
        ::close(_socket);
     }

    ssize_t
    ProxyConnection::write(const char *buffer, int size, bool more)
    {
        if (closed()) {
            throw ProxyIOError();
        }

        if (_ssl != nullptr) {
            return _ssl_write(buffer, size);
        } else {
            return _write(buffer, size, more);
        }
    }

    ssize_t
    ProxyConnection::_ssl_write(const char *buffer, int size)
    {
        ssize_t bytes_written = 0;
        char *msg = nullptr;

        do {
            size_t n;
            int w = ::SSL_write_ex(_ssl, buffer, size, &n);
            int err = ::SSL_get_error(_ssl, n);
            if (w > 0) {
                bytes_written += n;
                buffer += n;
                size -= n;
            } else {
                switch (err) {
                case SSL_ERROR_WANT_READ:
                case SSL_ERROR_WANT_WRITE:
                    // do nothing
                    continue;

                case SSL_ERROR_SYSCALL:
                    if (w == -1) {
                        LOG_ERROR("SSL_write: error syscall: {}", err);
                    } else {
                        LOG_ERROR("SSL_write: error syscall: EOF detected");
                        w = -1;
                    }
                    break;

                case SSL_ERROR_ZERO_RETURN:
                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    LOG_ERROR("SSL_write: error ssl: err={}, msg={}", err, msg);
                    w = -1;
                    break;

                default:
                    LOG_ERROR("SSL_write: unknown error: {}", err);
                    w = -1;
                    break;
                }
                close();

                throw ProxyIOError();
            }
        } while (size > 0);

        return bytes_written;
    }

    ssize_t
    ProxyConnection::_write(const char *buffer, int size, bool more)
    {
        ssize_t bytes_written = 0;
        do {
            ssize_t n = ::send(_socket, buffer, size, more ? MSG_MORE : 0);
            if (n > 0) {
                bytes_written += n;
                buffer += n;
                size -= n;
            } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                continue;
            } else {
                LOG_ERROR("Error writing to socket: {}", strerror(errno));
                close();
                throw ProxyIOError();
            }
        } while (size > 0);

        return bytes_written;
    }

    ssize_t
    ProxyConnection::read(char *buffer, int max_size, int at_least)
    {
        if (max_size == 0) {
            return 0;
        }

        if (_ssl != nullptr) {
            return _ssl_read(buffer, max_size, at_least);
        } else {
            return _read(buffer, max_size, at_least);
        }
    }

    ssize_t
    ProxyConnection::_ssl_read(char *buffer, int max_size, int at_least)
    {
        char *msg = nullptr;
        ssize_t bytes_read = 0;

        do {
            size_t n;
            int r = ::SSL_read_ex(_ssl, buffer, max_size, &n);
            int err = ::SSL_get_error(_ssl, n);
            if (r > 0) {
                bytes_read += n;
                buffer += n;
                max_size -= n;
                at_least -= n;
            } else {
                switch (err) {
                case SSL_ERROR_WANT_READ:
                case SSL_ERROR_WANT_WRITE:
                    // do nothing
                    continue;

                case SSL_ERROR_SYSCALL:
                    if (r == -1) {
                        LOG_ERROR("SSL_read: error syscall: {}", err);
                    } else {
                        LOG_ERROR("SSL_read: error syscall: EOF detected");
                        r = -1;
                    }
                    break;

                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    LOG_ERROR("SSL_read: error ssl: {}", msg);
                    r = -1;
                    break;

                case SSL_ERROR_ZERO_RETURN:
                    /* SSL manual says:
                    * -------------------------------------------------------------
                    * The TLS/SSL peer has closed the connection for
                    * writing by sending the close_notify alert. No more data can be
                    * read. Note that SSL_ERROR_ZERO_RETURN does not necessarily
                    * indicate that the underlying transport has been closed.
                    * -------------------------------------------------------------
                    * We don't want to trigger failover but it is also possible that
                    * the connectoon has been closed. So returns 0 to ask pool_read()
                    * to close connection to frontend.
                    */
                    msg = ERR_error_string(err, nullptr);
                    LOG_ERROR("SSL_read: SSL_ERROR_ZERO_RETURN: {}", msg);
                    r = 0;
                    break;
                default:
                    LOG_ERROR("SSL_read: unknown error: {}", err);
                    r = -1;
                    break;
                }
                close();
                throw ProxyIOError();
            }
        } while (at_least > 0);

        return bytes_read;
    }

    ssize_t
    ProxyConnection::_read(char *buffer, int max_size, int at_least)
    {
        ssize_t bytes_read = 0;
        do {
            ssize_t n = ::recv(_socket, buffer, max_size, 0);
            if (n > 0) {
                bytes_read += n;
                buffer += n;
                max_size -= n;
                at_least -= n;
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Read {} bytes from socket, remaining={}", n, at_least);
            } else if (n == 0) {
                // Connection closed by the client
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "Connection closed by client");
                close();
                throw ProxyIOError();
            } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                continue;
            } else {
                // Error occurred
                LOG_ERROR("Error reading from socket: {}", strerror(errno));
                close();
                throw ProxyIOError();
            }
        } while (at_least > 0);

        return bytes_read;
    }


    ProxyConnectionPtr
    ProxyConnection::create(const std::string &hostname, int port)
    {
        struct addrinfo hints, *res, *p;
        int sock = -1;

        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC; // Allow IPv4 or IPv6
        hints.ai_socktype = SOCK_STREAM;

        std::string port_str = std::to_string(port);
        int status = getaddrinfo(hostname.c_str(), port_str.c_str(), &hints, &res);
        if (status != 0) {
            LOG_ERROR("Error resolving hostname {}: {}", hostname, gai_strerror(status));
            throw ProxyIOConnectionError();
        }

        for (p = res; p != nullptr; p = p->ai_next) {
            sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (sock == -1) {
                continue;
            }

            if (connect(sock, p->ai_addr, p->ai_addrlen) == -1) {
                ::close(sock);
                sock = -1;
                continue;
            }

            break; // Successfully connected
        }

        freeaddrinfo(res);

        if (sock == -1) {
            LOG_ERROR("Error connecting to {}:{}", hostname, port);
            throw ProxyIOConnectionError();
        }

        return std::make_shared<ProxyConnection>(sock);
    }

    void
    ProxyConnection::setup_SSL(SSL *ssl)
    {
        _ssl = ssl;
        int rc = ::SSL_set_fd(ssl, _socket);
        if (rc <= 0) {
            LOG_ERROR("Error setting SSL fd: {}", _socket);
            throw ProxySSLConnectionError();
        }

        // set socket to non-blocking while we do the SSL handshake
        // it is set back to blocking after the handshake (after ssl_accept/connect)
        _set_non_blocking();

    #if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
        // set the connection object in the SSL ex data for debugging
        SSL_set_ex_data(ssl, 0, this);
    #endif
    }

    void
    ProxyConnection::_set_blocking()
    {
        int flags = fcntl(_socket, F_GETFL, 0);
        int rc = fcntl(_socket, F_SETFL, flags & ~O_NONBLOCK);
        if (rc < 0) {
            LOG_ERROR("Error setting socket to blocking: {}", strerror(errno));
            throw ProxyIOError();
        }
    }

    void
    ProxyConnection::_set_non_blocking()
    {
        int flags = fcntl(_socket, F_GETFL, 0);
        int rc = fcntl(_socket, F_SETFL, flags | O_NONBLOCK);
        if (rc < 0) {
            LOG_ERROR("Error setting socket to non-blocking: {}", strerror(errno));
            throw ProxyIOError();
        }
    }

    void
    ProxyConnection::_handle_ssl_error(int rc)
    {
        int err = SSL_get_error(rc);
        char *msg = ::ERR_error_string(err, nullptr);
        switch (err) {
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL handshake in progress, need data: err={}", err);
                return;
            case SSL_ERROR_SYSCALL:
                LOG_ERROR("SSL handshake failed: error syscall: errno={}\n", errno);
                break;
            case SSL_ERROR_SSL:
                LOG_ERROR("SSL handshake failed: error ssl, msg={}", msg);
                break;
            default:
                LOG_ERROR("SSL handshake failed: rc={}, err={}, msg={}", rc, err, msg);
                break;
        }

        // throw exception
        close();
        throw ProxySSLConnectionError();
    }

    int
    ProxyConnection::SSL_accept()
    {
        int rc = ::SSL_accept(_ssl);
        if (rc <= 0) {
            // throws exception on fatal error, otherwise returns
            _handle_ssl_error(rc);
            return -1;
        }

        // success
        _ssl_enabled = true;

        // set socket back to blocking
        _set_blocking();

        return 0;
    }

    int
    ProxyConnection::SSL_connect()
    {
        int rc = ::SSL_connect(_ssl);
        if (rc <= 0) {
            // throws exception on fatal error, otherwise returns
            _handle_ssl_error(rc);
            return -1;
        }

        // success
        _ssl_enabled = true;

        // set socket back to blocking
        _set_blocking();

        return 0;
    }

    bool
    ProxyConnection::has_pending(std::vector<ProxyConnectionPtr> connections,
                                 std::set<int> &fds)
    {
        struct pollfd pfds[connections.size()];
        fds.clear();

        int i = 0;
        for (auto &conn : connections) {
            if (conn->_ssl_enabled && ::SSL_pending(conn->_ssl) > 0) {
                fds.insert(conn->get_socket());
            }
            pfds[i].fd = conn->get_socket();
            pfds[i].events = POLLIN;
            pfds[i].revents = 0;
            i++;
        }

        int n = poll(pfds, connections.size(), 0);
        if (n > 0) {
            for (i = 0; i < connections.size(); i++) {
                if (pfds[i].revents & POLLIN) {
                    fds.insert(pfds[i].fd);
                }
            }
        }

        return fds.size() > 0;
    }

    /** Does connection have pending data */
    bool
    ProxyConnection::has_pending()
    {
        // if ssl is enabled, check for buffered data on ssl connection first
        if (_ssl_enabled && ::SSL_pending(_ssl) > 0) {
            return true;
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
} // namespace springtail::pg_proxy
