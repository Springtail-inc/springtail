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

namespace springtail {
namespace pg_proxy {

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
                        SPDLOG_ERROR("SSL_write: error syscall: {}", err);
                    } else {
                        SPDLOG_ERROR("SSL_write: error syscall: EOF detected");
                        w = -1;
                    }
                    break;

                case SSL_ERROR_ZERO_RETURN:
                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    SPDLOG_ERROR("SSL_write: error ssl: err={}, msg={}", err, msg);
                    w = -1;
                    break;

                default:
                    SPDLOG_ERROR("SSL_write: unknown error: {}", err);
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
                SPDLOG_ERROR("Error writing to socket: {}", strerror(errno));
                close();
                throw ProxyIOError();
            }
        } while (size > 0);

        return bytes_written;
    }

    ssize_t
    ProxyConnection::read(char *buffer, int max_size, int at_least)
    {
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
                        SPDLOG_ERROR("SSL_read: error syscall: {}", err);
                    } else {
                        SPDLOG_ERROR("SSL_read: error syscall: EOF detected");
                        r = -1;
                    }
                    break;

                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    SPDLOG_ERROR("SSL_read: error ssl: {}", msg);
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
                    SPDLOG_ERROR("SSL_read: SSL_ERROR_ZERO_RETURN: {}", msg);
                    r = 0;
                    break;
                default:
                    SPDLOG_ERROR("SSL_read: unknown error: {}", err);
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
            } else if (n == 0) {
                // Connection closed by the client
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "Connection closed by client");
                close();
                throw ProxyIOError();
            } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                continue;
            } else {
                // Error occurred
                SPDLOG_ERROR("Error reading from socket: {}", strerror(errno));
                close();
                throw ProxyIOError();
            }
        } while (at_least > 0);

        return bytes_read;
    }

    static std::string hostname_to_ip(const std::string &hostname)
    {
	    struct hostent *he;
	    struct in_addr **addr_list;
	    int i;

	    if ( (he = gethostbyname( hostname.c_str() ) ) == nullptr) {
		    // get the host info
		    SPDLOG_ERROR("gethostbyname");
		    return {};
	    }

	    addr_list = (struct in_addr **) he->h_addr_list;

	    for(i = 0; addr_list[i] != nullptr; i++) {
		    // return the first one;
		    return {inet_ntoa(*addr_list[i])};
	    }

    	return {};
    }

    ProxyConnectionPtr
    ProxyConnection::create(const std::string &hostname, int port)
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock == -1) {
            SPDLOG_ERROR("Error creating socket: {}", strerror(errno));
            throw ProxyIOConnectionError();
        }

        SPDLOG_DEBUG_MODULE(LOG_PROXY, "Connecting to {}:{}", hostname, port);

        std::string ip = hostname_to_ip(hostname);
        if (ip.empty()) {
            SPDLOG_ERROR("Error resolving hostname: {}", hostname);
            ::close(sock);
            throw ProxyIOConnectionError();
        }

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
            SPDLOG_ERROR("Error converting hostname {} to address: {}", hostname, strerror(errno));
            ::close(sock);
            throw ProxyIOConnectionError();
        }

        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            SPDLOG_ERROR("Error connecting to {}:{}", hostname, port);
            ::close(sock);
            throw ProxyIOConnectionError();
        }

        return std::make_shared<ProxyConnection>(sock, addr);
    }

    void
    ProxyConnection::setup_SSL(SSL *ssl)
    {
        _ssl = ssl;
        int rc = ::SSL_set_fd(ssl, _socket);
        if (rc <= 0) {
            SPDLOG_ERROR("Error setting SSL fd: {}", _socket);
            throw ProxySSLConnectionError();
        }

        // set socket to non-blocking while we do the SSL handshake
        // it is set back to blocking after the handshake (after ssl_accept/connect)
        _set_non_blocking();

    #if SPDLOG_ACTIVE_LEVEL==SPDLOG_ACTIVE_DEBUG
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
            SPDLOG_ERROR("Error setting socket to blocking: {}", strerror(errno));
            throw ProxyIOError();
        }
    }

    void
    ProxyConnection::_set_non_blocking()
    {
        int flags = fcntl(_socket, F_GETFL, 0);
        int rc = fcntl(_socket, F_SETFL, flags | O_NONBLOCK);
        if (rc < 0) {
            SPDLOG_ERROR("Error setting socket to non-blocking: {}", strerror(errno));
            throw ProxyIOError();
        }
    }

    void
    ProxyConnection::_handle_ssl_error(int rc)
    {
        int err = SSL_get_error(rc);
        char *msg = ::ERR_error_string(err, NULL);
        switch (err) {
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
                SPDLOG_DEBUG_MODULE(LOG_PROXY, "SSL handshake in progress, need data: err={}", err);
                return;
            case SSL_ERROR_SYSCALL:
                SPDLOG_ERROR("SSL handshake failed: error syscall: errno={}\n", errno);
                break;
            case SSL_ERROR_SSL:
                SPDLOG_ERROR("SSL handshake failed: error ssl, msg={}", msg);
                break;
            default:
                SPDLOG_ERROR("SSL handshake failed: rc={}, err={}, msg={}", rc, err, msg);
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
} // namespace pg_proxy
} // namespace springtail
