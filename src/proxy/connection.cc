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
#include <proxy/connection.hh>

#ifndef MSG_MORE
#define MSG_MORE 0 // not defined for OSX
#endif

namespace springtail {

    ssize_t
    ProxyConnection::write(const char *buffer, int size, bool more)
    {
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
            ssize_t n = ::SSL_write(_ssl, buffer, size);
            int err = ::SSL_get_error(_ssl, n);
            if (n > 0) {
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
                    if (n == -1) {
                        SPDLOG_ERROR("SSL_write: error syscall: {}", err);
                    } else {
                        SPDLOG_ERROR("SSL_write: error syscall: EOF detected");
                        n = -1;
                    }
                    break;

                case SSL_ERROR_ZERO_RETURN:
                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    SPDLOG_ERROR("SSL_write: error ssl: err={}, msg={}", err, msg);
                    n = -1;
                    break;

                default:
                    SPDLOG_ERROR("SSL_write: unknown error: {}", err);
                    n = -1;
                    break;
                }
                close();
                return n;
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
                return -1;
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
            ssize_t n = ::SSL_read(_ssl, buffer, max_size);
            int err = ::SSL_get_error(_ssl, n);
            if (n > 0) {
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
                    if (n == -1) {
                        SPDLOG_ERROR("SSL_read: error syscall: {}", err);
                    } else {
                        SPDLOG_ERROR("SSL_read: error syscall: EOF detected");
                        n = -1;
                    }
                    break;

                case SSL_ERROR_SSL:
                    msg = ERR_error_string(err, nullptr);
                    SPDLOG_ERROR("SSL_read: error ssl: {}", msg);
                    n = -1;
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
                    n = 0;
                    break;
                default:
                    SPDLOG_ERROR("SSL_read: unknown error: {}", err);
                    n = -1;
                    break;
                }
                close();
                return n;
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
                SPDLOG_DEBUG("Connection closed by client");
                close();
                return 0;
            } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                continue;
            } else {
                // Error occurred
                SPDLOG_ERROR("Error reading from socket: {}", strerror(errno));
                close();
                return -1;
            }
        } while (at_least > 0);

        return bytes_read;
    }

    ssize_t
    ProxyConnection::read(ProxyBuffer &buffer, int at_least)
    {
        buffer.reset();
        ssize_t n = read(buffer.data(), buffer.capacity(), at_least);
        if (n > 0) {
            buffer.set_read(n);
        }
        return n;
    }

    ssize_t
    ProxyConnection::read(ProxyBuffer &buffer, int max_size, int at_least)
    {
        buffer.reset();
        ssize_t n = read(buffer.data(), max_size, at_least);
        if (n > 0) {
            buffer.set_read(n);
        }
        return n;
    }

    ssize_t
    ProxyConnection::read_fully(ProxyBuffer &buffer, int size)
    {
        ssize_t n = read(buffer.next_data(), size, size);
        if (n > 0) {
            buffer.set_read(n);
        }
        return n;
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
            return nullptr;
        }

        SPDLOG_DEBUG("Connecting to {}:{}", hostname, port);

        std::string ip = hostname_to_ip(hostname);
        if (ip.empty()) {
            SPDLOG_ERROR("Error resolving hostname: {}", hostname);
            ::close(sock);
            return nullptr;
        }

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
            SPDLOG_ERROR("Error converting hostname {} to address: {}", hostname, strerror(errno));
            ::close(sock);
            return nullptr;
        }

        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            SPDLOG_ERROR("Error connecting to {}:{}", hostname, port);
            ::close(sock);
            return nullptr;
        }

        return std::make_shared<ProxyConnection>(sock, addr);
    }
}