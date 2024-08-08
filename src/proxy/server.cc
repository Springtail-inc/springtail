#include <iostream>
#include <thread>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <common/logging.hh>
#include <common/thread_pool.hh>

#include <proxy/client_session.hh>
#include <proxy/server.hh>
#include <proxy/logger.hh>
#include <proxy/logging.hh>

namespace springtail::pg_proxy {

    /** Default log level for the proxy server */
    LogLevel proxy_log_level = LOG_LEVEL_DEBUG1;

    /**
     * @brief Construct a new Proxy Server object.
     * The server handles the poll loop and accepts new connections.
     * It dispatches readable sockets into the thread pool
     * @param proxy_port          port to listen for connections on
     * @param thread_pool_size    number of threads in the thread pool
     * @param cert_file           path to the server certificate file
     * @param key_file            path to the server key file
     * @param enable_ssl          enable SSL
     */
    ProxyServer::ProxyServer(int proxy_port,
                             int thread_pool_size,
                             const std::filesystem::path &cert_file,
                             const std::filesystem::path &key_file,
                             bool shadow_mode,
                             bool enable_ssl,
                             LoggerPtr logger)
      : _id(arc4random()),
        _user_mgr(std::make_shared<UserMgr>()),
        _thread_pool(thread_pool_size),
        _enable_ssl(enable_ssl),
        _shadow_mode(shadow_mode),
        _logger(logger)
    {
        // Initialize SSL
        if (_enable_ssl) {
            _ssl_ctx_server = _setup_SSL_context(cert_file, key_file);
            _ssl_ctx_client = _setup_SSL_context();
        }

        _socket = socket(AF_INET, SOCK_STREAM, 0);

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(proxy_port);

        int flags = 1;
        if (setsockopt(_socket, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(int)) < 0) {
            SPDLOG_ERROR("Error setting socket options: SO_REUSEADDR\n");
            close(_socket);
            exit(1);
        }

        if (bind(_socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            SPDLOG_ERROR("Error binding to socket\n");
            close(_socket);
            exit(1);
        }

        if (fcntl(_socket, F_SETFL, O_NONBLOCK) < 0) {
            SPDLOG_ERROR("Error setting socket non-blocking\n");
            close(_socket);
            exit(1);
        }

        if (listen(_socket, 16) < 0) {
            SPDLOG_ERROR("Error listening on socket\n");
            close(_socket);
            exit(1);
        }

        if (pipe(_pipe) < 0) {
            std::cerr << "Error creating pipe" << std::endl;
            close(_socket);
            exit(1);
        }
        // set pipe read non-blocking
        if (fcntl(_pipe[0], F_SETFL, O_NONBLOCK) < 0) {
            std::cerr << "Error setting pipe read non-blocking" << std::endl;
            close(_socket);
            close(_pipe[0]);
            close(_pipe[1]);
            exit(1);
        }

        // ignore SIGPIPE signals
        ::signal(SIGPIPE, SIG_IGN);

        SPDLOG_INFO("Proxy server listening on port={}", proxy_port);
    }

    /** Callback to get more info about what is going on in SSL */
    static void
    ssl_info_callback(const SSL *s, int where, int ret)
    {
        const char *str;
        int w = where & ~SSL_ST_MASK;

        ProxyConnection *conn = static_cast<ProxyConnection *>(SSL_get_ex_data(s, 0));
        int fd = conn->get_socket();

        if (w & SSL_ST_CONNECT) {
            str = "SSL_connect";
        } else if (w & SSL_ST_ACCEPT) {
            str = "SSL_accept";
        } else {
            str = "undefined";
        }

        if (where & SSL_CB_HANDSHAKE_START) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_HANDSHAKE_STARTED): fd={}", fd);
        } else if (where & SSL_CB_HANDSHAKE_DONE) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_HANDSHAKE_DONE): fd={}", fd);
        } else if (where & SSL_CB_LOOP) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_LOOP): fd={}, {}:{}", fd, str, SSL_state_string_long(s));
        } else if (where & SSL_CB_ALERT) {
            str = (where & SSL_CB_READ) ? "read" : "write";
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_ALERT): fd={}, SSL3 alert {}:{}:{}", fd, str,
                         SSL_alert_type_string_long(ret),
                         SSL_alert_desc_string_long(ret));
       } else if (where & SSL_CB_EXIT) {
            if (ret == 0) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_EXIT): fd={}, {}:failed in {}", fd, str, SSL_state_string_long(s));
            } else if (ret < 0) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info (CB_EXIT): fd={}, {}:error in {}", fd, str, SSL_state_string_long(s));
            }
        } else {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL info callback: fd={}, where={:#X}, ret={}", fd, where, ret);
        }
    }

    static int
    ssl_verify_callback(int preverify_ok, X509_STORE_CTX *x509_ctx)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "SSL verify callback: preverify_ok={}", preverify_ok);
        return 1; // no verification
    }

    SSL_CTX *
    ProxyServer::_setup_SSL_context(const std::filesystem::path &cert_file,
                                    const std::filesystem::path &key_file)
    {
        // create the SSL context
        SSL_CTX *ssl_ctx = ::SSL_CTX_new(::TLS_method());
        if (!ssl_ctx) {
            SPDLOG_ERROR("Error creating SSL context");
            exit(1);
        }

#if SPDLOG_ACTIVE_LEVEL==SPDLOG_ACTIVE_DEBUG
        // set the info callback
        SSL_CTX_set_info_callback(ssl_ctx, ssl_info_callback);
#endif

        /*
         * Disable OpenSSL's moving-write-buffer sanity check, because it causes
         * unnecessary failures in nonblocking send cases. (from pgbouncer code)
         */
        SSL_CTX_set_mode(ssl_ctx, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

        /* Set the key and cert */
        if (!cert_file.empty() && !key_file.empty()) {
            if (::SSL_CTX_use_certificate_file(ssl_ctx, cert_file.c_str(), SSL_FILETYPE_PEM) <= 0) {
                SPDLOG_ERROR("Error loading certificate file");
                exit(1);
            }

            if (::SSL_CTX_use_PrivateKey_file(ssl_ctx, key_file.c_str(), SSL_FILETYPE_PEM) <= 0 ) {
                SPDLOG_ERROR("Error loading certificate file");
                exit(1);
            }

            if (::SSL_CTX_check_private_key(ssl_ctx) != 1) {
                SPDLOG_ERROR("Private key does not match the certificate public key");
                exit(1);
            }
        }

        // set the verify mode to none
        // see https://www.openssl.org/docs/man3.2/man3/SSL_CTX_set_verify.html
        SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, ssl_verify_callback);
        SSL_CTX_set_verify_depth(ssl_ctx, 0);

        // from libpq be code
        /* disallow SSL session tickets */
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_TICKET);

        /* disallow SSL session caching, too */
        SSL_CTX_set_session_cache_mode(ssl_ctx, SSL_SESS_CACHE_OFF);

        /* disallow SSL compression */
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_COMPRESSION);

        return ssl_ctx;
    }

    void
    ProxyServer::_log_connect(SessionPtr session)
    {
        if (!_logger) {
            return;
        }
        // log the connection
        std::string endpoint = session->get_connection()->endpoint();

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "{} connected from: {} on socket {}",
                    (session->type() == Session::Type::CLIENT ? "Client" : "Server"), endpoint, session->get_connection()->get_socket());

        Logger::LogMsgType type;
        switch (session->type()) {
            case Session::Type::CLIENT:
                type = Logger::LogMsgType::FROM_CLIENT;
                break;
            case Session::Type::REPLICA:
                type = Logger::LogMsgType::FROM_REPLICA;
                break;
            case Session::Type::PRIMARY:
                type = Logger::LogMsgType::FROM_PRIMARY;
                break;
        }

        _logger->log_data(type, _id, session->id(), 0, '*', endpoint.length()+1, endpoint.c_str());
    }

    void
    ProxyServer::_log_disconnect(SessionPtr session)
    {
        if (!_logger) {
            return;
        }

        // log the connection
        std::string endpoint = session->get_connection()->endpoint();

        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "{} disconnected from: {} on socket {}",
                    (session->type() == Session::Type::CLIENT ? "Client" : "Server"), endpoint, session->get_connection()->get_socket());

        Logger::LogMsgType type;
        switch (session->type()) {
            case Session::Type::CLIENT:
                type = Logger::LogMsgType::FROM_CLIENT;
                break;
            case Session::Type::REPLICA:
                type = Logger::LogMsgType::FROM_REPLICA;
                break;
            case Session::Type::PRIMARY:
                type = Logger::LogMsgType::FROM_PRIMARY;
                break;
        }

        _logger->log_data(type, _id, session->id(), 0, '!', 0, nullptr);
    }

    void
    ProxyServer::_do_accept()
    {
        while (true) {
            // accept is non-blocking and will return when no more connections
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Accepting new connection");

            struct sockaddr_in client_address;
            socklen_t client_address_size = sizeof(client_address);

            // accept a new connection
            int client_socket = accept(_socket, (struct sockaddr*)&client_address, &client_address_size);
            if (client_socket == -1) {
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    std::cerr << "Error accepting connection: " << strerror(errno) << std::endl;
                }
                return;
            }

            int flags = 1;
            if (setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(int))) {
                std::cerr << "Error setting socket options: TCP_NODELAY" << std::endl;
                close(client_socket);
                return;
            }

            // create the connection and attach it to a client session
            ProxyConnectionPtr connection = std::make_shared<ProxyConnection>(client_socket, client_address);
            ClientSessionPtr session = std::make_shared<ClientSession>(connection, shared_from_this(), _shadow_mode);

            // add session to the waiting sessions list
            // and to the session map
            register_session(session, true);
        }
    }

    void
    ProxyServer::shutdown()
    {
        // send signal to shutdown
        _shutdown = true;
        char buf[1] = {0};
        write(_pipe[1], buf, 1);

        // other cleanup is down after while loop in run()
    }

    void
    ProxyServer::run()
    {
        while (!_shutdown) {
            // poll for readable sockets
            // lock the waiting sessions mutex
            std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);

            struct pollfd fds[2 + _waiting_sessions.size()];
            fds[0].fd = _socket;
            fds[0].events = POLLIN;

            fds[1].fd = _pipe[0];
            fds[1].events = POLLIN;

            int i = 2;
            for (auto &session_socket : _waiting_sessions) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Adding socket to poll list: {}", session_socket);
                fds[i].fd = session_socket;
                fds[i].events = POLLIN;
                i++;
            }

            lock.unlock();

            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Polling for sockets: size={}", _waiting_sessions.size());

            int n = poll(fds, i, -1);
            if (n == -1) {
                std::cerr << "Error polling sockets: " << strerror(errno) << std::endl;
                if (errno == EINTR || errno == EAGAIN) {
                    // if interrupted or no data, continue
                    continue;
                }
                break;
            }
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Poll returned: {}", n);

            // handle any new accepts
            if (fds[0].revents & POLLIN) {
                _do_accept();
                n--;
            }

            // read from the pipe; drain it
            if (fds[1].revents & POLLIN) {
                char buf[128];
                while (read(_pipe[0], buf, 128) > 0) {
                    // drain the pipe (we don't care about the data, just the signal)
                }
                n--;
            }

            // go through fds and find the sessions that are now runnable
            // remove them from waiting sessions list, insert them into runnable sessions list
            std::set<SessionPtr> runnable_sessions;

            std::unique_lock<std::mutex> lock2(_waiting_sessions_mutex);
            for (int i = 2; i < _sessions.size() + 2 && n > 0; i++) {
                if (fds[i].revents & POLLIN) {
                    auto session = _sessions.find(fds[i].fd);
                    PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Socket {} is now runnable", fds[i].fd);
                    if (session != _sessions.end()) {
                        runnable_sessions.insert(session->second);
                        _waiting_sessions.erase(fds[i].fd);
                    } else {
                        SPDLOG_ERROR("Socket {} not found in sessions map", fds[i].fd);
                        _waiting_sessions.erase(fds[i].fd);
                    }
                    n--;
                }
            }
            lock2.unlock();

            // queue the sessions that are now runnable
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Queueing {} sessions", runnable_sessions.size());
            for (auto &session : runnable_sessions) {
                _thread_pool.queue(session);
            }
        }

        // do cleanup
        SPDLOG_INFO("Proxy server shutting down");

        // close socket
        ::close(_socket);

        // close pipe
        ::close(_pipe[0]);
        ::close(_pipe[1]);

        // shutdown ssl
        if (_enable_ssl) {
            ::SSL_CTX_free(_ssl_ctx_server);
            ::SSL_CTX_free(_ssl_ctx_client);
        }

        // flush logger
        if (_logger) {
            _logger->flush();
        }
    }

    void
    ProxyServer::signal(ProxyConnectionPtr connection)
    {
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _waiting_sessions.insert(connection->get_socket());
        lock.unlock();

        char buf[1] = {0};
        write(_pipe[1], buf, 1);

        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Signaled server waiting on socket {}", connection->get_socket());
    }

    void
    ProxyServer::shutdown_session(SessionPtr session)
    {
        int socket = session->get_connection()->get_socket();
        _log_disconnect(session);
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _sessions.erase(socket);
        _waiting_sessions.erase(socket);
    }

    void
    ProxyServer::register_session(SessionPtr session,
                                  bool waiting_session_insert)
    {
        // all new sessions must be registered, so good place to log it
        _log_connect(session);

        // add the session to the list of sessions
        int socket = session->get_connection()->get_socket();
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _sessions.insert(std::make_pair(socket, session));
        if (waiting_session_insert) {
            _waiting_sessions.insert(socket);
        }
    }
} // namespace springtail::pg_proxy
