#include <iostream>
#include <thread>
#include <sys/socket.h>
#include <sys/eventfd.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>

#include <absl/log/log.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <fmt/core.h>

#include <common/logging.hh>
#include <common/thread_pool.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>

#include <proxy/client_session.hh>
#include <proxy/server.hh>
#include <proxy/logger.hh>
#include <proxy/logging.hh>

namespace springtail::pg_proxy {

    /** Default log level for the proxy server */
    LogLevel proxy_log_level = LOG_LEVEL_DEBUG1;

    void
    ProxyServer::init(int proxy_port,
                      int thread_pool_size,
                      const std::filesystem::path &cert_file,
                      const std::filesystem::path &key_file,
                      MODE mode,
                      bool enable_ssl,
                      LoggerPtr shadow_logger)
    {
        _thread_pool = std::make_shared<ThreadPool<Session>>(thread_pool_size);
        _id = arc4random();
        _enable_ssl = enable_ssl;
        _mode = mode;
        _logger = shadow_logger;

        // Initialize SSL
        if (_enable_ssl) {
            _ssl_ctx_server = _setup_SSL_context(cert_file, key_file);
            _ssl_ctx_client = _setup_SSL_context();
        }

        _socket = socket(AF_INET6, SOCK_STREAM, 0);

        struct sockaddr_in6 serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin6_family = AF_INET6;
        serv_addr.sin6_addr = in6addr_any;
        serv_addr.sin6_port = htons(proxy_port);

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


        if ((_efd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE)) < 0) {
            SPDLOG_ERROR("Error creating eventfd\n");
            close(_socket);
            exit(1);
        }

        // ignore SIGPIPE signals
        ::signal(SIGPIPE, SIG_IGN);

        DatabaseMgr::get_instance()->init();
        UserMgr::get_instance()->init(USER_MGR_SLEEP_INTERVAL_SECS);

        SPDLOG_INFO("Proxy server initialized and is listening on port={}", proxy_port);
    }

    void
    ProxyServer::set_log_level(int log_level)
    {
        proxy_log_level = static_cast<LogLevel>(log_level);
    }

    /** Callback to get more info about what is going on in SSL */
    [[maybe_unused]] static void
    ssl_info_callback(const SSL *s, int where, int ret)
    {
        [[maybe_unused]] const char *str;
        int w = where & ~SSL_ST_MASK;

        ProxyConnection *conn = static_cast<ProxyConnection *>(SSL_get_ex_data(s, 0));
        [[maybe_unused]] int fd = conn->get_socket();

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
    ProxyServer::log_connect(SessionPtr session)
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
            default:
                LOG(FATAL) << "Invalid session type: " << session->type();
        }

        _logger->log_data(type, _id, session->id(), 0, '*', endpoint.length()+1, endpoint.c_str());
    }

    void
    ProxyServer::log_disconnect(SessionPtr session)
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
            default:
                LOG(FATAL) << "Invalid session type: " << session->type();
        }

        _logger->log_data(type, _id, session->id(), 0, '!', 0, nullptr);
    }

    void
    ProxyServer::_do_accept()
    {
        while (true) {
            // accept is non-blocking and will return when no more connections
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Accepting new connection");

            // accept a new connection
            int client_socket = accept(_socket, nullptr, nullptr);
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
            ProxyConnectionPtr connection = std::make_shared<ProxyConnection>(client_socket);
            ClientSessionPtr session = std::make_shared<ClientSession>(connection);
            log_connect(session);

            // add session to the waiting sessions list
            // and to the session map
            register_session(session, nullptr, client_socket, true);
        }
    }

    void
    ProxyServer::_internal_shutdown()
    {
        // send signal to shutdown
        SPDLOG_INFO("Proxy server shutting down");
        _shutdown = true;
        _wake_event_loop();
        // other cleanup is down after while loop in run()
    }

    void
    ProxyServer::run()
    {
        std::set<SessionPtr, Session::SessionComparator> runnable_sessions;

        while (!_shutdown) {
            // poll for readable sockets
            // lock the waiting sessions mutex
            std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);

            struct pollfd fds[2 + _waiting_sessions.size()];
            fds[0].fd = _socket;
            fds[0].events = POLLIN;

            fds[1].fd = _efd;
            fds[1].events = POLLIN;

            int nfds = 2;
            for (auto &session_socket : _waiting_sessions) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Adding socket to poll list: {}", session_socket);
                fds[nfds].fd = session_socket;
                fds[nfds].events = POLLIN;
                fds[nfds].revents = 0;
                nfds++;
            }

            lock.unlock();

            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Polling for sockets: size={}", _waiting_sessions.size());

            int n = poll(fds, nfds, -1);
            if (n == -1) {
                SPDLOG_ERROR("Error polling sockets: errno={}", strerror(errno));
                if (errno == EINTR || errno == EAGAIN) {
                    // if interrupted or no data, continue
                    continue;
                }
                break;
            }
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Poll returned: {}", n);

            // handle any new accepts
            if (fds[0].revents & POLLIN) {
                _do_accept();
                n--;
            }

            // read from the eventfd; drain it
            if (fds[1].revents & POLLIN) {
                // drain the pipe (we don't care about the data, just the signal)
                uint64_t val;
                [[maybe_unused]] int ret = read(_efd, &val, 8);
                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Draining eventfd: {}", ret);
                n--;
            }

            // go through fds and find the sessions that are now session
            // remove them from waiting sessions list, insert them into session sessions list
            std::unique_lock<std::mutex> lock2(_waiting_sessions_mutex);
            for (int i = 2; i < nfds + 2 && n > 0; i++) {
                if (fds[i].revents & POLLIN) {
                    int fd = fds[i].fd;
                    auto session_itr = _sessions.find(fd);
                    PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Socket {} is now runnable", fd);

                    // lookup fd in sessions map
                    if (session_itr != _sessions.end()) {
                        // find session object and insert into session sessions
                        auto session = session_itr->second;
                        // for this session update the set of fds that have data
                        session->add_fd(fd);
                        // insert into set of runnable sessions
                        auto ins_res = runnable_sessions.insert(session);
                        if (!ins_res.second) {
                            // already inserted session into runnable set, socket should not be waiting_sessions
                            DCHECK_EQ(_waiting_sessions.erase(fd), 0);
                        } else {
                            // remove all associated sockets from the waiting sessions list
                            auto it = _session_sockets.find(session);
                            CHECK(it != _session_sockets.end());

                            for (auto socket : it->second) {
                                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Removing socket {} from waiting sessions for {}", socket, session->name());
                                CHECK_EQ(_waiting_sessions.erase(socket), 1);
                            }
                        }
                    } else {
                        SPDLOG_WARN("Socket {} not found in sessions map", fd);
                        CHECK_EQ(_waiting_sessions.erase(fd), 1);
                    }
                    n--;
                }
            }
            lock2.unlock();

            // queue the sessions that are now session
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Queueing {} sessions", runnable_sessions.size());
            for (auto &session : runnable_sessions) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Queueing session {}", session->name());
                _thread_pool->queue(session);
            }

            runnable_sessions.clear();
        }

        SPDLOG_INFO("Proxy server shutting down");

        // close socket
        ::close(_socket);

        // close eventfd
        ::close(_efd);

        // shutdown ssl
        if (_enable_ssl) {
            ::SSL_CTX_free(_ssl_ctx_client);
        }

        _thread_pool->shutdown();
        pg_proxy::UserMgr::get_instance()->stop_thread();
        UserMgr::shutdown();
        DatabaseMgr::shutdown();

        // flush logger
        if (_logger) {
            _logger->flush();
        }
        SPDLOG_INFO("Proxy server finished cleanup");
    }

    void
    ProxyServer::signal(SessionPtr session)
    {
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        session->clear_fds();

        // lookup session in the session map
        auto session_itr = _session_sockets.find(session);
        CHECK(session_itr != _session_sockets.end());

        // go through the list of sockets and add them back to the waiting sessions list
        for (auto socket: session_itr->second) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Signaled server waiting on socket {}", socket);
            _waiting_sessions.insert(socket);
        }
         lock.unlock();

        // signal the eventfd to wake up the main loop
        _wake_event_loop();
    }

    void
    ProxyServer::shutdown_session(SessionPtr session)
    {
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _internal_shutdown_session(session, lock);
        lock.unlock();

        _wake_event_loop();
    }

    void
    ProxyServer::_internal_shutdown_session(SessionPtr session, std::unique_lock<std::mutex> &lock)
    {
        CHECK(lock.owns_lock());

        auto session_itr = _session_sockets.find(session);
        if (session_itr == _session_sockets.end()) {
            SPDLOG_WARN("Session not found in session sockets map: {}", session->name());
            return;
        }

        // go through the list of sockets and remove them
        for (auto socket: session_itr->second) {
            _waiting_sessions.erase(socket);
            _sessions.erase(socket);
        }

        // remove the session from the sockets map (session->socket)
        _session_sockets.erase(session_itr);
    }

    void
    ProxyServer::register_session(SessionPtr new_session,
                                  SessionPtr old_session,
                                  int socket,
                                  bool waiting_session_insert)
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Registering socket {} with session: {}", socket, new_session->name());

        if (old_session != nullptr) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG4, "De-registering session: {}", old_session->name());
        }

        // add the session to the list of sessions
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);

        // first do a removal based on socket
        if (old_session != nullptr) {
            _internal_shutdown_session(old_session, lock);
        }

        // double check the socket isn't registered
        _sessions.erase(socket);

        // then do the insert; <socket, session>
        auto ins_res = _sessions.insert(std::make_pair(socket, new_session));
        CHECK(ins_res.second);

        // then do the insert; <session, vector:socket>
        auto res = _session_sockets.insert(std::make_pair(new_session, std::vector<int>()));
        res.first->second.push_back(socket);

        if (waiting_session_insert) {
            _waiting_sessions.insert(socket);
        } else {
            int n = _waiting_sessions.erase(socket);
            DCHECK_EQ(n, 0);
        }
        lock.unlock();

        _wake_event_loop();
    }

    void
    ProxyServer::_wake_event_loop()
    {
        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Waking up event loop");
        uint64_t val = 1;
        [[maybe_unused]] int ret = write(_efd, &val, sizeof(uint64_t));
        PROXY_DEBUG(LOG_LEVEL_DEBUG4, "Wrote to eventfd: {}", ret);
    }

} // namespace springtail::pg_proxy
