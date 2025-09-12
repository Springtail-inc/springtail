#include <iostream>
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

#include <admin_http/admin_server.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <common/redis.hh>
#include <common/redis_types.hh>
#include <common/thread_pool.hh>

#include <proxy/client_session.hh>
#include <proxy/logger.hh>
#include <proxy/server.hh>
#include <proxy/server_session.hh>

namespace springtail::pg_proxy {

    void
    ProxyServer::init(int proxy_port,
                      int thread_pool_size,
                      const std::filesystem::path &cert_file,
                      const std::filesystem::path &key_file,
                      int keep_alive_port,
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
            LOG_ERROR("Error setting socket options: SO_REUSEADDR\n");
            close(_socket);
            exit(1);
        }

        if (bind(_socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            LOG_ERROR("Error binding to socket\n");
            close(_socket);
            exit(1);
        }

        if (fcntl(_socket, F_SETFL, O_NONBLOCK) < 0) {
            LOG_ERROR("Error setting socket non-blocking\n");
            close(_socket);
            exit(1);
        }

        if (listen(_socket, 16) < 0) {
            LOG_ERROR("Error listening on socket\n");
            close(_socket);
            exit(1);
        }

        if ((_efd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE)) < 0) {
            LOG_ERROR("Error creating eventfd\n");
            close(_socket);
            exit(1);
        }

        // ignore SIGPIPE signals
        ::signal(SIGPIPE, SIG_IGN);

        if (keep_alive_port > 0) {
            _keep_alive_thread = std::thread(&ProxyServer::_start_keep_alive, this, keep_alive_port);
        }

        // register "/proxy" route with AdminServer
        AdminServer::get_instance()->register_get_route(
            "/proxy",
            [this]([[maybe_unused]] const std::string &path, [[maybe_unused]] const httplib::Params &params, nlohmann::json &json_response){
                if (!ProxyServer::_has_instance()) {
                    json_response =  R"({})";
                    return;
                }
                json_response = {
                    {"socket_fd", _socket},
                    {"event_fd", _efd},
                    {"id", _id},
                    {"sessions", nlohmann::json::object()}
                };
                for (const auto &[fd, session]: _sessions) {
                    json_response["sessions"][std::to_string(fd)] = {
                        {"name", session->name() },
                        {"database", std::format("{}:{}", session->database(),session->database_id())},
                        {"ready", session->is_ready()}
                    };
                }
            });
        LOG_INFO("Proxy server initialized and is listening on port={}", proxy_port);
    }

    void
    ProxyServer::_start_keep_alive(int port)
    {
        int socket = ::socket(AF_INET6, SOCK_STREAM, 0);
        if (socket < 0) {
            LOG_ERROR("Error creating keepalive socket");
            return;
        }

        struct sockaddr_in6 addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port);

        int flags = 1;
        if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(int)) < 0) {
            LOG_ERROR("Error setting keepalive socket options: SO_REUSEADDR");
            ::close(socket);
            return;
        }

        if (bind(socket, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
            LOG_ERROR("Error binding keepalive socket");
            ::close(socket);
            return;
        }

        if (listen(socket, 1) < 0) {
            LOG_ERROR("Error listening on keepalive socket");
            ::close(socket);
            return;
        }

        LOG_INFO("Keepalive socket listening on port {}", port);

        while (!_shutdown) {
            int client = accept(socket, nullptr, nullptr);
            if (client >= 0) {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Accepted keepalive connection");
                // Wait for remote disconnect with timeout to check shutdown flag
                char buf[32];
                fd_set readfds;
                struct timeval tv;

                while (!_shutdown) {
                    FD_ZERO(&readfds);
                    FD_SET(client, &readfds);
                    tv.tv_sec = 1;  // 1 second timeout
                    tv.tv_usec = 0;

                    int ret = select(client + 1, &readfds, nullptr, nullptr, &tv);
                    if (ret < 0) {
                        break;  // Error
                    } else if (ret == 0) {
                        continue;  // Timeout, check shutdown flag
                    }

                    // Data available to read
                    ssize_t n = read(client, buf, sizeof(buf));
                    if (n <= 0) break;  // EOF or error
                }
                ::close(client);
            }
        }

        ::close(socket);
        LOG_INFO("Keepalive socket closed, keepalive thread exiting");
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
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_HANDSHAKE_STARTED): fd={}", fd);
        } else if (where & SSL_CB_HANDSHAKE_DONE) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_HANDSHAKE_DONE): fd={}", fd);
        } else if (where & SSL_CB_LOOP) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_LOOP): fd={}, {}:{}", fd, str, SSL_state_string_long(s));
        } else if (where & SSL_CB_ALERT) {
            str = (where & SSL_CB_READ) ? "read" : "write";
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_ALERT): fd={}, SSL3 alert {}:{}:{}", fd, str,
                         SSL_alert_type_string_long(ret),
                         SSL_alert_desc_string_long(ret));
       } else if (where & SSL_CB_EXIT) {
            if (ret == 0) {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_EXIT): fd={}, {}:failed in {}", fd, str, SSL_state_string_long(s));
            } else if (ret < 0) {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info (CB_EXIT): fd={}, {}:error in {}", fd, str, SSL_state_string_long(s));
            }
        } else {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL info callback: fd={}, where={:#X}, ret={}", fd, where, ret);
        }
    }

    static int
    ssl_verify_callback(int preverify_ok, X509_STORE_CTX *x509_ctx)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "SSL verify callback: preverify_ok={}", preverify_ok);
        return 1; // no verification
    }

    SSL_CTX *
    ProxyServer::_setup_SSL_context(const std::filesystem::path &cert_file,
                                    const std::filesystem::path &key_file)
    {
        // create the SSL context
        SSL_CTX *ssl_ctx = ::SSL_CTX_new(::TLS_method());
        if (!ssl_ctx) {
            LOG_ERROR("Error creating SSL context");
            exit(1);
        }

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_DEBUG
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
                LOG_ERROR("Error loading certificate file");
                exit(1);
            }

            if (::SSL_CTX_use_PrivateKey_file(ssl_ctx, key_file.c_str(), SSL_FILETYPE_PEM) <= 0 ) {
                LOG_ERROR("Error loading certificate file");
                exit(1);
            }

            if (::SSL_CTX_check_private_key(ssl_ctx) != 1) {
                LOG_ERROR("Private key does not match the certificate public key");
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

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "{} connected from: {} on socket {}",
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

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "{} disconnected from: {} on socket {}",
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
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Accepting new connection");

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
        AdminServer::get_instance()->deregister_get_route("/proxy");
        LOG_INFO("Proxy server shutting down");
        notify_shutdown();
        _proxy_thread.join();
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
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Adding socket to poll list: {}", session_socket);
                fds[nfds].fd = session_socket;
                fds[nfds].events = POLLIN;
                fds[nfds].revents = 0;
                nfds++;
            }

            lock.unlock();

            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Polling for sockets: size={}", _waiting_sessions.size());

            int n = poll(fds, nfds, -1);
            if (n == -1) {
                LOG_ERROR("Error polling sockets: errno={}", strerror(errno));
                if (errno == EINTR || errno == EAGAIN) {
                    // if interrupted or no data, continue
                    continue;
                }
                break;
            }
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Poll returned: {}", n);

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
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG3, "Draining eventfd: {}", ret);
                n--;
            }

            // go through fds and find the sessions that are now session
            // remove them from waiting sessions list, insert them into session sessions list
            std::unique_lock<std::mutex> lock2(_waiting_sessions_mutex);
            for (int i = 2; i < nfds + 2 && n > 0; i++) {
                if (fds[i].revents & POLLIN) {
                    int fd = fds[i].fd;
                    auto session_itr = _sessions.find(fd);
                    LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Socket {} is now runnable", fd);

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
                                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Removing socket {} from waiting sessions for {}", socket, session->name());
                                _waiting_sessions.erase(socket);
                            }
                        }
                    } else {
                        LOG_WARN("Socket {} not found in sessions map", fd);
                        CHECK_EQ(_waiting_sessions.erase(fd), 1);
                    }
                    n--;
                }
            }
            lock2.unlock();

            DCHECK_EQ(n, 0);

            // queue the sessions that are now session
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Queueing {} sessions", runnable_sessions.size());
            for (auto &session : runnable_sessions) {
                LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Queueing session {}", session->name());
                _thread_pool->queue(session);
            }

            runnable_sessions.clear();
        }

        LOG_INFO("Proxy server shutting down");

        // close socket
        ::close(_socket);

        // close eventfd
        ::close(_efd);

        // shutdown ssl
        if (_enable_ssl) {
            ::SSL_CTX_free(_ssl_ctx_client);
        }

        // shutdown thread pool
        _thread_pool->shutdown();

        // wait for keepalive thread to finish
        if (_keep_alive_thread.joinable()) {
            _keep_alive_thread.join();
        }

        // flush logger
        if (_logger) {
            _logger->flush();
        }
        LOG_INFO("Proxy server finished cleanup");
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
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Signaled server waiting on socket {}", socket);
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
        if (session_itr != _session_sockets.end()) {
            // this is the primary session used for lookup
            // go through the list of sockets and remove them
            for (auto socket: session_itr->second) {
                _waiting_sessions.erase(socket);
                _sessions.erase(socket);
            }

            // remove the session from the sockets map (session->socket)
            _session_sockets.erase(session_itr);
            return;
        }

        // this is a secondary session, most likely a server session
        // need to remove its socket from the appropriate maps
        int socket = session->get_connection()->get_socket();

        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Session not found in session sockets, removing socket: {}", socket);

        _waiting_sessions.erase(socket);

        // find the primary session by socket
        auto itr = _sessions.find(socket);
        if (itr == _sessions.end()) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Socket {} not found in sessions map", socket);
            return;
        }

        // do a lookup in the session sockets list
        auto primary_itr = _session_sockets.find(itr->second);
        if (primary_itr != _session_sockets.end()) {
            // remove the socket from the vector of sockets associated with the primary session
            primary_itr->second.erase(std::remove(primary_itr->second.begin(),
                primary_itr->second.end(), socket), primary_itr->second.end());
        }
    }

    void
    ProxyServer::register_session(SessionPtr new_session,
                                  SessionPtr old_session,
                                  int socket,
                                  bool waiting_session_insert)
    {
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Registering socket {} with session: {}", socket, new_session->name());

        if (old_session != nullptr) {
            LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "De-registering session: {}", old_session->name());
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
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Waking up event loop");
        uint64_t val = 1;
        [[maybe_unused]] int ret = write(_efd, &val, sizeof(uint64_t));
        LOG_DEBUG(LOG_PROXY, LOG_LEVEL_DEBUG4, "Wrote to eventfd: {}", ret);
    }

    void
    ProxyServer::start()
    {
        bool force_shadow = springtail_retreive_argument<bool>(ServiceId::ProxyServerId, "force_shadow").value();
        bool force_primary = springtail_retreive_argument<bool>(ServiceId::ProxyServerId, "force_primary").value();

        nlohmann::json json = Properties::get(Properties::PROXY_CONFIG);
        int num_threads = Json::get_or<int>(json, "threads", 4);
        int port = Json::get_or<int>(json, "port", 8888);
        int keep_alive_port = Json::get_or<int>(json, "keep_alive_port", 0);

        // setup ssl config
        bool enable_ssl = Json::get_or<bool>(json, "enable_ssl", false);
        std::filesystem::path certificate = Json::get_or<std::filesystem::path>(json, "cert_path", "");
        std::filesystem::path key = Json::get_or<std::filesystem::path>(json, "key_path", "");
        if (enable_ssl &&
            (!std::filesystem::exists(certificate) || !std::filesystem::exists(key))) {
            throw Error("Certificate/key file does not exist and ssl is enabled");
        }

        if (!enable_ssl) {
            LOG_INFO("SSL Disabled");
        }

        // setup the log path
        LoggerPtr logger = nullptr;
        std::filesystem::path log = Json::get_or<std::filesystem::path>(json, "shadow_log_path", "");
        if (!log.empty()) {
            std::fstream log_file;
            try {
                // create and truncate the file
                log_file.open(log, std::ios::out | std::ios::trunc | std::ios::binary);
                log_file.close();
            } catch (const std::ios_base::failure &e) {
                throw Error(fmt::format("Error creating shadow log file {}: {}", log, e.what()));
            }

            LOG_INFO("Logging initialized to: {}", log.string());
            logger = std::make_shared<Logger>(log, 1024*1024*100, 5);
        } else {
            LOG_INFO("Shadow logging disabled: log={}", log.string());
        }

        ProxyServer::MODE server_mode = ProxyServer::MODE::NORMAL;
        std::string mode = Json::get_or<std::string>(json, "mode", "normal");

        // overrides from command line (for debugging)
        if (force_primary) {
            mode = "primary";
        } else if (force_shadow) {
            mode = "shadow";
        }

        // setup the mode
        if (mode == "shadow") {
            server_mode = ProxyServer::MODE::SHADOW;
            CHECK_NE(logger, nullptr);
        } else if (mode == "normal") {
            server_mode = ProxyServer::MODE::NORMAL;
        } else if (mode == "primary") {
            server_mode = ProxyServer::MODE::PRIMARY;
        } else {
            throw Error("Invalid mode specified");
        }

        get_instance()->init(port, num_threads, certificate, key, keep_alive_port, server_mode, enable_ssl, logger);

        get_instance()->_proxy_thread = std::thread(&ProxyServer::run, ProxyServer::get_instance());

        // force user manager to startup user sync thread
        UserMgr::get_instance();
    }

} // namespace springtail::pg_proxy
