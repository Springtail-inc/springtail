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

#include <common/logging.hh>
#include <common/thread_pool.hh>

#include <proxy/server.hh>
#include <proxy/client_session.hh>

#ifndef MSG_MORE
#define MSG_MORE 0 // not defined for OSX
#endif

namespace springtail {
    ProxyServer::ProxyServer(const std::string &address,
                             int port,
                             int thread_pool_size)
      : _request_handler(std::make_shared<ProxyRequestHandler>()),
        _user_mgr(std::make_shared<UserMgr>()),
        _thread_pool(thread_pool_size)
    {
        _socket = socket(AF_INET, SOCK_STREAM, 0);

        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = inet_addr(address.c_str());
        serv_addr.sin_port = htons(port);

        int flags = 1;
        if (setsockopt(_socket, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(int)) < 0) {
            std::cerr << "Error setting socket options: SO_REUSEADDR" << std::endl;
            close(_socket);
            exit(1);
        }

        if (bind(_socket, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "Error binding to socket" << std::endl;
            close(_socket);
            exit(1);
        }

        if (fcntl(_socket, F_SETFL, O_NONBLOCK) < 0) {
            std::cerr << "Error setting socket non-blocking" << std::endl;
            close(_socket);
            exit(1);
        }

        if (listen(_socket, 16) < 0) {
            std::cerr << "Error listening on socket" << std::endl;
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

        SPDLOG_INFO("Proxy server listening on {}:{}", address, port);
    }

    void
    ProxyServer::_do_accept()
    {
        while (true) { // accept is non-blocking and will return when no more connections
            SPDLOG_DEBUG("Accepting new connection");

            struct sockaddr_in client_address;
            socklen_t client_address_size = sizeof(client_address);

            // Accept a new connection
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

            // Get client IP address
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &(client_address.sin_addr), client_ip, INET_ADDRSTRLEN);

            SPDLOG_DEBUG("Client connected from: {} on socket {}", client_ip, client_socket);

            ProxyConnectionPtr connection = std::make_shared<ProxyConnection>(client_socket, client_address);
            ClientSessionPtr session = std::make_shared<ClientSession>(connection, shared_from_this());

            // add session to the waiting sessions list and to the session map
            std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
            _sessions.insert(std::make_pair(client_socket, session));
            _waiting_sessions.insert(client_socket);
            lock.unlock();
        }
    }

    void
    ProxyServer::run()
    {
        while (true) {
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
                SPDLOG_DEBUG("Adding socket to poll list: {}", session_socket);
                fds[i].fd = session_socket;
                fds[i].events = POLLIN;
                i++;
            }

            lock.unlock();

            SPDLOG_DEBUG("Polling for sockets: size={}", _waiting_sessions.size());

            int n = poll(fds, i, -1);
            if (n == -1) {
                std::cerr << "Error polling sockets: " << strerror(errno) << std::endl;
                break;
            }
            SPDLOG_DEBUG("Poll returned: {}", n);

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
                    SPDLOG_DEBUG("Socket {} is now runnable", fds[i].fd);
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
            SPDLOG_DEBUG("Queueing {} sessions", runnable_sessions.size());
            for (auto &session : runnable_sessions) {
                _thread_pool.queue(session);
            }
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

        SPDLOG_DEBUG("Signaled server waiting on socket {}", connection->get_socket());
    }

    void
    ProxyServer::shutdown_session(SessionPtr session)
    {
        int socket = session->get_connection()->get_socket();
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _sessions.erase(socket);
        _waiting_sessions.erase(socket);
    }

    void
    ProxyServer::register_session(SessionPtr session)
    {
        std::unique_lock<std::mutex> lock(_waiting_sessions_mutex);
        _sessions.insert(std::make_pair(session->get_connection()->get_socket(), session));
    }

    ssize_t
    ProxyConnection::write(const char *buffer, int size, bool more)
    {
        ssize_t bytes_written = 0;
        do {
            ssize_t n = send(_socket, buffer, size, more ? MSG_MORE : 0);

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
        ssize_t bytes_read = 0;
        do {
            ssize_t n = recv(_socket, buffer, max_size, 0);
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