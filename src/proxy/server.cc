#include <iostream>
#include <thread>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include <common/logging.hh>

#include <proxy/server.hh>
#include <proxy/client_session.hh>

#ifndef MSG_MORE
#define MSG_MORE 0 // not defined for OSX
#endif

namespace springtail {
    ProxyServer::ProxyServer(const std::string &address,
                             int port)
      : _request_handler(std::make_shared<ProxyRequestHandler>())
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

        if (listen(_socket, 16) < 0) {
            std::cerr << "Error listening on socket" << std::endl;
            close(_socket);
            exit(1);
        }

        SPDLOG_INFO("Proxy server listening on {}:{}", address, port);
    }

    void
    ProxyServer::run()
    {
        while (true) {
            struct sockaddr_in client_address;
            socklen_t client_address_size = sizeof(client_address);

            // Accept a new connection
            int client_socket = accept(_socket, (struct sockaddr*)&client_address, &client_address_size);
            if (client_socket == -1) {
                std::cerr << "Error accepting connection: " << strerror(errno) << std::endl;
                return;
            }

            int flags = 1;
            if (setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(int))) {
                std::cerr << "Error setting socket options: TCP_NODELAY" << std::endl;
                close(client_socket);
                continue;
            }

            // Get client IP address
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &(client_address.sin_addr), client_ip, INET_ADDRSTRLEN);

            std::cout << "Client connected from: " << client_ip << std::endl;

            ProxyConnectionPtr connection = std::make_shared<ProxyConnection>(client_socket, client_address);

            std::thread client_thread(&ProxyServer::_handle_connection, this, connection);
            client_thread.detach();
        }
    }

    void
    ProxyServer::_handle_connection(ProxyConnectionPtr connection)
    {
        // thread entry for new connection
        ClientSessionPtr session = std::make_shared<ClientSession>(connection, _request_handler);
        session->start();
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
    ProxyConnection::read_fully(ProxyBuffer &buffer, int size)
    {
        ssize_t n = read(buffer.next_data(), size, size);
        if (n > 0) {
            buffer.set_read(n);
        }
        return n;
    }

}