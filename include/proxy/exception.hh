#pragma once

#include <common/exception.hh>

namespace springtail {

    class ProxyError : public Error {
    public:
        ProxyError() { }
        ProxyError(const std::string &msg)
            : Error(msg)
        {}
    };

    class ProxyIOError : public ProxyError {
    public:
        const char *what() const noexcept {
            return "Fatal IO error";
        }
    };

    class ProxyIOConnectionError : public ProxyIOError {
    public:
        const char *what() const noexcept {
            return "Connection failed";
        }
    };

    class ProxySSLConnectionError : public ProxyIOError {
    public:
        const char *what() const noexcept {
            return "SSL connection failed";
        }
    };

    class ProxyAuthError : public ProxyError {
    public:
        const char *what() const noexcept {
            return "Authentication failed";
        }
    };

} // namespace springtail