#pragma once

#include <boost/asio.hpp>

namespace springtail {
    class ProxyRequestHandler {
    public:
        ProxyRequestHandler() {}

        void process(const char *buffer, std::size_t size);
    };
}