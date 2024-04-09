#pragma once
#include <memory>

namespace springtail {
    class ProxyRequestHandler {
    public:
        ProxyRequestHandler() {}

        void process(const char *buffer, std::size_t size);
    };
    using ProxyRequestHandlerPtr = std::shared_ptr<ProxyRequestHandler>;
}