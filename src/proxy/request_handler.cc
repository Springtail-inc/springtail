#include <iostream>

#include <common/logging.hh>

#include <proxy/request_handler.hh>

namespace springtail {

    void
    ProxyRequestHandler::process(const char *buffer,
                                 std::size_t size)
    {
        SPDLOG_DEBUG("Request handler read: {} bytes\n", size);

        for (int i = 0; i < size; i++) {
            std::cout << buffer[i];
        }
        std::cout << "\n";

        return;
    }

}