#pragma once

#include <string>
#include <proxy/buffer.hh>

namespace springtail {
    class ProxyError {
    public:
        constexpr static std::string_view INVALID_PASSWORD = "28P01";

        static inline void
        encode_error(ProxyBuffer &buffer,
                     const std::string_view error_code,
                     const std::string_view error_message)
        {
            buffer.put('E');
            buffer.put32(23 + error_code.size() + error_message.size());
            buffer.putString("SERROR");
            buffer.putString("VERROR");
            buffer.put('C');
            buffer.putString(error_code);
            buffer.put('M');
            buffer.putString(error_message);
            buffer.put(0);
        }
    };

}