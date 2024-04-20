#pragma once

#include <string>
#include <proxy/buffer.hh>

namespace springtail {
    class ProxyError {
    public:
        constexpr static std::string_view INVALID_PASSWORD = "28P01";
        constexpr static std::string_view SYNTAX_ERROR = "42601";
        constexpr static std::string_view PERMISSION_DENIED = "42501";

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

        static inline void
        decode_error(ProxyBuffer &buffer,
                     std::string &severity,
                     std::string &text,
                     std::string &error_code,
                     std::string &error_message)
        {
            char type = '\0';
            do {
                type = buffer.get();
                switch (type) {
                case 'S':
                    severity = buffer.getString();
                    break;
                case 'V':
                    text = buffer.getString();
                    break;
                case 'C':
                    error_code = buffer.getString();
                    break;
                case 'M':
                    error_message = buffer.getString();
                    break;
                case '\0':
                    break;
                default:
                    buffer.getString();
                    break;
                }
            } while (type != '\0');
        }
    };

}