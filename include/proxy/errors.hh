#pragma once

#include <string>
#include <proxy/buffer_pool.hh>

namespace springtail {
namespace pg_proxy {

    /**
     * @brief Class for encoding and decoding error messages from postgres
     */
    class ProxyProtoError {
    public:
        // for full error codes: https://www.postgresql.org/docs/16/errcodes-appendix.html
        constexpr static std::string_view INVALID_PASSWORD = "28P01";
        constexpr static std::string_view SYNTAX_ERROR = "42601";
        constexpr static std::string_view PERMISSION_DENIED = "42501";
        constexpr static std::string_view INVALID_DATABASE = "3D000";
        constexpr static std::string_view CONNECTION_FAILURE = "08006";

        static inline void
        encode_error(BufferPtr buffer,
                     const std::string_view error_code,
                     const std::string_view error_message,
                     const std::string severity = "ERROR")
        {
            buffer->put('E');
            buffer->put32(11 + severity.size() + error_code.size() + error_message.size());
            buffer->put('S');
            buffer->put_string(severity);
            buffer->put('C');
            buffer->put_string(error_code);
            buffer->put('M');
            buffer->put_string(error_message);
            buffer->put(0);
        }

        static inline void
        decode_error(BufferPtr &buffer,
                     std::string &severity,
                     std::string &text,
                     std::string &error_code,
                     std::string &error_message)
        {
            char type = '\0';
            do {
                type = buffer->get();
                switch (type) {
                case 'S':
                    severity = buffer->get_string();
                    break;
                case 'V':
                    text = buffer->get_string();
                    break;
                case 'C':
                    error_code = buffer->get_string();
                    break;
                case 'M':
                    error_message = buffer->get_string();
                    break;
                case '\0':
                    break;
                default:
                    buffer->get_string();
                    break;
                }
            } while (type != '\0');
        }
    };
} // namespace pg_proxy
} // namespace springtail
