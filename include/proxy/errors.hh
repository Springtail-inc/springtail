#include <proxy/buffer.hh>

namespace springtail {
    class ProxyError {
    public:
        constexpr static char INVALID_PASSWORD[] = "28P01";

        static inline void encode_error(ProxyBuffer &buffer,
                          const char *error_code,
                          const std::string &error_message) {
            buffer.put('E');
            buffer.put32(23 + strlen(error_code) + error_message.size());
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