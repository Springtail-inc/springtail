#pragma once

#include <memory>
#include <string>
#include <variant>
#include <cstdint>

namespace springtail {

    /** Messages between sessions */
    class SessionMsg {
    public:
        enum Type : int8_t {
            ///// client to server messages
            MSG_CLIENT_SERVER_STARTUP=0,
            // simple query; data str: query
            MSG_CLIENT_SERVER_SIMPLE_QUERY=1,
            // parse; data str: prepared statement name
            MSG_CLIENT_SERVER_PARSE=2,
            // bind; data str: portal name
            MSG_CLIENT_SERVER_BIND=3,
            // describe; data str: portal name
            MSG_CLIENT_SERVER_DESCRIBE=4,
            // execute; data str: portal name
            MSG_CLIENT_SERVER_EXECUTE=5,

            ///// server to client messages
            // auth complete; no data
            MSG_SERVER_CLIENT_AUTH_DONE=50,
            // ready for query; data char: transaction status 'I', 'T', 'E'
            MSG_SERVER_CLIENT_READY=51,

            MSG_SERVER_CLIENT_FATAL_ERROR=99
        };

        // union of message types based on SessionMsg type
        using Data = std::variant<std::string, char,
            std::pair<std::string,std::string>>;

        SessionMsg(Type type, Data data)
            : _type(type), _data(data)
        {}

        SessionMsg(Type type)
            : _type(type)
        {}

        Type type() const { return _type; }

        const std::string &get_str() const {
            return std::get<std::string>(_data);
        }

        char get_char() const {
            return std::get<char>(_data);
        }

        const std::pair<std::string,std::string> &get_str_pair() const {
            return std::get<std::pair<std::string,std::string>>(_data);
        }

    private:
        Type _type;
        Data _data;
    };
    using SessionMsgPtr = std::shared_ptr<SessionMsg>;

} // namespace springtail