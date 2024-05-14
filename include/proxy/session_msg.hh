#pragma once

#include <memory>
#include <string>
#include <variant>
#include <cstdint>
#include <vector>
#include <queue>
#include <optional>

#include <proxy/buffer_pool.hh>
#include <proxy/query_stmt_cache.hh>

namespace springtail {

    /** Messages between sessions */
    class SessionMsg {
    public:
        enum Type : int8_t {
            ///// client to server messages
            MSG_CLIENT_SERVER_STARTUP=0,
            // simple query; data str: query
            MSG_CLIENT_SERVER_SIMPLE_QUERY=1,
            // parse; data buffer
            MSG_CLIENT_SERVER_PARSE=2,
            // bind; data buffer
            MSG_CLIENT_SERVER_BIND=3,
            // describe; data buffer
            MSG_CLIENT_SERVER_DESCRIBE=4,
            // execute; data buffer
            MSG_CLIENT_SERVER_EXECUTE=5,
            // close; data buffer
            MSG_CLIENT_SERVER_CLOSE=6,

            ///// server to client messages
            // auth complete; no data
            MSG_SERVER_CLIENT_AUTH_DONE=50,
            // ready for query; data char: transaction status 'I', 'T', 'E'
            MSG_SERVER_CLIENT_READY=51,

            MSG_SERVER_CLIENT_FATAL_ERROR=99
        };

        // union of message types based on SessionMsg type
        using Data = std::variant<std::string, char, BufferPtr>;

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

        BufferPtr get_buffer() const {
            return std::get<BufferPtr>(_data);
        }

        const QueryStmtPtr peek_dependency() const {
            if (_dependencies.empty()) {
                return nullptr;
            }
            return _dependencies.front();
        }

        void pop_dependency() {
            _dependencies.pop();
        }

        bool has_provides() const {
            return !_provides.empty();
        }

        bool has_dependencies() const {
            return !_dependencies.empty();
        }

        const std::string &peek_provides() const {
            return _provides.front();
        }

        void pop_provides() {
            _provides.pop();
        }

        void add_dependency(QueryStmtPtr stmt) {
            _dependencies.push(stmt);
        }

        void add_provides(const std::string &hash) {
            _provides.push(hash);
        }

        static std::shared_ptr<SessionMsg> create(Type type, Data data='\0') {
            return std::make_shared<SessionMsg>(type, data);
        }

    private:
        Type _type;
        Data _data;
        std::queue<QueryStmtPtr> _dependencies; ///< query statements
        std::queue<std::string>  _provides; ///< hash of query plus name
    };
    using SessionMsgPtr = std::shared_ptr<SessionMsg>;

} // namespace springtail