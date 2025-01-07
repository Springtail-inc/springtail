#pragma once

#include <memory>
#include <string>
#include <variant>
#include <cstdint>
#include <vector>
#include <queue>
#include <optional>

#include <proxy/buffer_pool.hh>
#include <proxy/history_cache.hh>

namespace springtail::pg_proxy {

    /** Messages between sessions */
    class SessionMsg {
    public:
        struct MsgStatus {
            char transaction_status; ///< transaction status 'I', 'T', 'E'
        };

        /** message types -- add string defn to session.cc type_map */
        enum Type : int8_t {
            ///// client to server messages
            MSG_CLIENT_SERVER_STARTUP=0,      ///< startup; do auth, etc.; no data
            MSG_CLIENT_SERVER_SIMPLE_QUERY=1, ///< simple query; data str: query
            MSG_CLIENT_SERVER_PARSE=2,        ///< parse packet; data buffer
            MSG_CLIENT_SERVER_BIND=3,         ///< bind packet; data buffer
            MSG_CLIENT_SERVER_DESCRIBE=4,     ///< describe packet; data buffer
            MSG_CLIENT_SERVER_EXECUTE=5,      ///< execute packet; data buffer
            MSG_CLIENT_SERVER_CLOSE=6,        ///< close packet; data buffer
            MSG_CLIENT_SERVER_SYNC=7,         ///< sync packet; data buffer
            MSG_CLIENT_SERVER_SHUTDOWN=8,     ///< shutdown packet; no data
            MSG_CLIENT_SERVER_FORWARD=10,     ///< forward packet; data buffer
            MSG_CLIENT_SERVER_INIT_PARAMS=11, ///< init params; data buffer

            ///// server to client messages
            MSG_SERVER_CLIENT_AUTH_DONE=50,   ///< auth complete; no data
            MSG_SERVER_CLIENT_READY=51,       ///< ready for query; MsgStatus data
            MSG_SERVER_CLIENT_MSG_SUCCESS=52, ///< message response; success
            MSG_SERVER_CLIENT_MSG_ERROR=53,   ///< message response; success
            MSG_SERVER_CLIENT_COPY_READY=54,  ///< ready to receive copy data; no data
            MSG_SERVER_CLIENT_FATAL_ERROR=99  ///< fatal error; no data
        };

        /** type to string map -- defined in session.cc */
        static const std::map<Type, std::string> type_map;

        /** Constructor with data */
        SessionMsg(Type type, QueryStmtPtr data, uint64_t seq_id)
            : _type(type), _data(data), _seq_id(seq_id)
        {}

        SessionMsg(Type type, MsgStatus &status)
            : _type(type), _status(status)
        {}

        SessionMsg(Type type, BufferPtr buffer, uint64_t seq_id)
            : _type(type), _buffer(buffer), _seq_id(seq_id)
        {}

        /** Constructor without data */
        SessionMsg(Type type, uint64_t seq_id=-1)
            : _type(type), _seq_id(seq_id)
        {}

        /** Get type */
        Type type() const { return _type; }

        std::string const type_str() const {
            return type_map.at(_type);
        }

        const QueryStmtPtr data() const {
            return _data;
        }

        /** Get status */
        const MsgStatus &status() const {
            return _status;
        }

        const BufferPtr buffer() const {
            return _buffer;
        }

        /** Get query statement ptr from dependency queue; otherwise nullptr if none */
        const QueryStmtPtr peek_dependency() const {
            if (_dependencies.empty()) {
                return nullptr;
            }
            return _dependencies.front();
        }

        /** Get dependency at index */
        QueryStmtPtr get_dependency(size_t idx) const {
            return _dependencies.at(idx);
        }

        /** Number of dependencies */
        size_t num_dependencies() const {
            return _dependencies.size();
        }

        /** Add a query statement to dependency queue */
        void add_dependency(QueryStmtPtr stmt) {
            _dependencies.push_back(stmt);
        }

        /** Set dependencies by moving from input vector */
        void set_dependencies(std::vector<QueryStmtPtr> &&dependencies) {
            _dependencies = std::move(dependencies);
        }

        /** Set message status */
        void set_status_ready(MsgStatus &status) {
            _status = status;
            _type = MSG_SERVER_CLIENT_READY;
        }

        void set_msg_response(bool success, int completed=0) {
            _completed = completed;
            if (success) {
                _type = Type::MSG_SERVER_CLIENT_MSG_SUCCESS;
            } else {
                _type = Type::MSG_SERVER_CLIENT_MSG_ERROR;
            }
        }

        /** Get number of completed queries */
        int completed() const {
            return _completed;
        }

        /** Get sequence id */
        uint64_t seq_id() const {
            return _seq_id;
        }

        /** Clone the message */
        std::shared_ptr<SessionMsg> clone() {
            auto msg = std::make_shared<SessionMsg>(_type, _data, _seq_id);
            msg->_status = _status;
            msg->_completed = _completed;
            msg->_dependencies = _dependencies;
            return msg;
        }

        /** Helper to create a session message */
        static std::shared_ptr<SessionMsg> create(Type type, QueryStmtPtr data=nullptr, uint64_t seq_id=-1) {
            return std::make_shared<SessionMsg>(type, data, seq_id);
        }

        static std::shared_ptr<SessionMsg> create(Type type, BufferPtr data, uint64_t seq_id) {
            return std::make_shared<SessionMsg>(type, data, seq_id);
        }

        static std::shared_ptr<SessionMsg> create(Type type, uint64_t seq_id) {
            return std::make_shared<SessionMsg>(type, seq_id);
        }

    private:
        Type _type;                              ///< message type
        QueryStmtPtr _data=nullptr;              ///< message data
        BufferPtr _buffer;                       ///< buffer for message data
        MsgStatus _status;                       ///< message status
        int _completed=0;                        ///< number of completed queries (for multi-statement queries)
        std::vector<QueryStmtPtr> _dependencies; ///< query statements
        uint64_t _seq_id=0;                      ///< sequence id for this message
    };
    using SessionMsgPtr = std::shared_ptr<SessionMsg>;

} // namespace springtail::pg_proxy
