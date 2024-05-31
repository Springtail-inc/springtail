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

namespace springtail {

    /** Messages between sessions */
    class SessionMsg {
    public:
        struct MsgStatus {
            char transaction_status; ///< transaction status 'I', 'T', 'E'
        };

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
            MSG_CLIENT_SERVER_FORWARD=8,      ///< forward packet; data buffer

            ///// server to client messages
            MSG_SERVER_CLIENT_AUTH_DONE=50,   ///< auth complete; no data
            MSG_SERVER_CLIENT_READY=51,       ///< ready for query; MsgStatus data
            MSG_SERVER_CLIENT_MSG_SUCCESS=52, ///< message response; success
            MSG_SERVER_CLIENT_MSG_ERROR=53,   ///< message response; success

            MSG_SERVER_CLIENT_FATAL_ERROR=99  ///< fatal error; no data
        };

        /** Constructor with data */
        SessionMsg(Type type, QueryStmtPtr data)
            : _type(type), _data(data)
        {}

        SessionMsg(Type type, MsgStatus &status)
            : _type(type), _status(status)
        {}

        SessionMsg(Type type, BufferPtr buffer)
            : _type(type), _buffer(buffer)
        {}

        /** Constructor without data */
        SessionMsg(Type type)
            : _type(type)
        {}

        /** Get type */
        Type type() const { return _type; }

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

        int completed() const {
            return _completed;
        }

        /** Helper to create a session message */
        static std::shared_ptr<SessionMsg> create(Type type, QueryStmtPtr data=nullptr) {
            return std::make_shared<SessionMsg>(type, data);
        }

        static std::shared_ptr<SessionMsg> create(Type type, BufferPtr data) {
            return std::make_shared<SessionMsg>(type, data);
        }

    private:
        Type _type;                              ///< message type
        QueryStmtPtr _data=nullptr;              ///< message data
        BufferPtr _buffer;                       ///< buffer for message data
        MsgStatus _status;                       ///< message status
        int _completed=0;                        ///< number of completed queries (for multi-statement queries)
        std::vector<QueryStmtPtr> _dependencies; ///< query statements
    };
    using SessionMsgPtr = std::shared_ptr<SessionMsg>;

} // namespace springtail