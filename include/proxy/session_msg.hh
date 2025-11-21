#pragma once

#include <shared_mutex>

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
            NONE=0,                           ///< no message
            ///// client to server messages
            MSG_CLIENT_SERVER_SIMPLE_QUERY=1, ///< simple query; data str: query
            MSG_CLIENT_SERVER_EXTENDED=2,     ///< extended packet (parse/bind/describe/execute/close/sync/flush); data buffer
            MSG_CLIENT_SERVER_FUNCTION=3,     ///< function call; data buffer
            MSG_CLIENT_SERVER_FORWARD=4,      ///< forward packet; data buffer
            MSG_CLIENT_SERVER_FLUSH=5,        ///< flush packet; data buffer
            MSG_CLIENT_SERVER_STATE_REPLAY=6, ///< state replay packet; dependencies

            MSG_SERVER_CLIENT_FATAL_ERROR=99  ///< fatal error; no data
        };

        /** type to string map -- defined in session.cc */
        static const std::map<Type, std::string> type_map;

        /** Constructor with data */
        SessionMsg(Type type, QueryStmtPtr data, uint64_t seq_id)
            : _type(type), _data(data), _seq_id(seq_id)
        {
            if (data != nullptr) {
                _is_read_safe = data->is_read_safe;
            }
        }

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

        /** Get buffer */
        const BufferPtr buffer() const {
            return _buffer;
        }

        /** Get dependencies as query statements */
        const std::vector<QueryStmtPtr> &qs_dependencies() const {
            return _dependencies;
        }

        /** Convert dependencies to session messages and return as vector */
        std::vector<std::shared_ptr<SessionMsg>> dependencies() const
        {
            std::vector<std::shared_ptr<SessionMsg>> deps;
            for (const auto& qs : _dependencies) {
                // For simple queries, create a simple query message
                if (qs->data_type == QueryStmt::DataType::SIMPLE) {
                    deps.emplace_back(std::make_shared<SessionMsg>(MSG_CLIENT_SERVER_SIMPLE_QUERY, qs, _seq_id));
                } else if (qs->data_type == QueryStmt::DataType::PACKET) {
                    deps.emplace_back(std::make_shared<SessionMsg>(MSG_CLIENT_SERVER_EXTENDED, qs->buffer(), _seq_id));
                } else {
                    // should not happen; other types not valid for dependencies
                    LOG_ERROR("Invalid dependency data type in session message");
                    DCHECK(false) << "Invalid dependency data type in session message";
                }
            }

            return deps;
        }

        /** Number of dependencies */
        size_t num_dependencies() const {
            return _dependencies.size();
        }

        /** Add a query statement to dependency queue */
        void add_dependency(QueryStmtPtr stmt) {
            _dependencies.push_back(stmt);
        }

        /** Add dependencies by moving from input vector */
        void add_dependencies(std::vector<QueryStmtPtr> &&dependencies) {
            _dependencies.insert(_dependencies.end(),
                                 std::make_move_iterator(dependencies.begin()),
                                 std::make_move_iterator(dependencies.end()));
        }

        /** Set dependencies by moving from input vector */
        void set_dependencies(std::vector<QueryStmtPtr> &&dependencies) {
            _dependencies = std::move(dependencies);
        }

        void set_msg_response(int completed=0) {
            _completed = completed;
        }

        /** Get number of completed queries */
        int completed() const {
            return _completed;
        }

        /** Get sequence id */
        uint64_t seq_id() const {
            return _seq_id;
        }

        bool is_read_safe() const {
            return _is_read_safe;
        }

        /** Clone the message */
        std::shared_ptr<SessionMsg> clone() {
            auto msg = std::make_shared<SessionMsg>(_type, _data, _seq_id);
            msg->_status = _status;
            msg->_completed = _completed;
            msg->_dependencies = _dependencies;
            msg->_is_read_safe = _is_read_safe;
            return msg;
        }

        /**
         * @brief Check if message is a replay message
         *
         * messages that contain dependencies that must be replayed
         * to restore session and transaction state before executing queries.
         * @returns true if message is a replay message, false otherwise
         */
        bool is_dependency_message() const {
            return _type == MSG_CLIENT_SERVER_STATE_REPLAY;
        }

        /**
         * @brief Check if message is a simple query
         * @return true if message is a simple query, false otherwise
         */
        bool is_simple_query_message() const {
            return _type == MSG_CLIENT_SERVER_SIMPLE_QUERY;
        }

        /**
         * @brief Check if message is a sync message
         * @return true if message is a sync message, false otherwise
         */
        bool is_sync_message() const {
            return _type == MSG_CLIENT_SERVER_EXTENDED && _data != nullptr &&
                   _data->type == QueryStmt::Type::SYNC;
        }

        /**
         * @brief Check if message is a flush message
         * @return true if message is a flush message, false otherwise
         */
        bool is_flush_message() const {
            return _type == MSG_CLIENT_SERVER_FLUSH;
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
        bool _is_read_safe=false;                ///< is read-only
    };
    using SessionMsgPtr = std::shared_ptr<SessionMsg>;


    /**
     * Message queue for session messages from client to server
     * Provides a batch interface for messages that can be run without
     * waiting for a response from the server.
     */
    template<typename T>
    class SessionMsgQueue {
    public:
        /** Constructor */
        SessionMsgQueue() = default;

        /**
         * @brief Add a new batch to the queue, maintains batch boundaries
         * @param new_batch New batch of messages to add to the queue
         */
        void push_batch(std::deque<T> new_batch) {
            std::unique_lock lock(_mutex);
            if (new_batch.empty()) {
                return;
            }
            _msg_queue.push(std::move(new_batch));
        }

        /**
         * Reset the queue to empty state
         */
        void reset() {
            std::unique_lock lock(_mutex);
            while (!_msg_queue.empty()) {
                _msg_queue.pop();
            }
        }

        /**
         * Is msg queue empty
         */
        bool empty() {
            std::shared_lock lock(_mutex);
            return _msg_queue.empty();
        }

        /**
         * Get size of msg queue
         */
        size_t size() {
            std::shared_lock lock(_mutex);
            return _msg_queue.size();
        }

        /**
         * Pop the next batch from the queue for processing
         * Returns the batch by value (moved) to avoid dangling references.
         */
        std::deque<T> pop_batch() {
            std::unique_lock lock(_mutex);
            if (_msg_queue.empty()) {
                return {};
            }
            auto front = std::move(_msg_queue.front());
            _msg_queue.pop();
            return front;
        }

    private:
        std::shared_mutex _mutex;  ///< mutex for queues

        /** current batch of messages that are being processed, popped from queue */
        std::deque<T> _processing_batch;
        /** queue of pending message batches */
        std::queue<std::deque<T>> _msg_queue;
    };

} // namespace springtail::pg_proxy
