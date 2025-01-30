#pragma once

#include <memory>
#include <string>
#include <variant>
#include <cstdint>
#include <vector>
#include <queue>
#include <optional>
#include <deque>
#include <map>
#include <shared_mutex>

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
            MSG_CLIENT_SERVER_SIMPLE_QUERY=1, ///< simple query; data str: query
            MSG_CLIENT_SERVER_PARSE=2,        ///< parse packet; data buffer
            MSG_CLIENT_SERVER_BIND=3,         ///< bind packet; data buffer
            MSG_CLIENT_SERVER_DESCRIBE=4,     ///< describe packet; data buffer
            MSG_CLIENT_SERVER_EXECUTE=5,      ///< execute packet; data buffer
            MSG_CLIENT_SERVER_CLOSE=6,        ///< close packet; data buffer
            MSG_CLIENT_SERVER_SYNC=7,         ///< sync packet; data buffer
            MSG_CLIENT_SERVER_FUNCTION=8,     ///< function call; data buffer
            MSG_CLIENT_SERVER_FORWARD=10,     ///< forward packet; data buffer

            MSG_SERVER_CLIENT_FATAL_ERROR=99  ///< fatal error; no data
        };

        /** type to string map -- defined in session.cc */
        static const std::map<Type, std::string> type_map;

        /** Constructor with data */
        SessionMsg(Type type, QueryStmtPtr data, uint64_t seq_id)
            : _type(type), _data(data), _seq_id(seq_id)
        {
            if (data != nullptr) {
                _is_readsafe = data->is_read_safe;
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
            return _is_readsafe;
        }

        /** Clone the message */
        std::shared_ptr<SessionMsg> clone() {
            auto msg = std::make_shared<SessionMsg>(_type, _data, _seq_id);
            msg->_status = _status;
            msg->_completed = _completed;
            msg->_dependencies = _dependencies;
            msg->_is_readsafe = _is_readsafe;
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
        bool _is_readsafe=false;                 ///< is read-only
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
         * @brief Add a new batch to the queue
         */
        void push_batch(std::deque<T> new_batch) {
            std::unique_lock lock(_mutex);
            if (new_batch.empty()) {
                return;
            }
            _msg_queue.push(std::move(new_batch));
        }

        /**
         * @brief Load the next processing batch if current one is empty
         * @return true if one was loaded, or false if queue is empty
         */
        bool load_processing_batch() {
            std::unique_lock lock(_mutex);
            if (_processing_batch.empty()) {
                if (_msg_queue.empty()) {
                    return false;
                }
                _processing_batch = std::move(_msg_queue.front());
                _msg_queue.pop();
            }
            return true;
        }

        /** Get iterator to start of processing batch -- not thread safe */
        auto processing_batch_start() {
            return _processing_batch.begin();
        }

        /** Get iterator to end of processing batch -- not thread safe */
        auto processing_batch_end() {
            return _processing_batch.end();
        }

        /**
         * @brief Get first message from processing batch; without removing it
         * @return first message or nullopt if empty
         */
        std::optional<T> front_processing_msg() {
            std::shared_lock lock(_mutex);
            if (_processing_batch.empty()) {
                return std::nullopt;
            }
            return _processing_batch.front();
        }

        /**
         * @brief Pop/remove first message from processing batch
         * @return first message or nullopt if empty
         */
        std::optional<T> pop_processing_msg() {
            std::unique_lock lock(_mutex);
            if (_processing_batch.empty()) {
                return std::nullopt;
            }
            auto msg = _processing_batch.front();
            _processing_batch.pop_front();
            return msg;
        }

        /**
         * Check if batch being processed has more message
         * @returns true if processing batch is not empty, false if empty
         */
        bool processing_empty() {
            std::shared_lock lock(_mutex);
            return _processing_batch.empty();
        }

        /** Reset the queue to empty state */
        void reset() {
            std::unique_lock lock(_mutex);
            _processing_batch.clear();
            while (!_msg_queue.empty()) {
                _msg_queue.pop();
            }
        }

    private:
        std::shared_mutex _mutex;  ///< mutex for queues

        /** current batch of messages that are being processed, popped from queue */
        std::deque<T> _processing_batch;
        /** queue of pending message batches */
        std::queue<std::deque<T>> _msg_queue;
    };

} // namespace springtail::pg_proxy
