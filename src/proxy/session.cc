#include <memory>
#include <cassert>
#include <atomic>

#include <common/logging.hh>

#include <proxy/session.hh>
#include <proxy/server.hh>
#include <proxy/connection.hh>
#include <proxy/exception.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/logging.hh>
#include <proxy/session_msg.hh>
#include <proxy/client_session.hh>

namespace springtail::pg_proxy {

    /** unique session id counter */
    static std::atomic<uint64_t> session_id(1);

    /** thread local session variable */
    thread_local Session* _current_session = nullptr;

    /** map of message type to string */
    const std::map<SessionMsg::Type, std::string> SessionMsg::type_map = {
            {MSG_CLIENT_SERVER_SIMPLE_QUERY, "MSG_CLIENT_SERVER_SIMPLE_QUERY"},
            {MSG_CLIENT_SERVER_PARSE, "MSG_CLIENT_SERVER_PARSE"},
            {MSG_CLIENT_SERVER_BIND, "MSG_CLIENT_SERVER_BIND"},
            {MSG_CLIENT_SERVER_DESCRIBE, "MSG_CLIENT_SERVER_DESCRIBE"},
            {MSG_CLIENT_SERVER_EXECUTE, "MSG_CLIENT_SERVER_EXECUTE"},
            {MSG_CLIENT_SERVER_CLOSE, "MSG_CLIENT_SERVER_CLOSE"},
            {MSG_CLIENT_SERVER_SYNC, "MSG_CLIENT_SERVER_SYNC"},
            {MSG_CLIENT_SERVER_FUNCTION, "MSG_CLIENT_SERVER_FUNCTION"},
            {MSG_CLIENT_SERVER_FORWARD, "MSG_CLIENT_SERVER_FORWARD"},
            {MSG_SERVER_CLIENT_FATAL_ERROR, "MSG_SERVER_CLIENT_FATAL_ERROR"}
    };

    Session::Session(ProxyConnectionPtr connection)
        : _connection(connection),
          _state(STARTUP),
          _type(Type::CLIENT),
          _id(session_id++)
    {}

    Session::Session(DatabaseInstancePtr instance,
                     ProxyConnectionPtr connection,
                     UserPtr user,
                     const std::string &database,
                     const std::unordered_map<std::string, std::string> &parameters,
                     Type type)
        : _connection(connection),
          _state(STARTUP),
          _type(type),
          _user(user),
          _database(database),
          _instance(instance),
          _parameters(parameters),
          _id(session_id++)
    {
        auto optional_db_id = DatabaseMgr::get_instance()->get_database_id(_database);
        if (optional_db_id.has_value()) {
            _db_id = optional_db_id.value();
        } else {
            _state = ERROR;
        }
    }

    void
    Session::operator()()
    {
        if (_running.test_and_set()) {
            SPDLOG_ERROR("{} Session already running", name());
            assert(0);
            return;
        }

        // call child run method
        run(_fds, _notifications);

        // clear fds and notifications
        clear_fds();
        clear_notifications();

        // re-enable processing for this socket
        _running.clear();
        if (!_connection->closed()) {
            ProxyServer::get_instance()->signal(shared_from_this());
        }
    }

    void
    Session::instance_removed()
    {
        // for primary this is fatal, for replica it is not
        // either way notify the server so that it can notify
        // the session through the primary path
        ProxyServer::get_instance()->notify_session(_connection->get_socket(), NOTIFY_MSG::INSTANCE_SHUTDOWN);
    }

    std::pair<char,int32_t>
    Session::read_hdr(ProxyConnectionPtr connection)
    {
        char buffer[5];
        ssize_t n = connection->read(buffer, 5, 5); // read at most 5B
        CHECK_EQ(n, 5);

        // op code
        char code = buffer[0];
        // message length includes length field but not code byte
        // so really msg_length -= 4
        int32_t msg_length = recvint32(&buffer[1]) - 4;

        return {code, msg_length};
    }

    BufferPtr
    Session::read_msg(ProxyConnectionPtr connection, char code, int msg_length,
                      Session::Type type, uint64_t id, uint64_t seq_id)
    {
        // create buffer to read message, add 5 for code and length
        BufferPtr buffer = BufferPool::get_instance()->get(msg_length + 5);
        buffer->put(code);
        buffer->put32(msg_length + 4);  // add 4B for length field

        // read in the message from connection
        ssize_t n = connection->read(buffer->current_data(), msg_length, msg_length);
        CHECK_EQ(n, msg_length);
        buffer->set_size(msg_length + 5);

        // log the data, current_data points past header
        log_buffer(type, id, true, code, msg_length, buffer->current_data(), seq_id);

        return buffer;
    }

    void
    Session::read_msg(ProxyConnectionPtr connection, BufferList &blist)
    {
        char buffer[1024];
        int offset = 0;

        // read at least 5 bytes, more if available, read into
        // existing buffer to avoid doing multiple system calls
        ssize_t n = connection->read(buffer, 1024, 5);
        assert(n >= 5);

        ssize_t msg_length = 0;

        while (offset < n) {
            // code is first byte, skip over it
            // message length includes length field but not code byte
            // so really msg_length -= 4
            msg_length = recvint32(buffer + offset + 1) + 1;
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Read message length: {}", msg_length);

            // allocate a buffer from the buffer pool and copy data in
            BufferPtr bufferp = blist.get(msg_length);

            // copy data into buffer
            bufferp->copy_into(buffer + offset, std::min(n, msg_length));

            // incr by full message length instead of by n
            // this allows us to find out if we read too little for a full buffer
            offset += msg_length;
        }

        // if we didn't get all the data for the last buffer
        if (offset > n) {
            // read remaining data into tail buffer
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "Need to read more data for message: {}", offset-n);
            BufferPtr tail = blist.buffers.back();
            int rd = connection->read(tail->data() + tail->size(), offset-n, offset-n);
            tail->incr_size(rd);
            CHECK_EQ(rd, offset-n);
        }
    }

    void
    Session::_send_buffer(BufferPtr buffer, uint64_t seq_id)
    {
        // send the buffer to the server
        ssize_t n = _connection->write(buffer->data(), buffer->size());
        CHECK_EQ(n, buffer->size());

        // log the buffer; data should point past header (1B code + 4B length)
        // see: https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-MESSAGE-CONCEPTS
        _log_buffer(false, buffer->data()[0], buffer->size() - 5, buffer->data() + 5, seq_id);
    }

    void
    Session::_stream_to_remote_session(char code, int32_t msg_length, uint64_t seq_id)
    {
        assert(_is_shadow || _associated_session != nullptr);
        char buffer[4096];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);

        int n;
        if (!_is_shadow) {
            n = _associated_session->get_connection()->write(buffer, 5);
            CHECK_EQ(n, 5);
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Streamed header to remote session: code={}, msg_length={}", (_type == CLIENT ? 'C': 'S'), _id, code, msg_length);
        }

        // iterate reading buffer from local session and write to remote session
        while (msg_length > 0) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Reading {} bytes from local socket", (_type == CLIENT ? 'C': 'S'), _id, std::min(msg_length, 4096));

            // throws exception on error
            int read_length = std::min(msg_length, 4096);
            int n = _connection->read(buffer, read_length, read_length);
            CHECK_EQ(n, read_length);

            // log the buffer as incoming
            _log_buffer(true, code, n, buffer, seq_id, n == msg_length);

            if (!_is_shadow) {
                int m = _associated_session->get_connection()->write(buffer, n);
                CHECK_EQ(m, n);

                // log the buffer as outgoing from associated session
                _associated_session->_log_buffer(false, code, n, buffer, seq_id, n == msg_length);

                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Streamed {} bytes to remote session", (_type == CLIENT ? 'C': 'S'), _id, m);
            }

            msg_length -= n;
        }
    }

    void
    Session::_send_to_remote_session(char code, const BufferPtr buffer, uint64_t seq_id)
    {
        if (_state == RESET_SESSION) {
            // if we are in reset session state, we don't send any data
            return;
        }

        assert(_is_shadow || _associated_session != nullptr);

        if (!_is_shadow) {
            // send data
            ssize_t n = _associated_session->get_connection()->write(buffer->data(), buffer->size());
            CHECK_EQ(n, buffer->size());

            // log the buffer as outgoing from associated session
            // adjust buffer size and data to remove code and length (5B)
            assert(buffer->size() >= 5);
            _associated_session->_log_buffer(false, code, buffer->size() - 5, buffer->data() + 5, seq_id, true);
        }
    }

    void
    Session::_handle_error()
    {
        // atomic set flag to true, if already set then return
        if (test_and_set_shutdown()) {
            return;
        }

        SPDLOG_WARN("Error state, closing connection: type={} for session id={}\n",
                     _type == Type::PRIMARY ? "PRIMARY" : "CLIENT", _id);

        // shutdown the session, calls into child class
        shutdown_session();
    }

    void
    Session::_log_buffer(bool incoming, char code,
                         int32_t len, const char *data,
                         uint64_t seq_id, bool final)
    {
        log_buffer(_type, _id, incoming, code, len, data, seq_id, final);
    }

    void
    Session::log_buffer(Type type, uint64_t id, bool incoming,
                        char code, int32_t len, const char *data,
                        uint64_t seq_id, bool final)
    {
        LoggerPtr logger = ProxyServer::get_instance()->get_logger();
        if (logger ==  nullptr) {
            return;
        }

        Logger::LogMsgType log_type;
        if (type == Type::CLIENT) {
            log_type = incoming ? Logger::LogMsgType::FROM_CLIENT : Logger::LogMsgType::TO_CLIENT;
        } else if (type == Type::PRIMARY) {
            log_type = incoming ? Logger::LogMsgType::FROM_PRIMARY : Logger::LogMsgType::TO_PRIMARY;
        } else {
            log_type = incoming ? Logger::LogMsgType::FROM_REPLICA : Logger::LogMsgType::TO_REPLICA;
        }

        logger->log_data(log_type, ProxyServer::get_instance()->id(),
                         id, seq_id, code, len, data, final);
    }

} // namespace springtail::pg_proxy
