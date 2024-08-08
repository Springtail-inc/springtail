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

    /** map of message type to string */
    const std::map<SessionMsg::Type, std::string> SessionMsg::type_map = {
            {MSG_CLIENT_SERVER_STARTUP, "MSG_CLIENT_SERVER_STARTUP"},
            {MSG_CLIENT_SERVER_SIMPLE_QUERY, "MSG_CLIENT_SERVER_SIMPLE_QUERY"},
            {MSG_CLIENT_SERVER_PARSE, "MSG_CLIENT_SERVER_PARSE"},
            {MSG_CLIENT_SERVER_BIND, "MSG_CLIENT_SERVER_BIND"},
            {MSG_CLIENT_SERVER_DESCRIBE, "MSG_CLIENT_SERVER_DESCRIBE"},
            {MSG_CLIENT_SERVER_EXECUTE, "MSG_CLIENT_SERVER_EXECUTE"},
            {MSG_CLIENT_SERVER_CLOSE, "MSG_CLIENT_SERVER_CLOSE"},
            {MSG_CLIENT_SERVER_SYNC, "MSG_CLIENT_SERVER_SYNC"},
            {MSG_CLIENT_SERVER_FORWARD, "MSG_CLIENT_SERVER_FORWARD"},
            {MSG_SERVER_CLIENT_AUTH_DONE, "MSG_SERVER_CLIENT_AUTH_DONE"},
            {MSG_SERVER_CLIENT_READY, "MSG_SERVER_CLIENT_READY"},
            {MSG_SERVER_CLIENT_MSG_SUCCESS, "MSG_SERVER_CLIENT_MSG_SUCCESS"},
            {MSG_SERVER_CLIENT_MSG_ERROR, "MSG_SERVER_CLIENT_MSG_ERROR"},
            {MSG_SERVER_CLIENT_FATAL_ERROR, "MSG_SERVER_CLIENT_FATAL_ERROR"}
    };

    Session::Session(ProxyConnectionPtr connection,
                     ProxyServerPtr server,
                     Type type)
        : _connection(connection),
          _server(server),
          _state(STARTUP),
          _type(type),
          _id(session_id++)
    {}

    Session::Session(DatabaseInstancePtr instance,
                     ProxyConnectionPtr connection,
                     ProxyServerPtr server,
                     UserPtr user,
                     const std::string &database,
                     Type type)
        : _connection(connection),
          _server(server),
          _state(STARTUP),
          _type(type),
          _user(user),
          _database(database),
          _instance(instance),
          _id(session_id++)
    {}

    void
    Session::operator()()
    {
        // thread entry point from server
        bool has_data = false;

        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[{}:{}] Processing data", (_type == CLIENT ? 'C': 'S'), _id);

        do {
            // thread entry point
            try {
                _process_connection();
            } catch (const ProxyIOError &e) {
                SPDLOG_ERROR("ProxyIOError: {}", e.what());
                _state = ERROR;
            } catch (const std::exception &e) {
                SPDLOG_ERROR("Exception: {}", e.what());
                _state = ERROR;
            } catch (...) {
                SPDLOG_ERROR("Unknown exception");
                _state = ERROR;
            }

            // cleanup connection and remove from server list if closed or error
            if (_state == ERROR || _connection->closed()) {
                _handle_error();
                return;
            }

            // see if remote session has messages that need to be processed
            if (_associated_session != nullptr) {
                _associated_session->_internal_process_msgs(true);
            }

            // if this is a client session with a shadow replica then process those messages
            if (_type == CLIENT) {
                ClientSession *client = static_cast<ClientSession*>(this);
                ServerSessionPtr shadow = client->get_shadow_session();
                assert (shadow == nullptr || shadow != _associated_session);
                if (shadow != nullptr && shadow != _associated_session) {
                    shadow->_internal_process_msgs(true);
                }
            }

            if (_waiting_on_session) {
                PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[{}:{}] Waiting on external session", (_type == CLIENT ? 'C': 'S'), _id);
                // note: this will not add the connection back to the server
                // poll list. Once the associated session is done it will
                // call back into this session to continue processing
                // at that time it should be added back to the server poll list

                return;
            }

            // check if we have messages pending that still need to be processed
            _internal_process_msgs(false);

            // check if there is more data to process
            // checks buffered data in ssl connection
            has_data = _connection->has_pending();
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Has data: {}", (_type == CLIENT ? 'C': 'S'), _id, has_data);
        } while (has_data);

        // signal server to wait on this connection
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Adding connection to server poll list: socket={}", (_type == CLIENT ? 'C': 'S'), _id, _connection->get_socket());
        _server->signal(_connection);
    }

    void
    Session::_internal_process_msgs(bool is_remote)
    {
        while (_ready_for_message) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[{}:{}] Looking for messages", (_type == CLIENT ? 'C': 'S'), _id);

            SessionMsgPtr msg = get_msg();
            if (msg == nullptr) {
                break;
            }

            // send message to session
            PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[{}:{}] Processing message: type: {}", (_type == CLIENT ? 'C': 'S'), _id, msg->type_str());

            try {
                _process_msg(msg);
            } catch (const ProxyIOError &e) {
                SPDLOG_ERROR("ProxyIOError: {}", e.what());
                _state = ERROR;
            } catch (const std::exception &e) {
                SPDLOG_ERROR("Exception: {}", e.what());
                _state = ERROR;
            } catch (...) {
                SPDLOG_ERROR("Unknown exception");
                _state = ERROR;
            }

            if (_state == ERROR || _connection->closed()) {
                _handle_error();
                return;
            }
        }

        // re-enable processing, add socket to poll list
        if (is_remote) {
            _enable_processing();
        }
    }

    void
    Session::_enable_processing()
    {
        if (_waiting_on_session) {
            // if we are waiting on a session, we should not be processing
            // incoming data from our connection
            assert(_associated_session != nullptr);
            return;
        }

        // add session connection to server poll list
        _server->signal(_connection);
    }

    UserLoginPtr
    Session::_get_user_login()
    {
        if (_user == nullptr) {
            return nullptr;
        }
        return _user->get_user_login();
    }

    std::pair<char,int32_t>
    Session::_read_hdr()
    {
        char buffer[5];
        ssize_t n = _connection->read(buffer, 5, 5); // read at most 5B
        assert(n == 5);

        // op code
        char code = buffer[0];
        // message length includes length field but not code byte
        // so really msg_length -= 4
        int32_t msg_length = recvint32(&buffer[1]) - 4;

        return {code, msg_length};
    }

    void
    Session::_read_msg(BufferList &blist)
    {
        char buffer[1024];
        int offset = 0;

        // read at least 5 bytes, more if available, read into
        // existing buffer to avoid doing multiple system calls
        ssize_t n = _connection->read(buffer, 1024, 5);
        assert(n >= 5);

        ssize_t msg_length = 0;

        while (offset < n) {
            // code is first byte, skip over it
            // message length includes length field but not code byte
            // so really msg_length -= 4
            msg_length = recvint32(buffer + offset + 1) + 1;
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Read message length: {}", (_type == CLIENT ? 'C': 'S'), _id, msg_length);

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
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Need to read more data for message: {}", (_type == CLIENT ? 'C': 'S'), _id, offset-n);
            BufferPtr tail = blist.buffers.back();
            int rd = _connection->read(tail->data() + tail->size(), offset-n, offset-n);
            tail->incr_size(rd);
            assert(rd == offset-n);
        }
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
            assert (n == 5);
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Streamed header to remote session: code={}, msg_length={}", (_type == CLIENT ? 'C': 'S'), _id, code, msg_length);
        }

        // iterate reading buffer from local session and write to remote session
        while (msg_length > 0) {
            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Reading {} bytes from local socket", (_type == CLIENT ? 'C': 'S'), _id, std::min(msg_length, 4096));

            // throws exception on error
            int n = _connection->read(buffer, std::min(msg_length, 4096));
            assert (n == std::min(msg_length, 4096));

            // log the buffer as incoming
            _log_buffer(true, code, n, buffer, seq_id, n == msg_length);

            if (!_is_shadow) {
                int m = _associated_session->get_connection()->write(buffer, n);
                assert (m == n);

                // log the buffer as outgoing from associated session
                _associated_session->_log_buffer(false, code, n, buffer, seq_id, n == msg_length);

                PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Streamed {} bytes to remote session", (_type == CLIENT ? 'C': 'S'), _id, m);
            }

            msg_length -= n;
        }
    }

    void
    Session::_send_to_remote_session(char code, int32_t msg_length, const char *data, uint64_t seq_id)
    {
        assert(_is_shadow || _associated_session != nullptr);
        char buffer[5];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);

        if (!_is_shadow) {
            // send header
            int n = _associated_session->get_connection()->write(buffer, 5);
            assert (n == 5);

            // send data
            n = _associated_session->get_connection()->write(data, msg_length);
            assert(n == msg_length);

            // log the buffer as outgoing from associated session
            _associated_session->_log_buffer(false, code, msg_length, data, seq_id, true);
        }
    }

    void
    Session::_handle_error()
    {
        bool shutting_down = _shut_down_flag.test_and_set();
        if (shutting_down) {
            return;
        }

        // general error handling
        // cleanup this session and check for associated session
        SPDLOG_ERROR("Error state, closing connection: type={} for session id={}",
                     _type == Type::PRIMARY ? "PRIMARY" : "CLIENT", _id);
        std::cout << this << std::endl;

        // close connection
        _connection->close();

        // let server handle full cleanup
        _server->shutdown_session(shared_from_this());

        // check for associated client session, and notify of error
        if ((_type == Type::PRIMARY || _type == Type::REPLICA) && _associated_session != nullptr) {
            // notify client session of error; treat this as an interrupt of sorts
            SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR);
            _associated_session->_msg_queue.clear();
            _associated_session->_ready_for_message = true;
            queue_msg(msg);
            _associated_session->_internal_process_msgs(true);
        }
        SPDLOG_ERROR("Shutdown complete");
    }

    void
    Session::_log_buffer(bool incoming, char code,
                         int32_t len, const char *data,
                         uint64_t seq_id, bool final)
    {
        LoggerPtr logger = _server->get_logger();
        if (logger ==  nullptr) {
            return;
        }

        if (seq_id == -1) {
            seq_id = _gen_seq_id();
        }

        Logger::LogMsgType log_type;

        if (_type == Type::CLIENT) {
            log_type = incoming ? Logger::LogMsgType::FROM_CLIENT : Logger::LogMsgType::TO_CLIENT;
        } else if (_type == Type::PRIMARY) {
            log_type = incoming ? Logger::LogMsgType::FROM_PRIMARY : Logger::LogMsgType::TO_PRIMARY;
        } else {
            log_type = incoming ? Logger::LogMsgType::FROM_REPLICA : Logger::LogMsgType::TO_REPLICA;
        }

        logger->log_data(log_type, _server->id(), _id, seq_id, code, len, data, final);
    }

} // namespace springtail::pg_proxy
