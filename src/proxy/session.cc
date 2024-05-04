#include <memory>
#include <cassert>
#include <atomic>

#include <common/logging.hh>

#include <proxy/session.hh>
#include <proxy/server.hh>
#include <proxy/connection.hh>
#include <proxy/exception.hh>
#include <proxy/buffer_pool.hh>

namespace springtail {

    /** unique session id counter */
    static std::atomic<uint64_t> session_id(0);

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

        SPDLOG_DEBUG("Processing data: session id: {}", _id);

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

            if (_waiting_on_session) {
                SPDLOG_DEBUG("Waiting on external session, id: {}", _id);
                // note: this will not add the connection back to the server
                // poll list. Once the associated session is done it will
                // call back into this session to continue processing
                // at that time it should be added back to the server poll list
                return;
            }

            // check if there is more data to process
            // checks buffered data in ssl connection
            has_data = _connection->has_pending();
        } while (has_data);

        // signal server to wait on this connection
        SPDLOG_DEBUG("Adding connection to server poll list: {}", _connection->get_socket());
        _server->signal(_connection);
    }

    void
    Session::_internal_process_msg(SessionMsgPtr msg)
    {
        // send message to session
        SPDLOG_DEBUG("Processing message: session id: {}", _id);

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

        // re-enable processing, add socket to poll list
        _enable_processing();
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
        ssize_t n = _connection->read(buffer, 5);
        assert(n >= 5);

        ssize_t msg_length = 0;

        while (offset < n) {
            // code is first byte, skip over it
            // message length includes length field but not code byte
            // so really msg_length -= 4
            msg_length = recvint32(buffer + offset + 1) + 1;

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
            SPDLOG_DEBUG("Need to read more data for message, this may block");
            BufferPtr tail = blist.buffers.back();
            int rd = _connection->read(tail->data() + tail->size(), offset-n, offset-n);
            assert(rd == offset-n);
        }
    }

    void
    Session::_stream_to_remote_session(char code, int32_t msg_length)
    {
        assert(_associated_session != nullptr);
        char buffer[4096];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);
        int n = _associated_session->get_connection()->write(buffer, 5);
        assert (n == 5);
        SPDLOG_DEBUG("Streamed header to remote session: code={}, msg_length={}", code, msg_length);

        // iterate reading buffer from local session and write to remote session
        while (msg_length > 0) {
            SPDLOG_DEBUG("Reading {} bytes from local socket", std::min(msg_length, 4096));
            // throws exception on error
            int n = _connection->read(buffer, std::min(msg_length, 4096));
            assert (n == std::min(msg_length, 4096));

            int m = _associated_session->get_connection()->write(buffer, n);
            assert (m == n);

            SPDLOG_DEBUG("Streamed {} bytes to remote session", m);

            msg_length -= m;
        }
    }

    void
    Session::_send_to_remote_session(char code, int32_t msg_length, const char *data)
    {
        assert(_associated_session != nullptr);
        char buffer[5];

        // first write the header, add 4 to msg length for size of length field
        buffer[0] = code;
        sendint32(msg_length+4, buffer + 1);
        int n = _associated_session->get_connection()->write(buffer, 5);
        assert (n == 5);

        n = _associated_session->get_connection()->write(data, msg_length);
        assert(n == msg_length);
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
            // notify client session of error
            SessionMsgPtr msg = std::make_shared<SessionMsg>(SessionMsg::MSG_SERVER_CLIENT_FATAL_ERROR);
            notify_client(msg);
        }
        SPDLOG_ERROR("Shutdown complete");
    }
}