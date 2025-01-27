#include <atomic>
#include <cassert>
#include <memory>

#include <common/logging.hh>
#include <proxy/buffer_pool.hh>
#include <proxy/client_session.hh>
#include <proxy/connection.hh>
#include <proxy/exception.hh>
#include <proxy/logging.hh>
#include <proxy/server.hh>
#include <proxy/session.hh>
#include <proxy/session_msg.hh>

namespace springtail::pg_proxy {

/** unique session id counter */
static std::atomic<uint64_t> session_id(1);

/** thread local session variable */
thread_local Session *_current_session = nullptr;

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
    {MSG_CLIENT_SERVER_SHUTDOWN, "MSG_CLIENT_SERVER_SHUTDOWN"},
    {MSG_CLIENT_SERVER_INIT_PARAMS, "MSG_CLIENT_SERVER_INIT_PARAMS"},
    {MSG_SERVER_CLIENT_AUTH_DONE, "MSG_SERVER_CLIENT_AUTH_DONE"},
    {MSG_SERVER_CLIENT_READY, "MSG_SERVER_CLIENT_READY"},
    {MSG_SERVER_CLIENT_MSG_SUCCESS, "MSG_SERVER_CLIENT_MSG_SUCCESS"},
    {MSG_SERVER_CLIENT_MSG_ERROR, "MSG_SERVER_CLIENT_MSG_ERROR"},
    {MSG_SERVER_CLIENT_FATAL_ERROR, "MSG_SERVER_CLIENT_FATAL_ERROR"},
    {MSG_SERVER_CLIENT_COPY_READY, "MSG_SERVER_CLIENT_COPY_READY"},
    {MSG_SERVER_CLIENT_FORWARD, "MSG_SERVER_CLIENT_FORWARD"}};

Session::Session(ProxyConnectionPtr connection, ProxyServerPtr server)
    : _connection(connection),
      _server(server),
      _state(STARTUP),
      _type(Type::CLIENT),
      _id(session_id++)
{
}

Session::Session(DatabaseInstancePtr instance,
                 ProxyConnectionPtr connection,
                 ProxyServerPtr server,
                 UserPtr user,
                 const std::string &database,
                 const std::unordered_map<std::string, std::string> &parameters,
                 Type type)
    : _connection(connection),
      _server(server),
      _state(STARTUP),
      _type(type),
      _user(user),
      _database(database),
      _instance(instance),
      _id(session_id++),
      _parameters(parameters)
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
    bool signal = true;

    // set thread local session
    _current_session = this;

    std::unique_lock<std::mutex> lock(_session_mutex, std::defer_lock);
    if (lock.try_lock()) {
        // do actual processing
        signal = _process(lock);

        // unlock prior to signalling server
        lock.unlock();
    } else {
        // failed to lock, session is already running
        SPDLOG_WARN("[{}:{}] Session already running", type_str(), _id);
    }

    _current_session = nullptr;

    // check once more for any messages (with lock unlocked) and
    // current session set to null to force lock
    if (!is_msg_queue_empty()) {
        _internal_process_msgs(false);
    }

    if (signal) {
        // signal server to wait on this connection
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Adding connection to poll list: socket={}",
                    type_str(), _id, _connection->get_socket());

        _server->signal(_connection);
    }
}

bool
Session::_process(std::unique_lock<std::mutex> &lock)
{
    // thread entry point from server
    bool has_data = false;

    PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[{}:{}] Processing data (lock)", type_str(), _id);
    do {
        // thread entry point
        try {
            if (has_data || _connection->has_pending()) {
                // if there is data then process it
                _process_connection();
            }
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
            _handle_error();  // this will set shutdown flag
            // don't return yet, let other session process pending msgs
        }

        // see if remote session has messages that need to be processed
        // if this is a primary or replica session, check the client session
        if (_type == PRIMARY || _type == REPLICA) {
            // check if the client session has messages to process
            ServerSession *server = static_cast<ServerSession *>(this);
            SessionPtr client = server->get_client_session();
            if (client && !client->is_msg_queue_empty()) {
                lock.unlock();
                client->_internal_process_msgs(true);
                lock.lock();
            }
        }

        // if this is a client session, check server sessions
        if (_type == CLIENT) {
            ClientSession *client = static_cast<ClientSession *>(this);
            ServerSessionPtr primary = client->get_primary_session();
            if (primary && !primary->is_msg_queue_empty()) {
                lock.unlock();
                primary->_internal_process_msgs(true);
                lock.lock();
            }
            ServerSessionPtr replica = client->get_replica_session();
            if (replica && !replica->is_msg_queue_empty()) {
                lock.unlock();
                replica->_internal_process_msgs(true);
                lock.lock();
            }
        }

        // safe to return now if shutting down
        if (is_shutdown()) {
            return false;
        }

        // check if we have messages pending that still need to be processed
        _internal_process_msgs(false);

        // check if there is more data to process
        // checks buffered data in ssl connection
        has_data = _connection->has_pending();
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Has data: {}", (_type == CLIENT ? 'C' : 'S'), _id,
                    has_data);
    } while (has_data);

    return true;
}

void
Session::_internal_process_msgs(bool is_remote)
{
    std::unique_lock<std::mutex> lock(_session_mutex, std::defer_lock);
    if (_current_session != this) {
        // this is called to pass a message from one session to another
        // this is normally called with is_remote = true (if _current_session != this)
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Processing remote message, getting lock",
                    (_type == CLIENT ? 'C' : 'S'), _id);
        // lock to process messages
        lock.lock();
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[{}:{}] Processing messages (lock), is_remote={}",
                    (_type == CLIENT ? 'C' : 'S'), _id, is_remote);
    }

    while (_ready_for_message) {
        PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[{}:{}] Looking for messages", (_type == CLIENT ? 'C' : 'S'),
                    _id);

        SessionMsgPtr msg = get_msg();
        if (msg == nullptr) {
            break;
        }

        // send message to session
        PROXY_DEBUG(LOG_LEVEL_DEBUG1, "[{}:{}] Processing message: type: {}",
                    (_type == CLIENT ? 'C' : 'S'), _id, msg->type_str());

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

    PROXY_DEBUG(LOG_LEVEL_DEBUG2, "[{}:{}] done with processing messages",
                (_type == CLIENT ? 'C' : 'S'), _id);
}

void
Session::_enable_processing()
{
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

std::pair<char, int32_t>
Session::_read_hdr()
{
    char buffer[5];
    ssize_t n = _connection->read(buffer, 5, 5);  // read at most 5B
    CHECK_EQ(n, 5);

    // op code
    char code = buffer[0];
    // message length includes length field but not code byte
    // so really msg_length -= 4
    int32_t msg_length = recvint32(&buffer[1]) - 4;

    if (msg_length > 100000) {
        // dump buffer
        std::string buf;
        for (int i = 0; i < 5; i++) {
            buf += fmt::format("{:02X}", i, buffer[i]);
        }
        SPDLOG_ERROR("Invalid message length: {}, buffer: {}", msg_length, buf);
    }

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
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Read message length: {}",
                    (_type == CLIENT ? 'C' : 'S'), _id, msg_length);

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
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Need to read more data for message: {}",
                    (_type == CLIENT ? 'C' : 'S'), _id, offset - n);
        BufferPtr tail = blist.buffers.back();
        int rd = _connection->read(tail->data() + tail->size(), offset - n, offset - n);
        tail->incr_size(rd);
        CHECK_EQ(rd, offset - n);
    }
}

void
Session::_stream_to_remote_session(char code, int32_t msg_length, uint64_t seq_id)
{
    assert(_is_shadow || _associated_session != nullptr);
    char buffer[4096];

    // first write the header, add 4 to msg length for size of length field
    buffer[0] = code;
    sendint32(msg_length + 4, buffer + 1);

    int n;
    if (!_is_shadow) {
        n = _associated_session->get_connection()->write(buffer, 5);
        CHECK_EQ(n, 5);
        PROXY_DEBUG(LOG_LEVEL_DEBUG3,
                    "[{}:{}] Streamed header to remote session: code={}, msg_length={}",
                    (_type == CLIENT ? 'C' : 'S'), _id, code, msg_length);
    }

    // iterate reading buffer from local session and write to remote session
    while (msg_length > 0) {
        PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Reading {} bytes from local socket",
                    (_type == CLIENT ? 'C' : 'S'), _id, std::min(msg_length, 4096));

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

            PROXY_DEBUG(LOG_LEVEL_DEBUG3, "[{}:{}] Streamed {} bytes to remote session",
                        (_type == CLIENT ? 'C' : 'S'), _id, m);
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
        _associated_session->_log_buffer(false, code, buffer->size() - 5, buffer->data() + 5,
                                         seq_id, true);
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

    // general error handling, shutdown for both clients and servers
    if (_type == Type::PRIMARY || _type == Type::REPLICA) {
        // This is a server session, notify client session of error
        ServerSession *server_session = static_cast<ServerSession *>(this);
        server_session->shutdown_client_session();

        // on error, the server shuts down the connection and releases the session
        // on clean shutdown (initiated by client), it is released back to the pool
        _connection->close();
        _server->shutdown_session(shared_from_this());

        // release the session, and deallocate it
        server_session->release_session(true);

        // the client will notify the other server session of the error
        // when it comes through this code path

    } else if (_type == Type::CLIENT) {
        // This is a client session, notify server sessions of error

        // first close connection and remove from server poll list
        _connection->close();
        _server->shutdown_session(shared_from_this());

        // notify server replica/primary sessions via shutdown_server_sessions()
        ClientSession *client = static_cast<ClientSession *>(this);
        client->shutdown_server_sessions();
    }
}

void
Session::_log_buffer(
    bool incoming, char code, int32_t len, const char *data, uint64_t seq_id, bool final)
{
    LoggerPtr logger = _server->get_logger();
    if (logger == nullptr) {
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

}  // namespace springtail::pg_proxy
