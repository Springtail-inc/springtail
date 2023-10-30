#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <variant>
#include <vector>

#include <sys/time.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <fmt/core.h>

#include <psql_cdc/pg_types.hh>
#include <psql_cdc/libpq_connection.hh>
#include <psql_cdc/pg_repl_msg.hh>
#include <psql_cdc/pg_repl_connection.hh>
#include <psql_cdc/exception.hh>

// from socket.h; indicates more data is coming to send
#if !defined(MSG_MORE)
    #define MSG_MORE 0x00 // a bit messy, macosx doesn't support MSG_MORE
#endif

namespace springtail
{
    /** SQL command to fetch current LSN from server */
    static const char *CURRENT_LSN_SQL = "SELECT pg_current_wal_lsn()";

    /**
     * @brief Constructor
     *
     * @param db_port server port; usually 5432
     * @param db_host server hostname
     * @param db_name server database name
     * @param db_user server username
     * @param db_pass server password
     * @param slot_name replication slot name
     */
    PgReplConnection::PgReplConnection(const int db_port,
                                       const std::string &db_host,
                                       const std::string &db_name,
                                       const std::string &db_user,
                                       const std::string &db_pass,
                                       const std::string &pub_name,
                                       const std::string &slot_name)
        : _db_port(db_port),
          _db_host(db_host),
          _db_name(db_name),
          _db_user(db_user),
          _db_pass(db_pass),
          _pub_name(pub_name),
          _slot_name(slot_name)
    {}


    /**
     * @brief Destructor
     */
    PgReplConnection::~PgReplConnection()
    {
        close();
    }


    /**
     * @brief Connect to server using params from constructor
     * @throws PgIOError on connection failure
     * @throws PgQueryError on failure to set search path
     */
    void PgReplConnection::connect()
    {
        if (_connection != nullptr) {
            throw PgAlreadyConnectedError();
        }
        _connection = new LibPqConnection();
        _connection->connect(_db_host, _db_name, _db_user, _db_pass, _db_port, true);
    }


    /**
     * @brief Close connection; stop streaming
     */
    void PgReplConnection::close()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        if (_started_streaming) {
            endStreaming();
            _started_streaming = false;
        }

        delete _connection;
        _connection = nullptr;
    }


    /**
     * @brief Start streaming at specified LSN,
     *
     * @param LSN LSN to start streaming from; INVALID_LSN (0) specifies server current LSN
     * @throws PgStreamingError if connection is already streaming
     * @throws PgQueryError if replication command failed
     */
    void PgReplConnection::startStreaming(LSN_t LSN)
    {
        if (_started_streaming) {
            // error already streaming
            throw PgStreamingError();
        }

        _stream_connection = new LibPqConnection();
        _stream_connection->connect(_db_host, _db_name, _db_user, _db_pass, _db_port, true);

        // get protocol version
        _server_version = _stream_connection->server_version();

        // currently only support version 1 or 2, pick which to use
        // version 2 supports streaming xacts (from PG 14+);
        // version 3 for PG 15+; two phase commit
        // version 4 for PG 16+; parallel streaming
        if (_server_version >= PG_VERS_14) {
            // enable streaming, which tries to remove the need
            // for spooling xact logs to disk in pgoutput which lessens lag
            _proto_version = 2;
        } else {
            _proto_version = 1;
        }

        // convert LSN to X/X format
        uint32_t lsn_higher = (uint32_t)(LSN>>32);
        uint32_t lsn_lower = (uint32_t)(LSN);

        // replication start command
        std::unique_ptr<char[]> pub_name = _stream_connection->escapeString(_pub_name);
        std::string cmd = fmt::format("START_REPLICATION SLOT \"{}\" LOGICAL {:X}/{:X} (proto_version '{}', publication_names '{}'",
            _slot_name, lsn_higher, lsn_lower, _proto_version, pub_name.get());

        if (_server_version >= PG_VERS_14) {
            // binary and logical messages not supported prior to pg14
            cmd += ", binary, messages)";
        } else {
            cmd += ")";
        }

        // extract message buffer
        const char *cmd_buffer = cmd.c_str();
        int cmd_length = std::strlen(cmd_buffer) + 1; // strlen doesn't include null

        // send header and then data
        // we do this on using sockets to support non-blocking reads of less
        // than the full message length on the stream connection for copy data
        std::cout << "Executing: " << cmd << std::endl;
        sendCopyData(cmd_buffer, cmd_length, MSG_QUERY);

        // read message header for response; msg type placed in _msg_type
        readMsgHeader();
        if (_msg_type != MSG_COPY_BOTH) {
            std::cerr << "Error could not start WAL streaming: (msg type=" << _msg_type << ")\n";
            delete _stream_connection;
            _stream_connection = nullptr;
            throw PgQueryError();
        }

        // skip over rest of result message
        skipMessage();

        _copy_state = NEW_MSG;
        _streaming_socket = _stream_connection->socket();
        _started_streaming = true;
    }


    /**
     * @brief End streaming; close streaming connection
     *        hide errors, as not useful at this point, as connection is being closed
     */
    void PgReplConnection::endStreaming()
    {
        if (!_started_streaming) {
            return;
        }

        // after this point assume streaming has ended
        _started_streaming = false;
        _copy_buffer_length = 0;
        _copy_buffer_offset = 0;
        _copy_msg_length = 0;
        _copy_msg_offset = 0;

        // see libpqwalreceiver.c libpqrcv_endstreaming() for detailed way to shutdown cleanly
        // send copy end message
        if (_stream_connection->put_copy_end(nullptr) <= 0 || _stream_connection->flush()) {
            std::cerr << "Error could not send end-of-streaming message to primary" << std::endl;
            delete _stream_connection;
            _stream_connection = nullptr;
            return;
        }

        ExecStatusType status = _stream_connection->status();
        _stream_connection->clear();
        if (status == PGRES_COPY_OUT) {
            // end the copy
            _stream_connection->end_copy();
        }

        // just close connection; cleans up result and other state
        // no need to go through too much trouble...
        delete _stream_connection;
        _stream_connection = nullptr;
    }


    /**
     * @brief Check the data stream for waiting data
     *
     * @param timeout_secs timeout in seconds; 0 return immediately
     * @return true if has data, false otherwise
     * @throws PgIOError on stream error
     */
    bool PgReplConnection::checkDataStream(int timeout_secs)
    {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(_streaming_socket, &fds);

        struct timeval t;
        t.tv_usec = 0;
        t.tv_sec = timeout_secs;

        // r < 0 error; r == 0 timeout; r > 0 readable socket
        std::cout << "Blocking in select for: " << timeout_secs << " secs\n";
        int r = select(_streaming_socket + 1, &fds, nullptr, nullptr, &t);
        if (r < 0) {
            throw PgIOError();
        }

        return (r > 0); // true if data on socket
    }


    /**
     * @brief No data received handle timeout, wait for more data
     * @return true if has data, false otherwise
     * @throws PgIOError on stream error
     */
    bool PgReplConnection::handleTimeout()
    {

        if (_started_streaming) {
            // check to see if we should fetch latest LSN to sync back
            int64_t now = getPgTimeInMillis();
            if ((now - _last_received_time) > IDLE_SLOT_TIMEOUT_MSEC) {
                // see if we've been idle for longer (received no data)
                // than IDLE_SLOT_TIMEOUT_MSEC; if so we force an update
                // based on the current LSN
                fastForwardStream();
            }

            // check to see if we should send a standby message
            if ((now - _last_status_time) > STANDBY_MSG_INTERVAL_MSEC) {
                sendStandbyStatusMsg();
            }
        }

        // select wait on data stream
        return checkDataStream(READ_TIMEOUT_SEC);
    }


    /**
     * @brief Send data on streaming connection
     *
     * @param buffer buffer to send
     * @param length length of data within buffer
     * @param msg_type type of message (optional; default COPY_DATA message)
     * @throws PgIOError on send error
     */
    void PgReplConnection::sendCopyData(const char *buffer,
                                        int length,
                                        char cmd = MSG_COPY_DATA)
    {
        char msg_header[COPY_MSG_HDR_SIZE];

        // marshall header, first byte is operation, next 4 bytes are length
        msg_header[0] = cmd;
        // add 4 to length, since length includes length field
        sendint32(length + 4, &msg_header[1]);

        int flags = MSG_NOSIGNAL | MSG_MORE ; // no SIGPIPE if connection is closed

        // send the header and then the operation
        int r = send(_streaming_socket, msg_header, 5, flags);
        if (r != 5) {
            std::cerr << "Failed to write copy data header (r=" << r << ")\n";
            throw PgIOError();
        }

        r = send(_streaming_socket, buffer, length, MSG_NOSIGNAL);
        if (r < length) {
            // XXX if r > 0 this technically isn't an error and we should
            // really continue retrying the send
            std::cerr << "Failed to write copy data body (r=" << r << ")\n";
            throw PgIOError();
        }
    }


    /**
     * @brief Read data from streaming connection
     *
     * @param buffer buffer to receive into
     * @param length length of data to read
     * @param async  flag to specify read should be non-blocking (optional; default=true)
     * @returns bytes read
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     */
    int PgReplConnection::recvCopyData(char *buffer,
                                        int length,
                                        bool async=true)
    {
        while (true) {
            int r = recv(_streaming_socket,
                         buffer, length, (async ? MSG_DONTWAIT : 0));

            std::cout << "Read raw data: " << r << std::endl;

            if (r == -1 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                std::cout << "Recv got ewouldblock\n";
                r = handleTimeout();
                if (r >= 0) {
                    // either data is now available or it isn't
                    // either way go around again.
                    continue;
                }
            }

            if (r < 0) {
                std::cerr << "Error recv return < 0; errno=" << errno << std::endl;
                throw PgIOError();
            }

            if (r == 0) {
                std::cerr << "Error recv got EOF\n";
                throw PgNotConnectedError();
            }

            return r;
        }
    }


    /**
     * @brief Read in full message header (1B msg type; 4B length)
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     */
    void PgReplConnection::readMsgHeader()
    {
        // msg header 1B for message type, 4B for length
        char msg_header[5];
        int  msg_header_offset = 0;

        // this is the start of a message; read the header
        while (msg_header_offset < COPY_MSG_HDR_SIZE) {

            // do a non-blocking read on the socket
            int r = recvCopyData(&msg_header[msg_header_offset],
                                 COPY_MSG_HDR_SIZE-msg_header_offset);

            msg_header_offset += r;
        }

        // decode header
        int32_t length = recvint32(&msg_header[1]);
        _msg_type = msg_header[0];

        // set msg length -- length includes the length field
        _copy_msg_length = length - 4;
        _copy_msg_offset = 0;

        // try and read in and parse error message as long as it all
        // fits within a single copy buffer (which it should)
        // other message types are handled by the caller
        if (_msg_type == MSG_ERROR_RESPONSE && _copy_msg_length < COPY_BUFFER_SIZE) {
            readMsgData(false);
            dumpErrorResponse();
        }
    }


    /**
     * @brief Helper to dump out error response
     */
    void PgReplConnection::dumpErrorResponse()
    {
        // format: code 1B, message string null terminated
        // code: 'M' has the most useful message
        int offset = 0;
        char code;
        // terminate loop if code='\0' terminate, or if no more data to read after code
        while ((code =_copy_buffer[offset]) != '\0' && ++offset < _copy_buffer_length) {
            char *str = &_copy_buffer[offset];
            int str_len = strnlen(str, _copy_buffer_length - offset);
            if (str_len < _copy_buffer_length - offset - 1) {
                std::cerr << "Code: " << code << " Msg: " << str << std::endl;
                offset += str_len + 1;
            } else {
                break;
            }
        }
    }


    /**
     * @brief Read in message data; may be partial message
     * @param async flag to specify operation is non-blocking (optional; default=true)
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     */
    void PgReplConnection::readMsgData(bool async=true)
    {
        int to_read = std::min(_copy_msg_length - _copy_msg_offset,
                               COPY_BUFFER_SIZE);

        int length = 0, offset = 0;

        while (to_read > 0) {
            // non blocking read, read the min of: COPY_BUFFER_SIZE
            // or the remaining data left for this message
            // this ensures we can at least decode the keep alive
            // message
            int r = recvCopyData(&_copy_buffer[offset],
                                 to_read, async);
            to_read -= r;
            length += r;
            offset += r;
        }

        _copy_buffer_length = length;
        _copy_buffer_offset = 0;

        // this may be ahead of where the consumer is, but it is where we are
        _copy_msg_offset += length;
    }


    /**
     * @brief Skipping over current message
     *
     * @param size data to skip
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     */
    void PgReplConnection::skipMessage()
    {
        while (_copy_msg_offset < _copy_msg_length) {
            // should read min of COPY_BUFFER_SIZE or remaining copy msg size
            // this shouldn't block since we've already read in the header
            readMsgData(false); // async=false
            _copy_msg_offset += _copy_buffer_length;
        }

        // discard copy buffer data
        _copy_buffer_length = 0;
        _copy_buffer_offset = 0;

        // reset message msg
        _copy_msg_offset = 0;
        _copy_msg_length = 0;
    }


    /**
     * @brief Read in copy data header; sets the copy message length
     *
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     * @throws PgCopyDoneError if copy done is returned
     */
    void PgReplConnection::readCopyHeader()
    {
        // this will essentially block until data is read
        // _copy_msg_length will be set
        readMsgHeader();

        // check for COPY DATA msg, handle other messages
        switch (_msg_type) {
            case MSG_COPY_DATA:
                _copy_state = READ_COPY_HEADER;
                return;

            case MSG_COPY_DONE:
                std::cout << "Got COPY DONE message\n";
                throw PgCopyDoneError();

            case MSG_ERROR_RESPONSE:
                // message is decoded in readMsgHeader
                std::cerr << "Got error response\n";
                throw PgIOError();

            case MSG_NOTIFICATION_RESPONSE:
            case MSG_NOTICE_RESPONSE:
            case MSG_PARAM_STATUS:
            default:
                // skip message for now
                std::cout << "Skipping message: " << _msg_type << std::endl;
                skipMessage();
                _copy_state = NEW_MSG;
                return;
        }
    }


    /**
     * @brief Read in the copy data, decoding the msg and processing keep alives
     *
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     * @throws PgCopyDoneError if copy done is returned
     */
    void PgReplConnection::readCopyData()
    {
        // read the copy message data
        readMsgData();

        // if this is the first part of the message we've read
        // decode the copy message; either a keep alive or xlog data
        if (_copy_state == READ_COPY_HEADER) {
            // see: https://www.postgresql.org/docs/14/protocol-replication.html
            // these message should be smaller than the copy buffer length
            int offset = 0;
            switch (_copy_buffer[0]) {
                case MSG_KEEP_ALIVE:
                    std::cout << "Found keep alive\n";
                    offset = processKeepAlive(_copy_buffer, _copy_buffer_length);

                    // there shouldn't be more data
                    if (offset != _copy_msg_length) {
                        std::cerr << "Found unexpected data after keep alive message\n";
                        throw PgUnexpectedDataError();
                    }

                    // need to read a new message
                    _copy_state = NEW_MSG;

                    break;

                case MSG_XLOG_DATA:
                    std::cout << "Found xlog data\n";
                    offset = processXlogHeader(_copy_buffer, _copy_buffer_length);

                    // adjust msg size to remove xlog header
                    _copy_msg_offset -= offset;
                    _copy_msg_length -= offset;
                    _copy_buffer_offset = offset;

                    _copy_state = STREAMING;

                    break;

                default:
                    std::cerr << "Unknown copy data command: " << _copy_buffer[0] << std::endl;
                    throw PgUnknownMessageError();
            }
        }
    }


    /**
     * @brief Read copy data from server; will block
     *
     * @param dataOut  Reference to hold output buffer and length; freed on
     *                 next call to readData
     * @throws PgIOError on receive error
     * @throws PgNotConnectedError if connection has closed
     * @throws PgNotStreamingError if connection is not streaming
     */
    void PgReplConnection::readData(PgCopyData &dataOut)
    {
        if (!_started_streaming) {
            throw PgNotStreamingError();
        }

        // calling readData implicitly ack's the last received LSN
        // only do this if full message has been consumed
        if (_copy_state == NEW_MSG) {
            setLastFlushedLSN(_last_received_lsn);
        }

        do {
            // see if we need to read the messsage header first
            if (_copy_state == NEW_MSG) {
                readCopyHeader();
            }

            // read the header in, now read the data message (or continue reading it)
            // skip if we need to read a new message
            if (_copy_state == READ_COPY_HEADER ||
                _copy_state == STREAMING) {
                readCopyData();
            }

            // if all data was consumed we go around again
            // this shouldn't be the case, but being safe...
            if (_copy_state == STREAMING && (_copy_buffer_length == 0)) {
                _copy_state = NEW_MSG;
            }

        // see if we have to read a new message
        } while (_copy_state == NEW_MSG);

        // if we got here we should be in the streaming state

        // if this is the last buffer reset the state for when we come back
        if (_copy_state == STREAMING && (_copy_msg_offset == _copy_msg_length)) {
            _copy_state = NEW_MSG;
        }

        // set the output data appropriately
        // _copy_buffer_offset may be non-zero due to xlog message
        dataOut.buffer = &_copy_buffer[_copy_buffer_offset];
        dataOut.length = _copy_buffer_length - _copy_buffer_offset;
        dataOut.msg_length = _copy_msg_length;
        dataOut.starting_lsn = _last_received_lsn;

        // copy msg offset is ahead by the length of data we just read
        // but dataOut.msg_offset points to where the consumer is in the stream
        dataOut.msg_offset = _copy_msg_offset - dataOut.length;
    }


    /**
     * @brief Decode keep alive
     * @return number of bytes consumed
     * @throws PgMessageToSmallError if buffer not big enough for message
     */
    int PgReplConnection::processKeepAlive(const char *buffer, int length)
    {
        // handle keep alive
        if (length < (1 + 8 + 8)) {
            std::cerr << "Error keep alive msg too small: " << length << std::endl;
            throw PgMessageTooSmallError();
        }

        int pos = 1;

        LSN_t wal_end = recvint64(&buffer[pos]); // read wal end LSN
        pos += 8;

        int64_t send_time = recvint64(&buffer[pos]);
        pos += 8;

        std::cout << "Keep alive: wal_end: " << wal_end << ", send time: "
                  << send_time << std::endl;

        bool response_requested = false;
        if (length >= (1 + 8 + 8 + 1)) {
            // the postgres code had this check
            if (buffer[pos]) {
                // response requested; send status msg
                response_requested = true;
            }
            pos += 1;
        }

        _server_latest_lsn = wal_end;
        _last_received_time = send_time;

        if (response_requested) {
            sendStandbyStatusMsg();
        }

        return pos;
    }


    /**
     * @brief Decode xlog data header
     * @return number of bytes consumed
     * @throws PgMessageToSmallError if buffer not big enough for message
     */
    int PgReplConnection::processXlogHeader(const char *buffer, int length)
    {
        // handle log data
        if (length < (1 + 8 + 8 + 8)) {
            std::cerr << "Error xlog data message too small: " << length << std::endl;
            throw PgMessageTooSmallError();
        }

        int pos = 1;

        LSN_t wal_start = recvint64(&buffer[pos]);
        pos += 8;

        LSN_t wal_end = recvint64(&buffer[pos]);
        pos += 8;

        int64_t send_time = recvint64(&buffer[pos]);
        pos += 8;

        _last_received_time = send_time;
        _last_received_lsn = wal_start;
        _server_latest_lsn = wal_end;

        return pos;
    }

    /**
     * @brief Fast forward the data stream to current LSN (ack to server)
     * @throws PgQueryError if query to get current LSN fails
     */
    void PgReplConnection::fastForwardStream()
    {
        // execute query: SELECT pg_current_wal_lsn()
        const char *cmd = CURRENT_LSN_SQL;

        // execute query
        _connection->exec(cmd);

        // process results; sanity checks first
        if (_connection->status() != PGRES_TUPLES_OK ||
            _connection->ntuples() <= 0 || _connection->length(0, 0) < 3) {
            std::cerr << "Error querying current LSN\n";
            throw PgQueryError();
        }

        // result in form: XXX/XXX; convert to LSN_t
        char *str = _connection->get_value(0, 0);
        LSN_t lsn = PgReplMsg::strToLSN(str);
        _connection->clear();

        // check that LSN is ahead of where we are
        if (lsn < _last_received_lsn) {
            // nothing to do, we'll get there
            return;
        }

        // check that there is still no data; false means no data
        // timeout of 0 means return immediately (i.e., poll)
        if (checkDataStream(0)) {
            return;
        }

        // fast forward stream
        _last_flushed_lsn = lsn;

        sendStandbyStatusMsg();
    }


    /**
     * @brief Encode the standby message into pre-allocated buffer of at least 34B
     *
     * @param last_received_lsn last received lsn (write position)
     * @param last_flushed_lsn last flushed lsn (flushed position)
     * @param send_time time for this send (pg msecs)
     * @return size of buffer returned
     */
    int PgReplConnection::encodeStandbyStatusMsg(LSN_t last_received_lsn,
                                                 LSN_t last_flushed_lsn,
                                                 int64_t send_time,
                                                 char replybuf[34])
    {
        int pos = 0;

        // see sendFeedback() in pg_recevlogical.c
        replybuf[pos] = MSG_STANDBY_STATUS;
        pos += 1;

        // check if this is right XXX
        sendint64(last_received_lsn, &replybuf[pos]); // write position
        pos += 8;

        sendint64(last_flushed_lsn, &replybuf[pos]); // flush position
        pos += 8;

        sendint64(INVALID_LSN, &replybuf[pos]);    // apply position
        pos += 8;

        sendint64(send_time, &replybuf[pos]);  // sendTime
        pos += 8;

        replybuf[pos] = 0; // reply requested 1/0
        pos += 1;

        return pos;
    }


    /**
     * @brief Send standby status feedback message to server
     * @throws PgIOError on send error
     */
    void PgReplConnection::sendStandbyStatusMsg()
    {
        char replybuf[STANDBY_MSG_SIZE];
        int64_t now = getPgTimeInMillis();

        // set applied lsn and flushed lsn to same value
        int len = encodeStandbyStatusMsg(_last_flushed_lsn,
                                         _last_flushed_lsn,
                                         now, replybuf);

        std::cout << "Standby message send: LSN=" << _last_flushed_lsn << std::endl;

        // send data
        sendCopyData(replybuf, len);

        _last_status_time = now;
    }


    /**
     * @brief Check if slot exists
     *
     * @return true if slot exists, false otherwise
     * @throws PgQueryError on error
     */
    bool PgReplConnection::checkSlotExists()
    {
        LSN_t restart_lsn;
        LSN_t flushed_lsn;

        return checkSlotExists(restart_lsn, flushed_lsn);
    }


    /**
     * @brief Check if the slot exists on the server
     *
     * @param restart_lsn_out output param: restart LSN
     * @param flushed_lsn_out output param: last flushed LSN
     *
     * @return true if slot exists, false otherwise
     * @throws PgQueryError on error
     */
    bool PgReplConnection::checkSlotExists(LSN_t &restart_lsn_out,
                                           LSN_t &flushed_lsn_out)
    {
        if (_started_streaming) {
            throw std::runtime_error("No queries after streaming starts");
        }

        std::unique_ptr<char[]> slot_name = _connection->escapeString(_slot_name);
        std::string cmd = fmt::format("SELECT restart_lsn, confirmed_flush_lsn from pg_catalog.pg_replication_slots WHERE slot_name='{}'",
                                      slot_name.get());

        // execute query
        std::cout << "Executing query: " << cmd << std::endl;
        _connection->exec(cmd);

        // process results
        if (_connection->status() != PGRES_COMMAND_OK &&
            _connection->status() != PGRES_TUPLES_OK) {
            std::cerr << "Error executing query: " << _connection->error_message() << std::endl;
            _connection->clear();

            throw PgQueryError();
        }

        std::cout << "Got command OK or TUPLES_OK tuples=" << _connection->ntuples() << std::endl;

        if (_connection->ntuples() > 0 && _connection->nfields() == 2) {
            char *restart_lsn_str = _connection->get_value(0, 0);
            char *confirmed_flush_lsn_str = _connection->get_value(0, 1);

            restart_lsn_out = PgReplMsg::strToLSN(restart_lsn_str);
            flushed_lsn_out = PgReplMsg::strToLSN(confirmed_flush_lsn_str);

            _connection->clear();
            return true;
        }

        _connection->clear();
        return false;
    }


    /**
     * @brief Drop replication slot
     *
     * @throws PgQueryError on error
     * @throws PgStreamingError if already streaming
     */
    void PgReplConnection::dropReplicationSlot()
    {
        if (_started_streaming) {
            throw PgStreamingError();
        }

        std::unique_ptr<char[]> slot_name = _connection->escapeString(_slot_name);

        std::string cmd = fmt::format("DROP_REPLICATION_SLOT '{}'", slot_name.get());

        // execute query
        std::cout << "Executing query: " << cmd << std::endl;
        _connection->exec(cmd);

        // process results
        if (_connection->status() == PGRES_COMMAND_OK) {
            std::cerr << "Error dropping replication slot" << std::endl;
            _connection->clear();
            throw PgQueryError();
        }

        _connection->clear();
    }


    /**
     * @brief Create replication slot
     *
     * @param export_snapshot should export be create (true/false)
     * @param temporary is replication slot temporary
     * @throws PgStreamingError if streaming already started
     * @throws PgQueryError on query error
     */
    void PgReplConnection::createReplicationSlot(bool export_snapshot,
                                                 bool temporary)
    {
        if (_started_streaming) {
            throw PgStreamingError();
        }

        std::stringstream s;

        // escape slot name
        std::unique_ptr<char[]> slot_name = _connection->escapeString(_slot_name);

        bool use_new_syntax = (_server_version >= 150000);

        // setup command
        s << "CREATE_REPLICATION_SLOT " << "\"" << slot_name.get() << "\"";
        if (temporary) {
            s << " TEMPORARY";
        }

        s << " LOGICAL pgoutput";

        if (use_new_syntax) {
            s << " (";
            if (export_snapshot) {
                s << "SNAPSHOT 'export'";
            } else {
                s << "SNAPSHOT 'nothing'";
            }
            s << ")";
        } else {
            // uses old format style
            if (export_snapshot) {
                s << " EXPORT_SNAPSHOT";
            } else {
                s << " NOEXPORT_SNAPSHOT";
            }
        }

        const std::string& tmp = s.str();
        const char* cmd = tmp.c_str();

        std::cout << "Executing query: " << cmd << std::endl;

        // execute query
        _connection->exec(cmd);

        // process results
        if (_connection->status() != PGRES_TUPLES_OK) {
            std::cerr << "Error executing CREATE_REPLICATION_SLOT: " << _connection->error_message() << std::endl;
            _connection->clear();
            throw PgQueryError();
        }

        // check result number of tuples and columns
        if (_connection->ntuples() != 1 || _connection->nfields() != 4) {
            std::cerr << "Unexpected number of rows or columns for CREATE REPLICATION SLOT: rows="
                      <<  _connection->ntuples() << " cols=" << _connection->nfields() << std::endl;
            _connection->clear();
            throw PgQueryError();
        }

        // result unused:
        // 0,0 = slot_name, 0,1 = XXX/XXX earliest LSN for streaming
        // 0,2 = snapshot name, 0,3 = output plugin

        _export_name = _connection->getString(0,2);

        _connection->clear();
    }


    /**
     * @brief Update last flushed lsn, we are safe to move log forward to here
     *
     * @param lsn LSN indicating safe point to truncate log up to
     */
    void PgReplConnection::setLastFlushedLSN(LSN_t lsn) noexcept
    {
        if (lsn == INVALID_LSN || lsn <= _last_flushed_lsn) {
            return;
        }

        _last_flushed_lsn = lsn;
        _last_flushed_time = getPgTimeInMillis();
    }

    /**
     * @brief Get server version
     * @return get remote server version; -1 if not set
     */
    int PgReplConnection::getServerVersion() noexcept
    {
        return _server_version;
    }

    /**
     * @brief Get pgoutput protocol version
     * @return pgoutput protocol version (1, 2, 3, 4) -- usually 2; -1 if not set
     */
    int PgReplConnection::getProtocolVersion() noexcept
    {
        return _proto_version;
    }
}
