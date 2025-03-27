#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <variant>
#include <vector>
#include <absl/log/check.h>

#include <sys/time.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <fmt/core.h>

#include <common/logging.hh>
#include <common/constants.hh>

#include <pg_repl/pg_types.hh>
#include <pg_repl/libpq_connection.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <pg_repl/pg_repl_connection.hh>
#include <pg_repl/exception.hh>

// from socket.h; indicates more data is coming to send
#if !defined(MSG_MORE)
    #define MSG_MORE 0x00 // a bit messy, macosx doesn't support MSG_MORE
#endif

namespace springtail
{
    /** SQL command to fetch current LSN from server */
    static constexpr char CURRENT_LSN_SQL[] = "SELECT pg_current_wal_lsn()";
    static constexpr char CONFIRMED_FLUSH_LSN_SQL[] = "SELECT wal_status, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'";


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


    PgReplConnection::~PgReplConnection()
    {
        close();
    }


    void
    PgReplConnection::connect()
    {
        if (_connection.get() != nullptr) {
            throw PgAlreadyConnectedError();
        }
        _connection = std::make_unique<LibPqConnection>();
        _connection->connect(_db_host, _db_name, _db_user, _db_pass, _db_port, false);
    }


    void
    PgReplConnection::close()
    {
        // end streaming if started, this will close streaming connection
        try {
            end_streaming();
        } catch (const std::exception &exc) {
            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Exception during close ending streaming: {}", exc.what());
        } // we are ending streaming, catch all exceptions

        // free the libpq standard connection if open
        if (_connection.get() != nullptr) {
            _connection.reset(nullptr);
        }
    }


    void
    PgReplConnection::reconnect()
    {
        // close streaming and non-streaming connection
        close();
        // reconnect non-streaming connection
        connect();
        // restart streaming
        start_streaming(_last_flushed_lsn, false);
    }


    void
    PgReplConnection::start_streaming(LSN_t LSN, bool do_init)
    {
        if (_started_streaming) {
            // error already streaming
            throw PgStreamingError();
        }

        _stream_connection = std::make_unique<LibPqConnection>();
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

        // execute query: SELECT wal_status, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'slot_name';
        _connection->exec(fmt::format(CONFIRMED_FLUSH_LSN_SQL, _slot_name));
        // process results; sanity checks first
        if (_connection->status() != PGRES_TUPLES_OK ||
            _connection->ntuples() < 1 || _connection->nfields() < 2) {
            SPDLOG_ERROR("Error querying confirmed_flush_lsn\n");
            _connection->clear();
            throw PgQueryError();
        }

        // get wal_status
        char *wal_status = _connection->get_value(0, 0);
        char *lsn_str = _connection->get_value(0, 1);
        LSN_t confirmed_flush_lsn = pg_msg::str_to_LSN(lsn_str);
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Found current LSN: {}", confirmed_flush_lsn);

        if (wal_status != nullptr && std::strcmp(wal_status, "lost") == 0) {
            // if we have a 'lost' status, we need to recreate the slot or hard fail
            SPDLOG_ERROR("Replication slot is lost");
            _connection->clear();

            if (do_init) {
                // try and re-estable the slot
                try {
                    drop_replication_slot();
                    LSN = create_replication_slot();
                    confirmed_flush_lsn = LSN;
                } catch (const std::exception& e) {
                    SPDLOG_ERROR("Failed to recreate replication slot: {}", e.what());
                    throw PgReplicationSlotError();
                }
            } else {
                throw PgReplicationSlotError();
            }
        } else {
            _connection->clear();
        }

        if (LSN == INVALID_LSN) {
            // no existing LSN, find the last confirmed one and use it
            // result in form: XXX/XXX; convert to LSN_t
            LSN = confirmed_flush_lsn;
        }

        if (LSN < confirmed_flush_lsn) {
            // this is possible if we've ack'ed (fast forwarded), the LSN
            // due to an idle DB; use the value returned from the DB
            SPDLOG_WARN("LSN {} is less than confirmed_flush_lsn {}", LSN, confirmed_flush_lsn);
            LSN = confirmed_flush_lsn;
        }

        _last_flushed_lsn = LSN;

        // replication start command
        std::string pub_name = _stream_connection->escape_string(_pub_name);
        std::string cmd = fmt::format("START_REPLICATION SLOT \"{}\" LOGICAL {} (proto_version '{}', publication_names '{}', streaming 'on'",
            _slot_name, pg_msg::lsn_to_str(LSN), _proto_version, pub_name);

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
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Executing copy command: {}", cmd);
        _send_copy_data(cmd_buffer, cmd_length, MSG_QUERY);

        // read message header for response; msg type placed in _msg_type
        _read_msg_header();
        if (_msg_type != MSG_COPY_BOTH) {
            SPDLOG_ERROR("Error could not start WAL streaming: msg type={}", _msg_type);
            _stream_connection.reset(nullptr);
            throw PgQueryError();
        }

        // skip over rest of result message
        _skip_message();

        // set last received time to now, starts timer for idle delay
        _last_received_time = get_pgtime_in_millis();

        _copy_state = NEW_MSG;
        _started_streaming = true;

        // close the connection; we don't need it anymore
        _connection->disconnect();
    }


    void
    PgReplConnection::end_streaming()
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
            SPDLOG_ERROR("Error could not send end-of-streaming message to primary\n");
            _stream_connection.reset(nullptr);
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
        _stream_connection.reset(nullptr);
    }


    bool
    PgReplConnection::_check_data_stream(int timeout_secs)
    {
        int r = _stream_connection->wait_until_readable(timeout_secs);
        if (r < 0) {
            throw PgIOError();
        }

        return (r > 0); // true if data on socket
    }


    bool
    PgReplConnection::wait_for_data(int timeout_secs=constant::COORDINATOR_KEEP_ALIVE_TIMEOUT)
    {

        if (_started_streaming) {
            // check to see if we should fetch latest LSN to sync back
            int64_t now = get_pgtime_in_millis();
            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Now: {}, Last received time: {}, now - last received time: {}", now, _last_received_time, now - _last_received_time);
            if ((now - _last_received_time) > IDLE_SLOT_TIMEOUT_MSEC) {
                // see if we've been idle for longer (received no data)
                // than IDLE_SLOT_TIMEOUT_MSEC; if so we force an update
                // based on the current LSN
                SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Forcing LSN update due to idle slot");
                _fast_forward_stream();
            }

            // check to see if we should send a standby message
            if ((now - _last_status_time) > STANDBY_MSG_INTERVAL_MSEC) {
                _send_standby_status_msg();
            }
        }

        // select wait on data stream
        return _check_data_stream(timeout_secs);
    }


    void
    PgReplConnection::_send_copy_data(const char *buffer,
                                      int length,
                                      char cmd = MSG_COPY_DATA)
    {
        char msg_header[COPY_MSG_HDR_SIZE];

        // marshall header, first byte is operation, next 4 bytes are length
        msg_header[0] = cmd;
        // add 4 to length, since length includes length field
        sendint32(length + 4, &msg_header[1]);

        // send the header and then the operation
        int r = _stream_connection->write(msg_header, COPY_MSG_HDR_SIZE);
        if (r != 5) {
            SPDLOG_ERROR("Failed to write copy data header: bytes={}", r);
            SPDLOG_ERROR("errno: {}", errno);
            throw PgIOError();
        }

        r = _stream_connection->write(buffer, length);
        if (r < length) {
            // error
            SPDLOG_ERROR("Failed to write copy data body: bytes={}", r);
            SPDLOG_ERROR("errno: {}", errno);
            throw PgIOError();
        }
    }


    int
    PgReplConnection::_recv_copy_data(char *buffer,
                                      int length,
                                      bool async=true)
    {
        while (!_shutdown) {
            int r = _stream_connection->read(buffer, length, async);
            // Extract the errno into a separate variable to ensure the the SPDLOG_DEBUG_MODULE below this
            // doesn't overwrite the err code
            auto err_no = errno;
            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Read {} bytes from connection (errno={})", r, err_no);
            if (r == -1 && (err_no == EWOULDBLOCK || err_no == EAGAIN || err_no == EINTR)) {
                r = wait_for_data();
                if (r >= 0) {
                    // either data is now available or it isn't
                    // either way go around again.
                    continue;
                }
            }

            if (r < 0) {
                if (err_no == ECONNRESET) {
                    SPDLOG_ERROR("Error recv got ECONNRESET\n");
                    throw PgNotConnectedError();
                }

                SPDLOG_ERROR("Error recv return < 0; errno={}\n", err_no);
                throw PgIOError();
            }

            if (_shutdown) {
                break;
            }

            return r;
        }

        assert(_shutdown);
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Shutting down recv copy data");
        throw PgIOShutdown();
    }


    void
    PgReplConnection::_read_msg_header()
    {
        // msg header 1B for message type, 4B for length
        char msg_header[5];
        int  msg_header_offset = 0;

        // this is the start of a message; read the header
        while (msg_header_offset < COPY_MSG_HDR_SIZE) {

            // do a non-blocking read on the socket
            int r = _recv_copy_data(&msg_header[msg_header_offset],
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
            _read_msg_data(false);
            _dump_error_response();
        }
    }


    void
    PgReplConnection::_dump_error_response()
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
                SPDLOG_ERROR("Code: {}, Msg: {}\n", code, str);
                offset += str_len + 1;
            } else {
                break;
            }
        }
    }


    void
    PgReplConnection::_read_msg_data(bool async)
    {
        int to_read = std::min(_copy_msg_length - _copy_msg_offset,
                               COPY_BUFFER_SIZE);

        int length = 0, offset = 0;

        while (to_read > 0) {
            // non blocking read, read the min of: COPY_BUFFER_SIZE
            // or the remaining data left for this message
            // this ensures we can at least decode the keep alive
            // message
            int r = _recv_copy_data(&_copy_buffer[offset],
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


    void
    PgReplConnection::_skip_message()
    {
        while (_copy_msg_offset < _copy_msg_length) {
            // should read min of COPY_BUFFER_SIZE or remaining copy msg size
            // this shouldn't block since we've already read in the header
            _read_msg_data(false); // async=false
            _copy_msg_offset += _copy_buffer_length;
        }

        // discard copy buffer data
        _copy_buffer_length = 0;
        _copy_buffer_offset = 0;

        // reset message msg
        _copy_msg_offset = 0;
        _copy_msg_length = 0;
    }


    void
    PgReplConnection::_read_copy_header()
    {
        // this will essentially block until data is read
        // _copy_msg_length will be set
        _read_msg_header();

        // check for COPY DATA msg, handle other messages
        switch (_msg_type) {
            case MSG_COPY_DATA:
                _copy_state = READ_COPY_HEADER;
                return;

            case MSG_COPY_DONE:
                SPDLOG_WARN("Got COPY DONE message\n");
                throw PgCopyDoneError();

            case MSG_ERROR_RESPONSE:
                // message is decoded in readMsgHeader
                SPDLOG_ERROR("Got error response\n");
                throw PgIOError();

            case MSG_NOTIFICATION_RESPONSE:
            case MSG_NOTICE_RESPONSE:
            case MSG_PARAM_STATUS:
            default:
                // skip message for now
                SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Skipping message: type={}", _msg_type);
                _skip_message();
                _copy_state = NEW_MSG;
                return;
        }
    }


    void
    PgReplConnection::_read_copy_data()
    {
        // read the copy message data
        _read_msg_data();

        // if this is the first part of the message we've read
        // decode the copy message; either a keep alive or xlog data
        if (_copy_state == READ_COPY_HEADER) {
            // see: https://www.postgresql.org/docs/14/protocol-replication.html
            // these message should be smaller than the copy buffer length
            int offset = 0;
            switch (_copy_buffer[0]) {
                case MSG_KEEP_ALIVE:
                    SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Found keep alive message");
                    offset = _process_keep_alive(_copy_buffer, _copy_buffer_length);

                    // there shouldn't be more data
                    if (offset != _copy_msg_length) {
                        SPDLOG_WARN("Found unexpected data after keep alive message\n");
                        throw PgUnexpectedDataError();
                    }

                    // need to read a new message
                    _copy_state = NEW_MSG;

                    break;

                case MSG_XLOG_DATA:
                    SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Found xlog data");
                    offset = _process_xlog_header(_copy_buffer, _copy_buffer_length);

                    // adjust msg size to remove xlog header
                    _copy_msg_offset -= offset;
                    _copy_msg_length -= offset;
                    _copy_buffer_offset = offset;

                    _copy_state = STREAMING;

                    break;

                default:
                    SPDLOG_WARN("Unknown copy data command: {}", _copy_buffer[0]);
                    throw PgUnknownMessageError();
            }
        }
    }


    void
    PgReplConnection::read_data(PgCopyData &dataOut)
    {
        if (!_started_streaming) {
            throw PgNotStreamingError();
        }

        // see if we need to read the messsage header first
        if (_copy_state == NEW_MSG) {
            _read_copy_header();
        }

        // read the header in, now read the data message (or continue reading it)
        // skip if we need to read a new message
        if (_copy_state == READ_COPY_HEADER ||
            _copy_state == STREAMING) {
            _read_copy_data();
        }

        // if all data was consumed we go around again
        // this shouldn't be the case, but being safe...
        if (_copy_state == STREAMING && (_copy_buffer_length == 0)) {
            _copy_state = NEW_MSG;
        }

        if (_copy_state == NEW_MSG) {
            // no data yet
            dataOut.length = 0;
            return;
        }

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
        dataOut.starting_lsn = _message_start_lsn;
        dataOut.ending_lsn = _message_end_lsn; // this is believed to be the end lsn for this message

        // copy msg offset is ahead by the length of data we just read
        // but dataOut.msg_offset points to where the consumer is in the stream
        dataOut.msg_offset = _copy_msg_offset - dataOut.length;
        dataOut.proto_version = _proto_version;
    }


    int
    PgReplConnection::_process_keep_alive(const char *buffer, int length)
    {
        // handle keep alive
        if (length < (1 + 8 + 8)) {
            SPDLOG_WARN("Error keep alive msg too small: len={}\n", length);
            throw PgMessageTooSmallError();
        }

        int pos = 1;

        LSN_t wal_end = recvint64(&buffer[pos]); // read wal end LSN
        pos += 8;

        [[maybe_unused]] int64_t send_time = recvint64(&buffer[pos]);
        pos += 8;

        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Keep alive msg recvd: wal_end={}, send_time={}, last_flushed LSN={}",
                            wal_end, send_time, _last_flushed_lsn);

        bool response_requested = false;
        if (length >= (1 + 8 + 8 + 1)) {
            // the postgres code had this check
            if (buffer[pos]) {
                // response requested; send status msg
                response_requested = true;
            }
            pos += 1;
        }

        if (wal_end > _server_latest_lsn) {
            _server_latest_lsn = wal_end;
        }

        if (response_requested) {
            _send_standby_status_msg();
        }

        return pos;
    }


    int
    PgReplConnection::_process_xlog_header(const char *buffer, int length)
    {
        // handle log data
        if (length < (1 + 8 + 8 + 8)) {
            SPDLOG_ERROR("Error xlog data message too small: len={}\n", length);
            throw PgMessageTooSmallError();
        }

        int pos = 1;

        LSN_t wal_start = recvint64(&buffer[pos]);
        pos += 8;

        // see https://github.com/postgres/postgres/blob/f234b8cd16a4ba6e12cc51a36c8e499661d535bb/src/backend/replication/walreceiver.c#L1256
        // wal_end is the end of the last record in the message supposedly
        LSN_t wal_end = recvint64(&buffer[pos]);
        pos += 8;

        int64_t send_time = recvint64(&buffer[pos]);
        pos += 8;

        _last_received_time = send_time / 1000;
        _message_start_lsn = wal_start;
        _message_end_lsn = wal_end;

        if (_message_end_lsn > _server_latest_lsn) {
            _server_latest_lsn = _message_end_lsn;
        }

        return pos;
    }

    void
    PgReplConnection::_fast_forward_stream()
    {
        // execute query: SELECT pg_current_wal_lsn()
        const char *cmd = CURRENT_LSN_SQL;

        // check connection and execute query
        if (!_connection->is_connected()) {
            _connection->connect(_db_host, _db_name, _db_user, _db_pass, _db_port, false);
        }

        _connection->exec(cmd);

        // process results; sanity checks first
        if (_connection->status() != PGRES_TUPLES_OK ||
            _connection->ntuples() <= 0 || _connection->length(0, 0) < 3) {
            SPDLOG_ERROR("Error querying current LSN\n");
            throw PgQueryError();
        }

        // result in form: XXX/XXX; convert to LSN_t
        char *str = _connection->get_value(0, 0);
        LSN_t lsn = pg_msg::str_to_LSN(str);

        // disconnect from db
        _connection->clear();
        _connection->disconnect();

        // update last received time
        _last_received_time = get_pgtime_in_millis();

        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Primary LSN={}, Last flushed LSN={}, MsgEnd LSN={}", lsn, _last_flushed_lsn, _message_end_lsn);

        // check that LSN is ahead of where we are
        if (lsn == _last_flushed_lsn || lsn < _message_end_lsn) {
            // nothing to do, we'll get there
            return;
        }

        // check that there is still no data; false means no data
        // timeout of 0 means return immediately (i.e., poll)
        if (_check_data_stream(0)) {
            return;
        }

        // fast forward stream
        _last_flushed_lsn = lsn;
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Fast forwarding stream to LSN: {}", lsn);

        _send_standby_status_msg();
    }


    int
    PgReplConnection::_encode_standby_status_msg(int64_t send_time,
                                                 char replybuf[34])
    {
        int pos = 0;

        // see sendFeedback() in pg_recevlogical.c
        replybuf[pos] = MSG_STANDBY_STATUS;
        pos += 1;

        // check if this is right XXX
        sendint64(_message_end_lsn+1, &replybuf[pos]); // write position
        pos += 8;

        sendint64(_last_flushed_lsn+1, &replybuf[pos]); // flush position
        pos += 8;

        sendint64(INVALID_LSN, &replybuf[pos]);    // apply position
        pos += 8;

        sendint64(send_time, &replybuf[pos]);  // sendTime
        pos += 8;

        replybuf[pos] = 0; // reply requested 1/0
        pos += 1;

        return pos;
    }


    void
    PgReplConnection::_send_standby_status_msg()
    {
        char replybuf[STANDBY_MSG_SIZE];
        int64_t now = get_pgtime_in_millis();

        // set applied lsn and flushed lsn to same value
        int len = _encode_standby_status_msg(now, replybuf);

        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Standby message send: LSN={}", _last_flushed_lsn);

        // send data
        _send_copy_data(replybuf, len);

        _last_status_time = now;
    }


    bool
    PgReplConnection::check_slot_exists()
    {
        LSN_t restart_lsn;
        LSN_t flushed_lsn;

        return check_slot_exists(restart_lsn, flushed_lsn);
    }


    bool
    PgReplConnection::check_slot_exists(LSN_t &restart_lsn_out,
                                        LSN_t &flushed_lsn_out)
    {
        if (_started_streaming) {
            throw std::runtime_error("No queries after streaming starts");
        }

        std::string slot_name = _connection->escape_string(_slot_name);
        std::string cmd = fmt::format("SELECT restart_lsn, confirmed_flush_lsn from pg_catalog.pg_replication_slots WHERE slot_name='{}'",
                                      slot_name);

        // execute query
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Executing query check slots: cmd={}", cmd);
        _connection->exec(cmd);

        // process results
        if (_connection->status() != PGRES_COMMAND_OK &&
            _connection->status() != PGRES_TUPLES_OK) {
            SPDLOG_ERROR("Error executing query: msg={}\n", _connection->error_message());
            _connection->clear();

            throw PgQueryError();
        }

        if (_connection->ntuples() > 0 && _connection->nfields() == 2) {
            char *restart_lsn_str = _connection->get_value(0, 0);
            char *confirmed_flush_lsn_str = _connection->get_value(0, 1);

            restart_lsn_out = pg_msg::str_to_LSN(restart_lsn_str);
            flushed_lsn_out = pg_msg::str_to_LSN(confirmed_flush_lsn_str);

            SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Slot exists: restart_lsn={}, confirmed_flush_lsn={}",
                                restart_lsn_out, flushed_lsn_out);

            _connection->clear();
            return true;
        }

        _connection->clear();
        return false;
    }


    void
    PgReplConnection::drop_replication_slot()
    {
        if (_started_streaming) {
            throw PgStreamingError();
        }

        std::string slot_name = _connection->escape_string(_slot_name);

        std::string cmd = fmt::format("SELECT pg_drop_replication_slot('{}')", slot_name);

        // execute query
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Executing query drop replication slot: cmd={}", cmd);
        _connection->exec(cmd);

        // process results
        if (_connection->status() != PGRES_TUPLES_OK &&
            _connection->status() != PGRES_COMMAND_OK) {
            SPDLOG_ERROR("Error dropping replication slot: {}\n", slot_name);
            _connection->clear();
            throw PgQueryError();
        }

        _connection->clear();
    }


    LSN_t
    PgReplConnection::create_replication_slot()
    {
        if (_started_streaming) {
            throw PgStreamingError();
        }

        std::string slot_name = _connection->escape_string(_slot_name);

        std::string cmd = fmt::format("SELECT * FROM pg_create_logical_replication_slot('{}', 'pgoutput')", slot_name);

        // execute query
        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Executing query create replication slot: cmd={}", cmd);
        _connection->exec(cmd);

        // process results
        if (_connection->status() != PGRES_COMMAND_OK &&
            _connection->status() != PGRES_TUPLES_OK) {
            SPDLOG_ERROR("Error creating replication slot: {}", slot_name);
            _connection->clear();
            throw PgQueryError();
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Replication slot created successfully: {}", slot_name);

        if (_connection->ntuples() < 1 && _connection->nfields() != 2) {
            SPDLOG_ERROR("Replication slot creation did not return expected number of tuples for slot: {}", slot_name);
            _connection->clear();
            throw PgQueryError();
        }

        // get slot name and lsn, convert lsn and return it
        char *slot_name_str = _connection->get_value(0, 0);
        char *lsn_str = _connection->get_value(0, 1);

        DCHECK_NE(slot_name_str, nullptr);
        DCHECK_EQ(std::strcmp(slot_name_str, slot_name.c_str()), 0);
        DCHECK_NE(lsn_str, nullptr);

        LSN_t lsn_output = pg_msg::str_to_LSN(lsn_str); // capture the output
        _connection->clear(); // clear the connection now

        return lsn_output;
    }


    void
    PgReplConnection::set_last_flushed_LSN(LSN_t lsn)
    {
        if (lsn == INVALID_LSN) {
            return;
        }

        SPDLOG_DEBUG_MODULE(LOG_PG_REPL, "Setting last flushed LSN: lsn={}", lsn);

        int64_t now = get_pgtime_in_millis();
        bool send_standby = false;

        // check if we should send a standby status message
        if ((now - _last_status_time) > STANDBY_MSG_INTERVAL_MSEC) {
            send_standby = true;
        }

        _last_flushed_lsn = lsn;
        _last_flushed_time = now;

        if (send_standby) {
            _send_standby_status_msg();
        }
    }


    int
    PgReplConnection::get_server_version() noexcept
    {
        return _server_version;
    }


    int
    PgReplConnection::get_protocol_version() noexcept
    {
        return _proto_version;
    }
}
