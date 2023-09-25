#include <iostream>
#include <sstream>
#include <cstdlib>
#include <sys/time.h>
#include <sys/select.h>

#include <psql_cdc/pg_types.hh>
#include <psql_cdc/pg_repl_connection.hh>

/** SQL command to set serach path */
const char *ALWAYS_SECURE_SEARCH_PATH_SQL =
    "SELECT pg_catalog.set_config('search_path', '', false);";

/** SQL command to fetch current LSN from server */
const char *CURRENT_LSN_SQL =
    "SELECT pg_current_wal_lsn()";

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
                               const std::string& db_host,
                               const std::string& db_name,
                               const std::string& db_user,
                               const std::string& db_pass,
                               const std::string& slot_name)
{
    _db_host = db_host;
    _db_port = db_port;
    _db_name = db_name;
    _db_user = db_user;
    _db_pass = db_pass;
    _slot_name = slot_name;
}

/**
 * @brief Destructor
 */
PgReplConnection::~PgReplConnection()
{
    close();
}

/**
 * @brief Helper to execute queries that have no return result
 *
 * @param cmd sql command to execute
 *
 * @return 0 on success; -1 otherwise
 */
int PgReplConnection::pgExec(const std::string cmd)
{
    std::cout << "Executing query: " << cmd;
    PGresult *res = PQexec(_connection, cmd.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "Error setting search path: " << PQerrorMessage(_connection);
        PQclear(res);
        return -1;
    }
    PQclear(res);
    return 0;
}

/**
 * @brief Connect to server using params from constructor
 * @return 0 success; <0 failure
 */
int PgReplConnection::connect()
{
    // create key value list for: host, port, dbname, user, password, options
    std::stringstream s;

    // escape options
    char *host = PQescapeLiteral(_connection, _db_host.c_str(), _db_host.length());
    char *name = PQescapeLiteral(_connection, _db_name.c_str(), _db_name.length());
    char *user = PQescapeLiteral(_connection, _db_user.c_str(), _db_user.length());
    char *pass = PQescapeLiteral(_connection, _db_pass.c_str(), _db_pass.length());

    s << "host='" << host << "' port=" << _db_port << " dbname='" << name
      << "' user='" << user << "' password='" << pass << "'' "
      << "options='-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3'";

    const std::string& tmp = s.str();
    const char* conninfo = tmp.c_str();

    // try connection
    _connection = PQconnectdb(conninfo);

    // free mem
    PQfreemem(host);
    PQfreemem(name);
    PQfreemem(user);
    PQfreemem(pass);

    if (PQstatus(_connection) != CONNECTION_OK) {
        std::cerr << "Error connecting: " << PQerrorMessage(_connection);
        return -1;
    }

    // for safety set search path
    int res = pgExec(ALWAYS_SECURE_SEARCH_PATH_SQL);
    if (res == -1) {
        // disconnect
        close();
    }
    return res;
}


/**
 * @brief Close connection; stop streaming
 */
void PgReplConnection::close()
{
    if (_started_streaming) {
        endStreaming();
        _started_streaming = false;
    }

    if (_copy_buffer != nullptr) {
        PQfreemem(_copy_buffer);
        _copy_buffer = nullptr;
        _copy_buffer_length = 0;
    }
}


/**
 * @brief Start streaming at specified LSN,
 *
 * @param LSN LSN to start streaming from; INVALID_LSN (0) specifies server current LSN
 * @return 0 success, <0 failure
 */
int PgReplConnection::startStreaming(LSN_t LSN)
{
    // get protocol version
    _server_version = PQserverVersion(_connection);

    /*
    int proto_version =
        server_version >= 160000 ? 4 :      // pg16+ supports 4
        server_version >= 150000 ? 3 :      // pg15+ supports 3
        server_version >= 140000 ? 2 : 1;   // pg14+ supports 2; <14 supports 1
    */
    // for now hardcode proto version to 1 since that is all we support
    _proto_version = 1;

    // convert LSN to X/X format
    uint32_t lsn_higher = (uint32_t)(LSN>>32);
    uint32_t lsn_lower = (uint32_t)(LSN);

    // create replication start command
    char *slot_name = PQescapeLiteral(_connection, _slot_name.c_str(), _slot_name.length());

    std::stringstream s;
    s << "START_REPLICATION SLOT \"" << slot_name << "\" LOGICAL "
      << std::hex << lsn_higher << "/" << std::hex << lsn_lower
      << " (proto_version '" << _proto_version << "')";

    const std::string& tmp = s.str();
    const char* cmd = tmp.c_str();

    // execute command remotely
    int res = pgExec(cmd);

    PQfreemem(slot_name);

    if (res == 0) {
        _started_streaming = true;
    }

    return res;
}


/**
 * @brief End streaming
 * @return 0 on success, -1 on failure
 */
int PgReplConnection::endStreaming()
{
    if (!_started_streaming) {
        return 0;
    }

    // taken from libpqwalreceiver.c libpqrcv_endstreaming()
    // send copy end message
    if (PQputCopyEnd(_connection, nullptr) <= 0 || PQflush(_connection)) {
        std::cerr << "Error could not send end-of-streaming message to primary";
        return -1;
    }

    PGresult *res = PQgetResult(_connection);

    ExecStatusType status = PQresultStatus(res);
    if (status == PGRES_TUPLES_OK) {
        if (PQnfields(res) < 2 || PQntuples(res) != 1) {
            // error unexpected result set after end-of-streaming
            return -1;
        }
        PQclear(res);

        // result should be followed by CommandComplete
        res = PQgetResult(_connection);

    } else if (status == PGRES_COPY_OUT) {
        PQclear(res);
        if (PQendcopy(_connection)) {
            // error shutting down copy
            return -1;
        }
        // result should be followed by CommandComplete
        res = PQgetResult(_connection);
    }

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // error reading result of streaming command
        return -1;
    }
    PQclear(res);

    // verify no more results
    res = PQgetResult(_connection);
    if (res != nullptr) {
        PQclear(res);
        // unexpected result after command complete
        return -1;
    }

    return 0;
}


/**
 * @brief Check the data stream for waiting data
 * @details [long description]
 *
 * @param timeout_secs timeout in seconds; 0 return immediately
 * @return 0 if no data (would block); -1 on error; 1 if data
 */
int PgReplConnection::checkDataStream(int timeout_secs)
{
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(PQsocket(_connection), &fds);

    struct timeval t;
    t.tv_usec = 0;
    t.tv_sec = timeout_secs;

    return select(PQsocket(_connection) + 1, &fds, nullptr, nullptr, &t);
}


/**
 * @brief Read copy data from server; will block
 *
 * @param dataOut  Reference to hold output buffer and length; freed on
 *                 next call to readData
 * @return 0 success; -1 end of stream; -2 some other error
 */
int PgReplConnection::readData(PgCopyData &dataOut)
{
    // calling readData implicitly ack's the last received LSN
    setLastFlushedLSN(_last_received_lsn);

    int rawlen = 0;
    while (rawlen == 0) {

        if (_copy_buffer != nullptr) {
            PQfreemem(_copy_buffer);
            _copy_buffer = nullptr;
            _copy_buffer_offset = 0;
        }

        // get copy data, the 1 indicates async
        int rawlen = PQgetCopyData(_connection, &_copy_buffer, 1);
        if (rawlen == 0) {

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

            // select wait on data stream
            int r = checkDataStream(READ_TIMEOUT_SEC);
            if (r == -1) {
                // end of stream
                std::cerr << "End of streaming";
                return -1;
            }

            if (r > 0) {
                // r > 0 indicating data available
                // consume it and go around again trying to read it
                if (PQconsumeInput(_connection) == 0) {
                    // error can't consume data
                    std::cerr << "Error consuming data";
                    return -1;
                }
            }
        } else if (rawlen == -1) {
            std::cerr << "End of streaming";
            return -1;
        } else if (rawlen == -2) {
            std::cerr << "No copy in progress";
            return -2;
        }
    }

    _copy_buffer_length = rawlen;

    // got data, decode copy header
    // see: https://www.postgresql.org/docs/14/protocol-replication.html
    switch (_copy_buffer[0]) {
        case MSG_KEEP_ALIVE:
            _copy_buffer_offset = processKeepAlive(_copy_buffer, rawlen);
            break;

        case MSG_XLOG_DATA:
            _copy_buffer_offset = processXlogHeader(_copy_buffer, rawlen);
            break;

        default:
            std::cerr << "Unknown copy data command: " << _copy_buffer[0];
            return -1;
    }

    // set output
    dataOut.buffer = _copy_buffer + _copy_buffer_offset;
    dataOut.length = rawlen - _copy_buffer_offset;

    return 0;
}


/**
 * @brief Decode keep alive
 * @return number of bytes consumed
 */
int PgReplConnection::processKeepAlive(const char *buffer, int length)
{
    // handle keep alive
    if (length < (1 + 8 + 8)) {
        std::cerr << "Error keep alive msg too small: " << length;
        return -1;
    }

    int pos = 1;

    LSN_t wal_end = recvint64(&buffer[pos]); // read wal end LSN
    pos += 8;

    int64_t send_time = recvint64(&buffer[pos]);
    pos += 8;

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
 */
int PgReplConnection::processXlogHeader(const char *buffer, int length)
{
    // handle log data
    if (length < (1 + 8 + 8 + 8)) {
        std::cerr << "Error xlog data message too small: " << length;
        return false;
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
 */
void PgReplConnection::fastForwardStream()
{
    // execute query: SELECT pg_current_wal_lsn()
    const char *cmd = CURRENT_LSN_SQL;

    // execute query
    PGresult *res = PQexec(_connection, cmd);

    // process results
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) <= 0) {
        // error
        return;
    }

    LSN_t lsn = (LSN_t)std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);

    // check that LSN is ahead of where we are
    if (lsn < _last_received_lsn) {
        // nothing to do, we'll get there
        return;
    }

    // check that there is still no data; 0 means no data
    // timeout of 0 means return immediately (i.e., poll)
    int r = checkDataStream(0);
    if (r != 0) {
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
 * @brief Check if slot exists
 *
 * @return true if exists, false otherwise
 */
bool PgReplConnection::checkSlotExists()
{
    char *slot_name = PQescapeLiteral(_connection, _slot_name.c_str(), _slot_name.length());

    std::stringstream s;
    s << "SELECT 1 from pg_catalog.pg_replication slots WHERE slot_name=" << slot_name;

    const std::string& tmp = s.str();
    const char* cmd = tmp.c_str();

    // execute query
    PGresult *res = PQexec(_connection, cmd);

    // free slot name
    PQfreemem(slot_name);

    // process results
    if (PQresultStatus(res) == PGRES_COMMAND_OK ||
        PQresultStatus(res) == PGRES_TUPLES_OK) {
        if (PQntuples(res) > 0) {
            int res_int = (int)std::atoi(PQgetvalue(res, 0, 0));
            if (res_int == 1) {
                PQclear(res);
                return true;
            }
        }
    }

    PQclear(res);
    return false;
}


/**
 * @brief Drop replication slot
 *
 * @return 0 on success, -1 otherwise
 */
int PgReplConnection::dropReplicationSlot()
{
    std::stringstream s;

    char *slot_name = PQescapeLiteral(_connection, _slot_name.c_str(), _slot_name.length());

    s << "DROP REPLICATION SLOT " << "\"" << _slot_name << "\"";

    const std::string& tmp = s.str();
    const char* cmd = tmp.c_str();

    // execute query
    PGresult *res = PQexec(_connection, cmd);

    PQfreemem(slot_name);

    // process results
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        std::cerr << "Error dropping replication slot";
        PQclear(res);
        return -1;
    }

    PQclear(res);
    return 0;
}


/**
 * @brief Create replication slot
 *
 * @param output_plugin one of:
 * @param export_snapshot should export be create (true/false)
 * @param temporary is replication slot temporary
 * @return 0 success, -1 failure
 */
int PgReplConnection::createReplicationSlot(OutputPlugin output_plugin,
                                            bool export_snapshot,
                                            bool temporary)
{
    std::stringstream s;

    // escape slot name
    char *slot_name = PQescapeLiteral(_connection, _slot_name.c_str(), _slot_name.length());

    // setup command
    s << "CREATE REPLICATION SLOT " << "\"" << _slot_name << "\"";
    if (temporary) {
        s << " TEMPORARY";
    }

    s << " LOGICAL";
    if (output_plugin == OutputPlugin::WAL2JSON) {
        s << " wal2json";
    } else if (output_plugin == OutputPlugin::PGOUTPUT) {
        s << " pgoutput";
    }

    // uses old format style
    if (export_snapshot) {
        s << " EXPORT_SNAPSHOT";
    } else {
        s << " NOEXPORT_SNAPSHOT";
    }

    const std::string& tmp = s.str();
    const char* cmd = tmp.c_str();

    // execute query
    PGresult *res = PQexec(_connection, cmd);

    // free slot name
    PQfreemem(slot_name);

    // process results
    if (PQresultStatus(res) == PGRES_TUPLES_OK) {
        if (PQntuples(res) != 1 || PQnfields(res) != 4) {
            std::cerr << "Unexpected number of rows or columns for CREATE REPLICATION SLOT: rows="
                 <<  PQntuples(res) << " cols=" << PQnfields(res);
        } else {
            slot_name = PQgetvalue(res, 0, 0);
            // char *consistent_point = PQgetvalue(res, 0, 1); // XXX/XXX earliest LSN for streaming
            char *snapshot_name = PQgetvalue(res, 0, 2);
            // char *output_plugin = PQgetvalue(res, 0, 3);  // unused

            // save export name in class
            _export_name = std::string(snapshot_name);

            PQclear(res);

            return 0;
        }
    } else {
        std::cerr << "Error executing CREATE REPLICATION SLOT";
    }

    PQclear(res);
    return -1;
}


/**
 * @brief Send standby status feedback message to server
 * @return 0 on success, -1 on failure
 */
int PgReplConnection::sendStandbyStatusMsg()
{
    char replybuf[1 + 8 + 8 + 8 + 8 + 1];
    int64_t now = getPgTimeInMillis();

    // set applied lsn and flushed lsn to same value
    int len = encodeStandbyStatusMsg(_last_flushed_lsn,
                                     _last_flushed_lsn,
                                     now, replybuf);

    if (PQputCopyData(_connection, replybuf, len) <= 0 || PQflush(_connection)) {
        std::cerr << "Error sending standby status update";
        return -1;
    }

    _last_status_time = now;

    return 0;
}

/**
 * @brief Update last flushed lsn, we are safe to move log forward to here
 *
 * @param lsn LSN indicating safe point to truncate log up to
 */
void PgReplConnection::setLastFlushedLSN(LSN_t lsn)
{
    if (lsn == INVALID_LSN || lsn <= _last_flushed_lsn) {
        return;
    }

    _last_flushed_lsn = lsn;
    _last_flushed_time = getPgTimeInMillis();
}
