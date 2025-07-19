#include <string>
#include <climits>
#include <optional>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

#include <fmt/core.h>
#include <absl/log/check.h>

#include <pg_repl/libpq_connection.hh>
#include <pg_repl/exception.hh>

#include <common/logging.hh>
#include <common/dns_resolver.hh>

namespace springtail {

    /** SQL command to set serach path */
    static const char *ALWAYS_SECURE_SEARCH_PATH_SQL =
        "SELECT pg_catalog.set_config('search_path', '', false);";

    /** start the xact in repeatable read isolation, creates a snapshot at xact start */
    static const char *BEGIN_QUERY = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ";

    /** end xact */
    static const char *END_QUERY = "END";

    /** rollback xact */
    static const char *ROLLBACK_QUERY = "ROLLBACK";

    /**
     * @brief Start a transaction
     */
    void LibPqConnection::start_transaction()
    {
        exec(BEGIN_QUERY);
        clear();
        _in_transaction = true;
    }


    /**
     * @brief End a transaction
     */
    void LibPqConnection::end_transaction()
    {
        if (!_in_transaction) {
            LOG_WARN("Cannot end transaction: not in a transaction");
            return;
        }

        exec(END_QUERY);
        clear();
        _in_transaction = false;
    }

    void LibPqConnection::rollback_transaction()
    {
        if (!_in_transaction) {
            LOG_WARN("Cannot rollback transaction: not in a transaction");
            return;
        }

        exec(ROLLBACK_QUERY);
        clear();
        _in_transaction = false;
    }


    void LibPqConnection::savepoint(const std::string &name)
    {
        if (!_in_transaction) {
            LOG_WARN("Cannot create savepoint: not in a transaction");
            return;
        }

        std::string cmd = fmt::format("SAVEPOINT {}", name);
        exec(cmd);
        clear();
    }

    void LibPqConnection::release_savepoint(const std::string &name)
    {
        if (!_in_transaction) {
            LOG_WARN("Cannot release savepoint: not in a transaction");
            return;
        }

        std::string cmd = fmt::format("RELEASE SAVEPOINT {}", name);
        exec(cmd);
        clear();
    }

    void LibPqConnection::rollback_savepoint(const std::string &name)
    {
        if (!_in_transaction) {
            LOG_WARN("Cannot rollback savepoint: not in a transaction");
            return;
        }

        std::string cmd = fmt::format("ROLLBACK TO SAVEPOINT {}", name);
        exec(cmd);
        clear();
    }

    /**
     * @brief Clear result if result exists (frees underlying resources)
     */
    void LibPqConnection::clear()
    {
        if (_result == nullptr) {
            return;
        }
        _result = nullptr;
    }

    /**
     * @brief Get number of tuples from last query result
     * @return number of tuples from last query or 0.
     */
    int LibPqConnection::ntuples()
    {
        if (_result == nullptr) {
            return 0;
        }
        return PQntuples(_result->result);
    }

    /**
     * @brief Get number of binary tuples from last query result
     * @return number of binary tuples from last query or 0.
     */
    int LibPqConnection::binary_tuples()
    {
        if (_result == nullptr) {
            return 0;
        }
        return PQbinaryTuples(_result->result);
    }

    /**
     * @brief Get number of fields from last query result
     * @return number of fields from last query or 0.
     */
    int LibPqConnection::nfields()
    {
        if (_result == nullptr) {
            return 0;
        }
        return PQnfields(_result->result);
    }

    /**
     * @brief Get copy data; uses internal buffer
     * @param async  flag indicating async operation
     */
    int LibPqConnection::get_copy_data(bool async)
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        free_copy_buffer();
        clear();

        int res = PQgetCopyData(_connection, &_buffer, (async ? 1 : 0));
        _result = std::make_shared<LibPqResult>(PQgetResult(_connection));

        return res;
    }

    /**
     * @brief Get pointer to internal copy buffer
     * @return internal copy buffer; should be freed by free_copy_data()
     */
    char *LibPqConnection::get_copy_buffer()
    {
        return _buffer;
    }

    /**
     * @brief Free copy data buffer from get_copy_data() if not already freed
     */
    void LibPqConnection::free_copy_buffer()
    {
        if (_buffer == nullptr) {
            return;
        }
        PQfreemem(_buffer);
        _buffer = nullptr;
    }

    /**
     * @brief Get result status code
     * @return result status code; e.g., PGRES_COPY_OUT, PGRES_COMMAND_OK, etc.
     */
    ExecStatusType LibPqConnection::status()
    {
        if (_result == nullptr) {
            throw PgNoResultError();
        }
        return PQresultStatus(_result->result);
    }


    /**
     * @brief Get error message from connection; should not be freed
     * @return pointer to error message from underlying connection; should not be freed
     */
    std::string LibPqConnection::error_message()
    {
        char *err = PQerrorMessage(_connection);
        if (err == nullptr) {
            return {};
        }
        return std::string(err);
    }

    std::string LibPqConnection::result_error_message()
    {
        // try and get error message from result; if not available, then return connection error message
        if (_result == nullptr) {
            return error_message();
        }

        char *err = PQresultErrorMessage(_result->result);
        if (err == nullptr) {
            return error_message();
        }
        return std::string(err);
    }

    bool LibPqConnection::exec_no_throw(const std::string &cmd, bool use_savepoint)
    {
        return exec_no_throw(cmd.c_str(), use_savepoint);
    }

    bool LibPqConnection::exec_no_throw(const char *cmd, bool use_savepoint)
    {
        if (_connection == nullptr) {
            return false;
        }

        // if we are in a transaction, create a savepoint
        // so that we can rollback to it if the command fails
        std::string savepoint_name;
        if (_in_transaction && use_savepoint) {
            savepoint_name = fmt::format("savepoint_{}", _savepoint_ctr++);
            savepoint(savepoint_name);
        }

        LOG_DEBUG(LOG_PG_REPL, "Executing query: {}", cmd);
        _result = std::make_shared<LibPqResult>(PQexec(_connection, cmd));

        auto status = PQresultStatus(_result->result);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK && status != PGRES_COPY_OUT) {
            if (!savepoint_name.empty()) {
                auto old_result = _result; // keep the result for logging
                // rollback to savepoint if in transaction
                LOG_DEBUG(LOG_PG_REPL, "Rolling back to savepoint: {}, error on exec: {}", savepoint_name, result_error_message());
                rollback_savepoint(savepoint_name);
                _result = old_result; // restore the result for logging
            }
            return false; // error executing query
        }

        // release savepoint if we created one
        if (!savepoint_name.empty()) {
            LOG_DEBUG(LOG_PG_REPL, "Releasing savepoint: {}", savepoint_name);
            release_savepoint(savepoint_name);
        }

        return true;
    }

    /**
     * @brief Execute libpq query helper; sets result internally
     * @param cmd SQL command
     */
    void LibPqConnection::exec(const std::string &cmd)
    {
        exec(cmd.c_str());
    }

    /**
     * @brief Execute libpq query helper; sets result internally
     * @param cmd SQL command
     */
    void LibPqConnection::exec(const char *cmd)
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        if (exec_no_throw(cmd, false) == false) {
            std::string error_message = fmt::format("msg={}, status={}", PQerrorMessage(_connection), PQresultErrorMessage(_result->result));
            LOG_ERROR("Error executing query: {}", error_message);
            throw PgQueryError();
        }
    }

    std::optional<std::string> LibPqConnection::get_sql_state()
    {
        if (_result == nullptr) {
            return std::nullopt;
        }

        const char *state = PQresultErrorField(_result->result, PG_DIAG_SQLSTATE);
        if (state == nullptr) {
            throw PgQueryError();
        }

        std::string state_str{state};
        if (state_str.empty() || state_str == SQL_SUCCESSFUL_COMPLETION) {
            return std::nullopt;
        }

        return state_str;
    }

    /**
     * @brief Retreive an int32 column value from a query result
     *
     * @param row row index
     * @param col col index
     * @return int32 value for row/col
     * @throws std::invalid_argument
     * @throws std::out_of_range
     */
    int32_t LibPqConnection::get_int32(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        char *end;
        int32_t r = std::strtol(value, &end, 10);
        if (end == value) {
            throw PgQueryError();
        }
        return r;
    }

    std::optional<int32_t>
    LibPqConnection::get_int32_optional(int row, int col)
    {
        if (PQgetisnull(_result->result, row, col)) {
            return {};
        }
        return get_int32(row, col);
    }

    int64_t LibPqConnection::get_int64(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        char *end;
        int64_t r = std::strtoll(value, &end, 10);
        if (end == value) {
            throw PgQueryError();
        }
        return r;
    }

    float LibPqConnection::get_float(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        char *end;
        float f = std::strtof(value, &end);
        if (end == value) {
            throw PgQueryError();
        }
        return f;
    }

    /**
     * @brief Retreive a string column value from a query result; maps NULL to empty string
     *
     * @param row row index
     * @param col col index
     * @return string value for row/col; null is mapped to empty string
     */
    std::string LibPqConnection::get_string(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        return std::string(value);
    }

    /**
     * @brief Retreive a string column value from a query result; maintains NULL value
     *
     * @param row row index
     * @param col col index
     * @return string value for row/col; optional is false if string is null
     */
    std::optional<std::string> LibPqConnection::get_string_optional(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }
        // PQgetvalue maps NULL to empty string, so need to explicitly check
        if (value[0] == '\0' && PQgetisnull(_result->result, row, col)) {
            // string is actually null
            return {};
        }

        return std::string(value);
    }


    /**
     * @brief Get boolean value from query result
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return boolean value for row/col; null is mapped to false
     */
    bool LibPqConnection::get_boolean(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        if (value[0] == 't' || value[0] == 1 || value[0] == 'y') {
            return true;
        }

        return false;
    }

    /**
     * @brief Get char value from query result
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return char value for row/col; null is mapped to '\0'
     */
    char LibPqConnection::get_char(int row, int col)
    {
        char *value = get_value(row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        return value[0];
    }

    /**
     * @brief escape a string; a literal not for identifiers
     *        when a connection is available it uses connection encoding
     *
     * @param str string to escape
     * @return string
     */
    std::string LibPqConnection::escape_string(const std::string &str)
    {
        char new_str[str.length() * 2 + 1];
        if (_connection == nullptr) {
            PQescapeString(new_str, str.c_str(), str.length());
        } else {
            PQescapeStringConn(const_cast<PGconn *>(_connection), new_str,
                               str.c_str(), str.length(), nullptr);
        }

        return std::string(new_str);
    }

    std::string LibPqConnection::_escape_identifier(const char *input)
    {
        int output_size = 2 * strlen(input) + 1;
        std::vector<char> data(output_size);
        char *output = data.data();
        const char *p = input;
        size_t len = 0;

        // Start with a double quote
        if (len < output_size - 1) {
            output[len++] = '"';
        }

        // Escape double quotes
        while (*p && len < output_size - 1) {
            if (*p == '"') {
                if (len < output_size - 2) {
                    output[len++] = '"';
                    output[len++] = '"';
                }
            } else {
                output[len++] = *p;
            }
            p++;
        }

        // End with a double quote
        if (len < output_size - 1) {
            output[len++] = '"';
        }

        output[len] = '\0'; // Null-terminate the output

        return std::string(output);
    }

    std::string LibPqConnection::escape_identifier(const std::string &str)
    {
        if (_connection == nullptr) {
            // just escape quotes, very simple
            return _escape_identifier(str.c_str());
        }

        // more complex, use libpq, takes char encoding into account
        char *new_str = PQescapeIdentifier(_connection, str.c_str(), str.length());

        // copy the string and free the memory
        std::string ret(new_str);
        PQfreemem(new_str);

        return ret;
    }

    /**
     * @brief Connection libpq helper; generate connection info string
     *
     * @param db_host hostname
     * @param db_name db name
     * @param db_user user name
     * @param db_pass password
     * @param db_port port
     * @param replication is connection for replication streaming (true/false)
     */
    void LibPqConnection::connect(const std::string &db_host,
                                  const std::string &db_name,
                                  const std::string &db_user,
                                  const std::string &db_pass,
                                  const int db_port,
                                  const bool replication)
    {
        if (_connection != nullptr) {
            throw PgAlreadyConnectedError();
        }

        // create key value list for: host, port, dbname, user, password, options
        // escape options
        std::string name = escape_string(db_name);
        std::string user = escape_string(db_user);
        std::string pass = escape_string(db_pass);

        // setting client encoding to UTF8
        // setting database=replication to put connection in replication mode
        std::string encoding("UTF8");

        std::string hosttype;
        std::string host;

        PGconn *connection = nullptr;

        int retries = 0;
        int backoff = RETRY_SLEEP_SECS;
        bool do_dns_lookup = false;

        while (retries < MAX_RETRY_COUNT) {
            if (do_dns_lookup) {
                // do ip lookup for hostname using internal dns resolver
                DNSResolver *resolver = DNSResolver::get_instance();
                std::optional<std::string> ip = resolver->resolve(db_host);
                if (!ip.has_value()) {
                    LOG_DEBUG(LOG_PG_REPL, "Failed to resolve hostname: {}", db_host);
                    throw PgConnectionError();
                }

                hosttype = "hostaddr";
                host = escape_string(ip.value());
            } else {
                // allow libpq to do the hostname lookup (using system resolver)
                hosttype = "host";
                host = escape_string(db_host);
            }

            // generate connection string
            std::string conninfo = fmt::format("{}='{}' port={} dbname='{}' user='{}' \
                password='{}' {}client_encoding={} \
                options='-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3'",
                hosttype, host, db_port, name, user, pass,
                (replication ? "replication=database ": ""), encoding);

            LOG_DEBUG(LOG_PG_REPL, "Attempting to connect: {}", conninfo);

            // try connection
            connection = PQconnectdb(conninfo.c_str());
            if (PQstatus(connection) != CONNECTION_OK) {
                // mask out password for logs
                std::string conninfo = fmt::format("{}='{}' port={} dbname='{}' user='{}' \
                    password='****' {}client_encoding={} \
                    options='-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3'",
                    hosttype, host, db_port, name, user,
                    (replication ? "replication=database ": ""), encoding);

                LOG_ERROR("Error connecting: conninfo: {}, msg: {}", conninfo, PQerrorMessage(connection));
                PQfinish(connection);

                // sleep and backoff
                std::this_thread::sleep_for(std::chrono::seconds(backoff));
                do_dns_lookup = true;

                backoff *= 2;
                retries++;
            } else {
                break;
            }
        }

        if (retries >= MAX_RETRY_COUNT) {
            LOG_ERROR("Failed to connect to database, too many retries: {}", db_name);
            throw PgConnectionError();
        }

        LOG_DEBUG(LOG_PG_REPL, "PG connected, protocol version={}, server version={}",
                            PQprotocolVersion(connection), PQserverVersion(connection));

        // for safety set search path
        try {
            PGresult *res = PQexec(connection, ALWAYS_SECURE_SEARCH_PATH_SQL);
            PQclear(res);
        } catch(PgQueryError &e) {
            // disconnect
            PQfinish(connection);
            throw e;
        }

        _connection = connection;
        _is_replication = replication;
    }

    /**
     * @brief Disconnect connection, ignore errors
     */
    void LibPqConnection::disconnect()
    {
        if (_connection == nullptr) {
            return;
        }

        if (_in_transaction) {
            try {
                // end transation if in one
                end_transaction();
            } catch (PgQueryError &e) {}
        }

        // free results if any
        clear();

        // free copy data if any
        free_copy_buffer();

        PQfinish(_connection);
        _connection = nullptr;
        _in_transaction = false;
    }


    /**
     * @brief Is the class connected
     * @return true if connected; false otherwise
     */
    bool LibPqConnection::is_connected()
    {
        if (_connection != nullptr && PQstatus(_connection) == CONNECTION_OK) {
            return true;
        }
        return false;
    }


    /**
     * @brief Get underlying socket from connection; use with care
     * @return socket fd
     */
    int LibPqConnection::socket()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        assert (_is_replication);

        return PQsocket(_connection);
    }

    bool LibPqConnection::is_ssl()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }
        return (PQsslInUse(_connection) == 1);  // 1 = true, 0 = false
    }

    int LibPqConnection::wait_until_readable(int timeout_sec)
    {
        assert (_is_replication);

        fd_set readfds;
        struct timeval timeout;
        struct timeval *tv;
        int sockfd = socket();

        while(true) {
            // Initialize the write file descriptor set
            FD_ZERO(&readfds);
            FD_SET(sockfd, &readfds);

            // Set the timeout (timeout_sec seconds, 0 microseconds)
            timeout.tv_sec = timeout_sec;
            timeout.tv_usec = 0;

            if (timeout_sec == -1) {
                tv = NULL;
            } else {
                tv = &timeout;
            }

            // Wait until the socket is readable or timeout occurs
            int result = select(sockfd + 1, &readfds, NULL, NULL, tv);

            if (result > 0 && FD_ISSET(sockfd, &readfds)) {
                // Socket is readable
                return 1;
            }

            if (result == 0) {
                // Timeout occurred
                return 0;
            }

            if (result == -1 && errno != EINTR) {
                // Error occurred
                return result;
            }
        }

        return 0;
    }

    int LibPqConnection::wait_until_writable(int timeout_sec)
    {
        assert (_is_replication);

        fd_set writefds;
        struct timeval timeout;
        struct timeval *tv;
        int sockfd = socket();

        while(true) {
            // Initialize the write file descriptor set
            FD_ZERO(&writefds);
            FD_SET(sockfd, &writefds);

            // Set the timeout (timeout_sec seconds, 0 microseconds)
            timeout.tv_sec = timeout_sec;
            timeout.tv_usec = 0;

            if (timeout_sec == -1) {
                tv = NULL;
            } else {
                tv = &timeout;
            }

            // Wait until the socket is writable or timeout occurs
            int result = select(sockfd + 1, NULL, &writefds, NULL, tv);

            if (result > 0 && FD_ISSET(sockfd, &writefds)) {
                // Socket is writable
                return 1;
            }

            if (result == 0) {
                // Timeout occurred
                return 0;
            }

            if (result == -1 && errno != EINTR) {
                // Error occurred
                return result;
            }
        }

        return 0;
    }

    ssize_t LibPqConnection::read(char *buf, size_t count, bool async)
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        assert (_is_replication);

        size_t off = 0;

        while (off < count) {
            // note this uses an internal interface
            ssize_t r = _secure_read(_connection, buf + off, count - off);

            // incr offset
            if (r > 0) {
                // return what data we have read, even if not async
                off += r;
                return off;
            }

            DCHECK(off == 0);

            if (r == 0) {
                // when using ssl, pqsecure_read will return 0 on SSL_ERROR_WANT_READ
                // typically this would be eof, but this is complicated, see
                // pqReadData() in https://doxygen.postgresql.org/fe-misc_8c_source.html
                if (errno == EPIPE || errno == ECONNRESET || errno == ECONNABORTED ||
                    errno == EHOSTDOWN || errno == EHOSTUNREACH || errno == ENETDOWN ||
                    errno == ENETRESET || errno == ENETUNREACH || errno == ETIMEDOUT) {
                    errno = ECONNRESET;
                    return -1;
                }

                // poll to see if there is data (timeout of 0)
                int rc = wait_until_readable(0);
                if (rc < 0) {
                    // on error return econnreset
                    errno = ECONNRESET;
                    return -1;
                } else if (rc > 0) {
                    // data is available, go around again
                    continue;
                } else if (async) {
                    assert (rc == 0);
                    // rc == 0, no data available, return 0 if async;
                    // otherwise fall through and wait for data
                    errno = EWOULDBLOCK;
                    return -1;
                }
            }

            if (r < 0) {
                // if async, or error and error is not a would block or again, then return error
                if (async || (!(errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR))) {
                    return (off == 0) ? r : off;
                }
            }

            // not async, fail through; block until socket is readable
            r = wait_until_readable();
            if (r < 0) {
                errno = ECONNRESET;
                return r;
            }
        }
        return off;
    }

    ssize_t LibPqConnection::write(const char *buf, size_t count, bool async)
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        assert (_is_replication);

        size_t off = 0;

        while (off < count) {
            // note this uses an internal interface
            ssize_t r = _secure_write(_connection, buf + off, count - off);
            // if async, or error and error is not a would block or again, then return error
            if (async || r < 0 || (!(errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR))) {
                return (off == 0) ? r : off;
            }

            // incr offset
            if (r > 0) {
                off += r;
                continue;
            }

            // block until socket is writable
            r = wait_until_writable();
            if (r < 0) {
                return (off == 0) ? r : off;
            }
        }
        return off;
    }

    /**
     * @brief Get raw value from result; use with care, freed by clear()
     * @param row row index
     * @param col column index
     * @return pointer based on result value
     */
    char *LibPqConnection::get_value(int row, int col)
    {
        if (_result == nullptr) {
            return nullptr;
        }
        return PQgetvalue(_result->result, row, col);
    }


    /**
     * @brief Flush connection
     * @return 1 unable to send all data -- wait for ready; -1 on failure; 0 success
     */
    int LibPqConnection::flush()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }
        return PQflush(_connection);
    }


    /**
     * @brief Get length
     *
     * @param row row index
     * @param col column index
     *
     * @return length of tuple
     */
    int LibPqConnection::length(int row, int col)
    {
        if (_result == nullptr) {
            return 0;
        }
        return PQgetlength(_result->result, row, col);
    }


    /**
     * @brief Get remote server version
     * @return remote server version; e.g., 11.0 = 110000; or 0
     */
    int LibPqConnection::server_version()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }
        return PQserverVersion(_connection);
    }


    /**
     * @brief Send end-of-data indicator to the server during copy.
     * @param errormsg if errormsg is not null, then copy will fail with error message
     * @return result of 1 if message was sent; call result() to get final result
     */
    int LibPqConnection::put_copy_end(const char *errormsg)
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        clear();

        int res = PQputCopyEnd(_connection, errormsg);
        _result = std::make_shared<LibPqResult>(PQgetResult(_connection));
        return res;
    }


    /**
     * @brief Waits until server has finished the copying
     * @return 0 on success, nonzero otherwise, use error_message() to see error.
     */
    int LibPqConnection::end_copy()
    {
        if (_connection == nullptr) {
            throw PgNotConnectedError();
        }

        clear();

        int res = PQendcopy(_connection);
        _result = std::make_shared<LibPqResult>(PQgetResult(_connection));
        return res;
    }

    /**
     * @brief Force a fetch (update) of the internal result; usually not necessary to call
     *        as command will fetch the result
     * @return true if result is not null
     */
    bool
    LibPqConnection::fetch_result()
    {
        _result = std::make_shared<LibPqResult>(PQgetResult(_connection));
        return (_result != nullptr);
    }

    ssize_t
    LibPqConnection::_secure_write(PGconn *conn,
                                   const char *ptr,
                                   size_t len)
    {
        if (PQsslInUse(conn)) {
            return _ssl_write(conn, ptr, len);
        } else {
            return _non_ssl_write(conn, ptr, len);
        }
    }

    ssize_t
    LibPqConnection::_secure_read(PGconn *conn,
                                  char *ptr,
                                  size_t len)
    {
        if (PQsslInUse(conn)) {
            return _ssl_read(conn, ptr, len);
        } else {
            return _non_ssl_read(conn, ptr, len);
        }
    }

    ssize_t
    LibPqConnection::_non_ssl_read(PGconn *conn,
                                   char *ptr,
                                   size_t len)
    {
        /* See pqsecure_raw_read() in fe-secure.c */
        int sock = PQsocket(conn);
        errno = 0;
        ssize_t n = recv(sock, ptr, len, 0);

        if (n < 0) {
            int result_errno = errno;

            /* Set error message if appropriate */
            switch (result_errno)
            {
                case EAGAIN:
                case EINTR:
                    /* no error message, caller is expected to retry */
                    break;

                case EPIPE:
                case ECONNRESET:
                    break;

                case 0:
                    /* If errno didn't get set, treat it as regular EOF */
                    n = 0;
                    break;

                default:
                    break;
            }
        }
        return n;
    }

    ssize_t
    LibPqConnection::_ssl_read(PGconn *conn,
                               char *ptr,
                               size_t len)
    {
        SSL *ssl = reinterpret_cast<SSL*>(PQgetssl(conn));

        /* See: pgtls_read() in fe-secure-openssl.c. */
        while (true) {
            errno = 0;
            ssize_t n = SSL_read(ssl, ptr, len);
            int err = SSL_get_error(ssl, n);

            switch (err) {
                case SSL_ERROR_NONE:
                    if (n < 0) {
                        /* assume the connection is broken */
                        errno = ECONNRESET;
                    }
                    return n;

                case SSL_ERROR_WANT_READ:
                    return 0;

                case SSL_ERROR_WANT_WRITE:
                    /*
                    * Returning 0 here would cause caller to wait for read-ready,
                    * which is not correct since what SSL wants is wait for
                    * write-ready.  The former could get us stuck in an infinite
                    * wait, so don't risk it; busy-loop instead.
                    */
                    continue;

                case SSL_ERROR_SYSCALL:
                    if (n < 0 && errno != 0) {
                        errno = errno;
                    } else {
                        /* assume the connection is broken */
                        errno = ECONNRESET;
                        n = -1;
                    }
                    return n;

                case SSL_ERROR_SSL:
                    /* assume the connection is broken */
                    errno = ECONNRESET;
                    return -1;

                case SSL_ERROR_ZERO_RETURN:
                    /*
                    * Per OpenSSL documentation, this error code is only returned for
                    * a clean connection closure, so we should not report it as a
                    * server crash.
                    */
                    errno = ECONNRESET;
                    return -1;

                default:
                    /* assume the connection is broken */
                    errno = ECONNRESET;
                    return -1;
            } // switch (err)
        } // while (true)

        // not reached
    }

    ssize_t
    LibPqConnection::_ssl_write(PGconn *conn,
                                const char *ptr,
                                size_t len)
    {
        SSL *ssl = reinterpret_cast<SSL*>(PQgetssl(conn));
        ssize_t     n;
        int         result_errno = 0;
        int         err;

        errno = 0;
        n = SSL_write(ssl, ptr, len);
        err = SSL_get_error(ssl, n);

        switch (err)
        {
            case SSL_ERROR_NONE:
                if (n < 0) {
                    result_errno = ECONNRESET;
                }
                break;

            case SSL_ERROR_WANT_READ:
                /*
                * Returning 0 here causes caller to wait for write-ready, which
                * is not really the right thing, but it's the best we can do.
                */
                n = 0;
                break;

            case SSL_ERROR_WANT_WRITE:
                n = 0;
                break;

            case SSL_ERROR_SYSCALL:
                /*
                * If errno is still zero then assume it's a read EOF situation,
                * and report EOF.  (This seems possible because SSL_write can
                * also do reads.)
                */
                if (n < 0 && errno != 0) {
                    result_errno = errno;
                } else {
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                }
                break;

            case SSL_ERROR_SSL:
                    /* assume the connection is broken */
                    result_errno = ECONNRESET;
                    n = -1;
                    break;

            case SSL_ERROR_ZERO_RETURN:
                /*
                * Per OpenSSL documentation, this error code is only returned for
                * a clean connection closure, so we should not report it as a
                * server crash.
                */
                result_errno = ECONNRESET;
                n = -1;
                break;

            default:
                /* assume the connection is broken */
                result_errno = ECONNRESET;
                n = -1;
                break;
        }

        /* ensure we return the intended errno to caller */
        errno = result_errno;
        return n;
    }

    ssize_t
    LibPqConnection::_non_ssl_write(PGconn *conn,
                                    const char *ptr,
                                    size_t len)
    {
        /* See pqsecure_raw_write() in fe-secure.c */
        int         sock = PQsocket(conn);
        int         flags = MSG_NOSIGNAL;
        int         result_errno = 0;

        ssize_t n = send(sock, ptr, len, flags);

        if (n < 0) {
            result_errno = errno;

            /* Set error message if appropriate */
            switch (result_errno)
            {
                case EAGAIN:
                case EINTR:
                    /* no error message, caller is expected to retry */
                    break;

                case EPIPE:
                    /* FALL THRU */

                case ECONNRESET:
                    //conn->write_failed = true;
                    /* Now claim the write succeeded */
                    n = len;
                    break;

                default:
                    //conn->write_failed = true;
                    /* Now claim the write succeeded */
                    n = len;
                    break;
            }
        }

        /* ensure we return the intended errno to caller */
        errno = result_errno;

        return n;
    }

};
