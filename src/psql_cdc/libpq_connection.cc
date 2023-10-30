#include <string>
#include <iostream>
#include <climits>
#include <optional>
#include <fmt/core.h>

#include <psql_cdc/libpq_connection.hh>
#include <psql_cdc/exception.hh>

namespace springtail {

    /** SQL command to set serach path */
    static const char *ALWAYS_SECURE_SEARCH_PATH_SQL =
        "SELECT pg_catalog.set_config('search_path', '', false);";

    /** start the xact in repeatable read isolation, creates a snapshot at xact start */
    static const char *BEGIN_QUERY = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ";

    /** end xact */
    static const char *END_QUERY = "END";

    /**
     * @brief Start a transaction
     */
    void LibPqConnection::startTransaction()
    {
        exec(BEGIN_QUERY);
        clear();
        _in_transaction = true;
    }


    /**
     * @brief End a transaction
     */
    void LibPqConnection::endTransaction()
    {
        exec(END_QUERY);
        clear();
        _in_transaction = false;
    }


    /**
     * @brief Clear result if result exists (frees underlying resources)
     */
    void LibPqConnection::clear()
    {
        if (_result == nullptr) {
            return;
        }
        PQclear(_result);
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
        return PQntuples(_result);
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
        return PQbinaryTuples(_result);
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
        return PQnfields(_result);
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

        return PQgetCopyData(_connection, &_buffer, (async ? 1 : 0));
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
        return PQresultStatus(_result);
    }


    /**
     * @brief Get error message from connection; should not be freed
     * @return pointer to error message from underlying connection; should not be freed
     */
    char *LibPqConnection::error_message()
    {
        return PQerrorMessage(_connection);
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

        // clear old result if there was one
        clear();

        std::cout << "Executing query: " << cmd << std::endl;
        PGresult *res = PQexec(_connection, cmd);
        if (PQresultStatus(res) != PGRES_COMMAND_OK &&
            PQresultStatus(res) != PGRES_TUPLES_OK &&
            PQresultStatus(res) != PGRES_COPY_OUT) {
            std::cerr << "Error executing query: " << PQerrorMessage(_connection)
                      << ", status=" << PQresultStatus(res) << std::endl;
            PQclear(res);
            throw PgQueryError();
        }

        _result = res;
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
    int32_t LibPqConnection::getInt32(int row, int col)
    {
        char *value = PQgetvalue(_result, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        int32_t r = std::stoi(value, nullptr, 10);
        return r;
    }

    /**
     * @brief Retreive a string column value from a query result; maps NULL to empty string
     *
     * @param row row index
     * @param col col index
     * @return string value for row/col; null is mapped to empty string
     */
    std::string LibPqConnection::getString(int row, int col)
    {
        char *value = PQgetvalue(_result, row, col);
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
    std::optional<std::string> LibPqConnection::getStringOptional(int row, int col)
    {
        char *value = PQgetvalue(_result, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }
        // PQgetvalue maps NULL to empty string, so need to explicitly check
        if (value[0] == '\0' && PQgetisnull(_result, row, col)) {
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
    bool LibPqConnection::getBoolean(int row, int col)
    {
        char *value = PQgetvalue(_result, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        if (value[0] == 't' || value[0] == 1 || value[0] == 'y') {
            return true;
        }

        return false;
    }

    /**
     * @brief escape a string
     *        when a connection is available it uses connection encoding
     *
     * @param str string to escape
     * @return safe ptr to the string, access with .get()
     */
    std::unique_ptr<char[]> LibPqConnection::escapeString(const std::string &str)
    {
        std::unique_ptr<char[]> str_ptr(new char[str.length() * 2 + 1]);

        if (_connection == nullptr) {
            PQescapeString(str_ptr.get(), str.c_str(), str.length());
        }

        PQescapeStringConn(const_cast<PGconn *>(_connection), str_ptr.get(),
                           str.c_str(), str.length(), nullptr);

        return str_ptr;
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
        std::unique_ptr<char[]> host = escapeString(db_host);
        std::unique_ptr<char[]> name = escapeString(db_name);
        std::unique_ptr<char[]> user = escapeString(db_user);
        std::unique_ptr<char[]> pass = escapeString(db_pass);

        // setting client encoding to UTF8
        // setting database=replication to put connection in replication mode
        std::string encoding("UTF8");

        std::string conninfo = fmt::format("host={} port={} dbname={} user={} \
            password={} {}client_encoding={} \
            options='-c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3'",
            host.get(), db_port, name.get(), user.get(), pass.get(),
            (replication ? "replication=database ": ""), encoding);

        std::cout << "Attempting to connect: " << conninfo << std::endl;

        // try connection
        PGconn *connection = PQconnectdb(conninfo.c_str());
        if (PQstatus(connection) != CONNECTION_OK) {
            std::cerr << "Error connecting: " << PQerrorMessage(connection) << std::endl;
            PQfinish(connection);
            throw PgIOError();
        }

        std::cout << fmt::format("PG connected, protocol version={}, server version={}\n",
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
                endTransaction();
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
        return (_connection != nullptr);
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
        return PQsocket(_connection);
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
        return PQgetvalue(_result, row, col);
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
        return PQgetlength(_result, row, col);
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
        _result = PQgetResult(_connection);
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
        _result = PQgetResult(_connection);
        return res;
    }

    /**
     * @brief Force a fetch (update) of the internal result; usually not necessary to call
     *        as command will fetch the result
     * @return true if result is not null
     */
    bool LibPqConnection::fetch_result()
    {
        _result = PQgetResult(_connection);
        return (_result != nullptr);
    }

};