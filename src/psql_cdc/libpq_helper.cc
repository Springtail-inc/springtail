#include <string>
#include <iostream>
#include <climits>
#include <optional>
#include <fmt/core.h>

#include <psql_cdc/libpq_helper.hh>
#include <psql_cdc/exception.hh>

namespace springtail {

namespace libpq {

    /** SQL command to set serach path */
    static const char *ALWAYS_SECURE_SEARCH_PATH_SQL =
        "SELECT pg_catalog.set_config('search_path', '', false);";

    /**
     * @brief Execute libpq query helper
     * @param connection libpq connection
     * @param cmd SQL command
     * @return result, must be freed with PQclear(res)
     */
    PGresult *exec(PGconn *connection, const std::string &cmd)
    {
        std::cout << "Executing query: " << cmd << std::endl;
        PGresult *res = PQexec(connection, cmd.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK &&
            PQresultStatus(res) != PGRES_TUPLES_OK &&
            PQresultStatus(res) != PGRES_COPY_OUT) {
            std::cerr << "Error executing query: " << PQerrorMessage(connection)
                      << ", status=" << PQresultStatus(res) << std::endl;
            PQclear(res);

            throw PgQueryError();
        }
        return res;
    }


    /**
     * @brief Retreive an int32 column value from a query result
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return int32 value for row/col
     * @throws std::invalid_argument
     * @throws std::out_of_range
     */
    int32_t getInt32(PGresult *res, int row, int col)
    {
        char *value = PQgetvalue(res, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        int32_t r = std::stoi(value, nullptr, 10);
        return r;
    }

    /**
     * @brief Retreive a string column value from a query result; maps NULL to empty string
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return string value for row/col; null is mapped to empty string
     */
    std::string getString(PGresult *res, int row, int col)
    {
        char *value = PQgetvalue(res, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }

        return std::string(value);
    }

    /**
     * @brief Retreive a string column value from a query result; maintains NULL value
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return string value for row/col; optional is false if string is null
     */
    std::optional<std::string> getStringOptional(PGresult *res, int row, int col)
    {
        char *value = PQgetvalue(res, row, col);
        if (value == nullptr) {
            throw PgQueryError();
        }
        // PQgetvalue maps NULL to empty string, so need to explicitly check
        if (value[0] == '\0' && PQgetisnull(res, row, col)) {
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
    bool getBoolean(PGresult *res, int row, int col)
    {
        char *value = PQgetvalue(res, row, col);
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
     *
     * @param str string to escape
     * @return safe ptr to the string, access with .get()
     */
    std::unique_ptr<char[]> escapeString(const std::string &str)
    {
        std::unique_ptr<char[]> str_ptr(new char[str.length() * 2 + 1]);
        PQescapeString(str_ptr.get(), str.c_str(), str.length());

        return str_ptr;
    }

    /**
     * @brief escape a string; given a connection
     *        preferred when a connection is available
     *
     * @param connection uses connection encoding type
     * @param str string to escape
     * @return safe ptr to the string, access with .get()
     */
    std::unique_ptr<char[]> escapeString(const PGconn *connection,
                                         const std::string &str)
    {
        std::unique_ptr<char[]> str_ptr(new char[str.length() * 2 + 1]);
        PQescapeStringConn(const_cast<PGconn *>(connection), str_ptr.get(),
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
     * @return libpq connection
     */
    PGconn *connect(const std::string &db_host,
                    const std::string &db_name,
                    const std::string &db_user,
                    const std::string &db_pass,
                    const int db_port,
                    const bool replication)
    {
        // create key value list for: host, port, dbname, user, password, options
        // escape options
        std::unique_ptr<char[]> host = libpq::escapeString(db_host);
        std::unique_ptr<char[]> name = libpq::escapeString(db_name);
        std::unique_ptr<char[]> user = libpq::escapeString(db_user);
        std::unique_ptr<char[]> pass = libpq::escapeString(db_pass);

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
            PGresult *res = exec(connection, ALWAYS_SECURE_SEARCH_PATH_SQL);
            PQclear(res);
        } catch(PgQueryError &e) {
            // disconnect
            PQfinish(connection);
            throw e;
        }

        return connection;
    }

};
};