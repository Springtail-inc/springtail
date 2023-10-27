#pragma once

#include <libpq-fe.h>
#include <optional>

namespace springtail {

namespace libpq {

    /**
     * @brief Execute libpq query helper
     * @param connection libpq connection
     * @param cmd SQL command
     * @return result, must be freed with PQclear(res)
     */
    PGresult *exec(PGconn *connection, const std::string &cmd);

    /**
     * @brief Retreive an int32 column value from a query result
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return int32 value for row/col
     */
    int32_t getInt32(PGresult *res, int row, int col);

    /**
     * @brief Retreive an string column value from a query result; maintains NULL value
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return string value for row/col; optional is false if string is null
     */
    std::optional<std::string> getStringOptional(PGresult *res, int row, int col);


    /**
     * @brief Retreive an string column value from a query result; maps NULL to empty string
     *
     * @param res query result
     * @param row row index
     * @param col col index
     * @return string value for row/col; null is mapped to empty string
     */
    std::string getString(PGresult *res, int row, int col);

    /**
     * @brief escape a string
     *
     * @param str string to escape
     * @return safe ptr to the string, access with .get()
     */
    std::unique_ptr<char[]> escapeString(const std::string &str);

    /**
     * @brief escape a string; given a connection
     *        preferred when a connection is available
     *
     * @param connection uses connection encoding type
     * @param str string to escape
     * @return safe ptr to the string, access with .get()
     */
    std::unique_ptr<char[]> escapeString(const PGconn *connection,
                                         const std::string &str);
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
                    const bool replication);
};
};