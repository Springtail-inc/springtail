#pragma once

#include <libpq-fe.h>
#include <optional>

namespace springtail {

    class LibPqConnection {

    private:
        /** Internal libpq connection */
        PGconn *_connection = nullptr;

        /** Result pointer from last query */
        PGresult *_result = nullptr;

        /** Indicates if connection is in a transaction or not */
        bool _in_transaction = false;

        /** Copy buffer pointer */
        char *_buffer = nullptr;

    public:

        LibPqConnection() {};

        ~LibPqConnection() {
            if (_connection != nullptr) {
                disconnect();
            }
        }

        /**
         * @brief Is the class connected
         * @return true if connected; false otherwise
         */
        bool is_connected();

        /**
         * @brief Get number of tuples from last query result
         * @return number of tuples from last query or 0.
         */
        int ntuples();

        /**
         * @brief Get number of binary tuples from last query result
         * @return number of binary tuples from last query or 0.
         */
        int binary_tuples();

        /**
         * @brief Get number of fields from last query result
         * @return number of fields from last query or 0.
         */
        int nfields();

        /**
         * @brief Force a fetch (update) of the internal result; usually not necessary to call
         *        as command will fetch the result
         * @return true if result is not null
         */
        bool fetch_result();

        /**
         * @brief Get raw value from result; use with care, freed by clear()
         * @param row row index
         * @param col column index
         * @return pointer based on result value
         */
        char *get_value(int row, int col);

        /**
         * @brief Get result status code from last query result
         * @return result status code; e.g., PGRES_COPY_OUT, PGRES_COMMAND_OK, etc.
         */
        ExecStatusType status();

        /**
         * @brief Clear the result if one exists
         */
        void clear();

        /**
         * @brief Get error message from connection; should not be freed
         * @return pointer to error message from underlying connection; should not be freed
         */
        char *error_message();

        /**
         * @brief Get copy data; uses internal buffer
         * @param async  flag indicating async operation
         */
        int get_copy_data(bool async);

        /**
         * @brief Get pointer to internal copy buffer
         * @return internal copy buffer; should be freed by free_copy_data()
         */
        char *get_copy_buffer();

        /**
         * @brief Free copy data from get_copy_data() if not already freed
         */
        void free_copy_buffer();

        /**
         * @brief Start a transaction
         */
        void startTransaction();

        /**
         * @brief End a transaction
         */
        void endTransaction();

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         */
        void exec(const std::string &cmd);

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         */
        void exec(const char *cmd);

        /**
         * @brief Retreive an int32 column value from a query result
         *
         * @param row row index
         * @param col col index
         * @return int32 value for row/col
         */
        int32_t getInt32(int row, int col);

        /**
         * @brief Retreive a string column value from a query result; maintains NULL value
         *
         * @param row row index
         * @param col col index
         * @return string value for row/col; optional is false if string is null
         */
        std::optional<std::string> getStringOptional(int row, int col);


        /**
         * @brief Retreive a string column value from a query result; maps NULL to empty string
         *
         * @param row row index
         * @param col col index
         * @return string value for row/col; null is mapped to empty string
         */
        std::string getString(int row, int col);

        /**
         * @brief Get boolean value from query result
         *
         * @param row row index
         * @param col col index
         * @return boolean value for row/col; null is mapped to false
         */
        bool getBoolean(int row, int col);

        /**
         * @brief escape a string
         *
         * @param str string to escape
         * @return safe ptr to the string, access with .get()
         */
        std::unique_ptr<char[]> escapeString(const std::string &str);

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
        void connect(const std::string &db_host,
                     const std::string &db_name,
                     const std::string &db_user,
                     const std::string &db_pass,
                     const int db_port,
                     const bool replication);

        /**
         * @brief Disconnect connection, ignore errors
         */
        void disconnect();

        /**
         * @brief Get underlying socket from connection; use with care
         * @return socket fd
         */
        int socket();

        /**
         * @brief Flush connection
         * @return 1 unable to send all data -- wait for ready; -1 on failure; 0 success
         */
        int flush();

        /**
         * @brief Get length of field value in bytes
         *
         * @param row row index
         * @param col column index
         *
         * @return length of tuple
         */
        int length(int row, int col);

        /**
         * @brief Get remote server version
         * @return remote server version; e.g., 11.0 = 110000; or 0
         */
        int server_version();

        /**
         * @brief Send end-of-data indicator to the server during copy.
         * @param errormsg if errormsg is not null, then copy will fail with error message
         * @return result of 1 if message was sent; call result() to get final result
         */
        int put_copy_end(const char *errormsg);

        /**
         * @brief Waits until server has finished the copying
         * @return 0 on success, nonzero otherwise, use error_message() to see error.
         */
        int end_copy();

    };
};