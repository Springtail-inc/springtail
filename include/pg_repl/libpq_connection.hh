#pragma once

#include <libpq-fe.h>
#include <optional>

namespace springtail {

    class LibPqConnection {

    public:

        static constexpr int MAX_RETRY_COUNT = 5;  ///< max retry count for connection
        static constexpr int RETRY_SLEEP_SECS = 2; ///< initial sleep time in sec between retries

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
        void start_transaction();

        /**
         * @brief End a transaction
         */
        void end_transaction();

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
        int32_t get_int32(int row, int col);

        /**
         * @brief Retreive an int32 column value from a query result; maintains NULL value
         *
         * @param row row index
         * @param col col index
         * @return int32 value for row/col; optional is false if int is null
         */
        std::optional<int32_t> get_int32_optional(int row, int col);

        /**
         * @brief Retreive an int64 column value from a query result
         *
         * @param row row index
         * @param col col index
         * @return int64 value for row/col
         */
        int64_t get_int64(int row, int col);

        /**
         * @brief Retreive a string column value from a query result; maintains NULL value
         *
         * @param row row index
         * @param col col index
         * @return string value for row/col; optional is false if string is null
         */
        std::optional<std::string> get_string_optional(int row, int col);


        /**
         * @brief Retreive a string column value from a query result; maps NULL to empty string
         *
         * @param row row index
         * @param col col index
         * @return string value for row/col; null is mapped to empty string
         */
        std::string get_string(int row, int col);

        /**
         * @brief Get boolean value from query result
         *
         * @param row row index
         * @param col col index
         * @return boolean value for row/col; null is mapped to false
         */
        bool get_boolean(int row, int col);

        /**
         * @brief escape a string; a literal not for identifiers
         *
         * @param str string to escape
         * @return string with escaped characters
         */
        std::string escape_string(const std::string &str);

        /**
         * @brief escape an identifier; not for literals
         *
         * @param str string to escape
         * @return string with escaped characters
         */
        std::string escape_identifier(const std::string &str);

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
                     const bool replication = false);

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
         * @brief is the connection using SSL
         */
        bool is_ssl();

        /**
         * @brief Wait for connection to be writeable
         * @param timeout_secs timeout in seconds; -1 for no timeout
         * @return int -1 on error, 0 on timeout, 1 on success
         */
        int wait_until_writable(int timeout_secs=-1);

        /**
         * @brief Wait for connection to be readable
         * @param timeout_secs timeout in seconds; -1 for no timeout
         * @return int -1 on error, 0 on timeout, 1 on success
         */
        int wait_until_readable(int timeout_secs=-1);

        /**
         * @brief Read from connection
         * @param buf buffer to read into
         * @param count number of bytes to read
         * @param async flag indicating async operation
         *              if not async will read all data or return an error
         * @return number of bytes read; -1 on error, errno set; 0 indicates no bytes ready
         */
        ssize_t read(char *buf, size_t count, bool async = false);

        /**
         * @brief Write to connection
         * @param buf buffer to write
         * @param count number of bytes to write
         * @param async flag indicating async operation;
         *              if not async will write all data or return an error
         * @return number of bytes written; -1 on error, errno set
         */
        ssize_t write(const char *buf, size_t count, bool async = false);

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

    private:
        /** Internal libpq connection */
        PGconn *_connection = nullptr;

        /** Result pointer from last query */
        PGresult *_result = nullptr;

        /** Indicates if connection is in a transaction or not */
        bool _in_transaction = false;

        /** Is the connection for replication */
        bool _is_replication = false;

        /** Copy buffer pointer */
        char *_buffer = nullptr;

        /** Simple version of escape identifier */
        static std::string _escape_identifier(const char *str);

        /** helper functions for reading and writing */
        static ssize_t _secure_read(PGconn *conn, void *ptr, size_t len);
        static ssize_t _secure_write(PGconn *conn, const void *ptr, size_t len);
        static ssize_t _ssl_read(PGconn *conn, void *ptr, size_t len);
        static ssize_t _non_ssl_read(PGconn *conn, void *ptr, size_t len);
        static ssize_t _ssl_write(PGconn *conn, const void *ptr, size_t len);
        static ssize_t _non_ssl_write(PGconn *conn, const void *ptr, size_t len);

    };
    using LibPqConnectionPtr = std::shared_ptr<LibPqConnection>;
};
