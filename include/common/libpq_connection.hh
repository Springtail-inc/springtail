#pragma once

#include <libpq-fe.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace springtail {

    class LibPqConnection {

    public:
        static constexpr int MAX_RETRY_COUNT = 5;  ///< max retry count for connection
        static constexpr int RETRY_SLEEP_SECS = 2; ///< initial sleep time in sec between retries

        /* Some SQLSTATEs see: get_sql_state() */
        // Class 00 — Successful Completion
        static constexpr const char* SQL_SUCCESSFUL_COMPLETION     = "00000";

        // Class 01 — Warning
        static constexpr const char* SQL_WARNING                   = "01000";

        // Class 02 — No Data (e.g. UPDATE/DELETE affected 0 rows)
        static constexpr const char* SQL_NO_DATA                   = "02000";

        // Class 3F — Invalid Schema Name
        static constexpr const char* SQL_INVALID_SCHEMA_NAME       = "3F000";

        // Class 42 — Syntax or Access Rule Violation
        static constexpr const char* SQL_SYNTAX_ERROR              = "42601";
        static constexpr const char* SQL_UNDEFINED_OBJECT          = "42704"; // Used by role doesn't exist
        static constexpr const char* SQL_UNDEFINED_TABLE           = "42P01";
        static constexpr const char* SQL_UNDEFINED_COLUMN          = "42703";
        static constexpr const char* SQL_DUPLICATE_COLUMN          = "42701";
        static constexpr const char* SQL_DUPLICATE_OBJECT          = "42710"; // Used by role already exists
        static constexpr const char* SQL_DUPLICATE_TABLE           = "42P07";
        static constexpr const char* SQL_INSUFFICIENT_PRIVILEGE    = "42501";

        // RAII wrapper around PGresult to ensure it is cleared
        // when the LibPqResult object is destroyed
        struct LibPqResult {
            PGresult *result = nullptr;

            explicit LibPqResult(PGresult *res) : result(res) {}
            LibPqResult(const LibPqResult &) = delete; // prevent copying
            LibPqResult &operator=(const LibPqResult &) = delete; // prevent assignment
            LibPqResult(LibPqResult &&other) noexcept : result(other.result) {
                other.result = nullptr; // transfer ownership
            }

            ~LibPqResult() {
                if (result != nullptr) {
                    PQclear(result);
                    result = nullptr;
                }
            }
        };
        using LibPqResultPtr = std::shared_ptr<LibPqResult>;

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
         * @return std::string of error message from underlying connection; should not be freed; empty if no error
         */
        std::string error_message();

        /**
         * @brief Get error message from last query result
         * @return std::string of error message from last query result; if none then return connection error_message()
         *         empty if no error
         */
        std::string result_error_message();

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
         * @brief Rollback transaction
         */
        void rollback_transaction();

        /**
         * @brief Set a savepoint in the current transaction
         * @param name name of the savepoint
         */
        void savepoint(const std::string &name);

        /**
         * @brief Release a savepoint in the current transaction
         * @param name name of the savepoint
         */
        void release_savepoint(const std::string &name);

        /**
         * @brief Rollback to a savepoint in the current transaction
         * @param name name of the savepoint
         */
        void rollback_savepoint(const std::string &name);

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         * @return true if result is successful; false otherwise; check result separately
         */
        bool exec_no_throw(const std::string &cmd, bool use_savepoint = true);

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         * @return true if result is successful; false otherwise; check result separately
         */
        bool exec_no_throw(const char *cmd, bool use_savepoint = true);

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         * @throws PgQueryError if result is not successful
         * @throws PgNotConnectedError if not connected
         */
        void exec(const std::string &cmd);

        /**
         * @brief Execute libpq query helper; sets result internally
         * @param connection libpq connection
         * @param cmd SQL command
         * @throws PgQueryError if result is not successful
         * @throws PgNotConnectedError if not connected
         */
        void exec(const char *cmd);

        /**
         * @brief Get sql state from last query result
         * @return sql state string; "00000" for success; see constants above for common values
         *         see https://www.postgresql.org/docs/current/errcodes-appendix.html for all values
         */
        std::optional<std::string> get_sql_state();

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
         * @brief Retreive a float column value from a query result
         *
         * @param row row index
         * @param col col index
         * @return float value for row/col
         */
        float get_float(int row, int col);

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
         * @brief Get char value from query result
         *
         * @param row row index
         * @param col col index
         * @return char value for row/col; null is mapped to '\0'
         */
        char get_char(int row, int col);

        /**
         * @brief escape a string; a literal not for identifiers
         *
         * @param str string to escape
         * @return string with escaped characters; does NOT include ''
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
         * @param options extra options that can be passed to postgres connection
         * @return libpq connection
         */
        void connect(const std::string &db_host,
                     const std::string &db_name,
                     const std::string &db_user,
                     const std::string &db_pass,
                     const int db_port,
                     const bool replication = false,
                     const std::optional<std::vector<std::pair<std::string, std::string>>> &options = std::nullopt);

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

        /** Result pointer from last query; RAII wrapper */
        LibPqResultPtr _result = nullptr;

        /** Indicates if connection is in a transaction or not */
        bool _in_transaction = false;

        /** Is the connection for replication */
        bool _is_replication = false;

        /** Copy buffer pointer */
        char *_buffer = nullptr;

        /** Counter for savepoints generated internally */
        uint64_t _savepoint_ctr = 0;

        /** Simple version of escape identifier */
        static std::string _escape_identifier(const char *str);

        /** helper functions for reading and writing */
        static ssize_t _secure_read(PGconn *conn, char *ptr, size_t len);
        static ssize_t _secure_write(PGconn *conn, const char *ptr, size_t len);
        static ssize_t _ssl_read(PGconn *conn, char *ptr, size_t len);
        static ssize_t _non_ssl_read(PGconn *conn, char *ptr, size_t len);
        static ssize_t _ssl_write(PGconn *conn, const char *ptr, size_t len);
        static ssize_t _non_ssl_write(PGconn *conn, const char *ptr, size_t len);

    };
    using LibPqConnectionPtr = std::shared_ptr<LibPqConnection>;
};
