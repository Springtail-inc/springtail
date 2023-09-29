#include <string>
#include <libpq-fe.h>

#include <psql_cdc/pg_types.hh>

namespace st_psql_cdc
{
    /**
     * @brief Output data structure used by read data call
     *        must be allocated and passed in
     */
    struct PgCopyData {
        const char *buffer;
        int length;
    };

    /**
     * @brief Postgres connection class
     * @details Provides interfaces for setting up replication
     *          connection and for streaming replication data
     */
    class PgReplConnection
    {
    private:
        // timeout between keep alive messages
        static const int64_t STANDBY_MSG_INTERVAL_MSEC = 30000L;
        // timeout for an idle slot -- no lsn received; fast forward stream
        static const int64_t IDLE_SLOT_TIMEOUT_MSEC = 300000L;
        // read timeout for copy data
        static const int     READ_TIMEOUT_SEC = 10;

        // Message constants
        static const char MSG_STANDBY_STATUS = 'r';
        static const char MSG_KEEP_ALIVE = 'k';
        static const char MSG_XLOG_DATA = 'w';

        int _db_port;
        std::string _db_host;
        std::string _db_name;
        std::string _db_user;
        std::string _db_pass;
        std::string _pub_name;
        std::string _slot_name;
        std::string _export_name;

        // remote server version
        int _server_version;

        // remote protocol version; only support 1 for now
        int _proto_version;

        bool _started_streaming = false;

        PGconn *_connection = nullptr;

        // buffer allocated for copy data
        // will be released on subsequent read calls
        char *_copy_buffer = nullptr;
        int _copy_buffer_offset = 0;
        int _copy_buffer_length = 0;

        // last flushed lsn
        LSN_t _last_flushed_lsn = INVALID_LSN;
        // last received lsn from data copy (from wal_start)
        LSN_t _last_received_lsn = INVALID_LSN;
        // servers latest lsn (from wal_end)
        LSN_t _server_latest_lsn = INVALID_LSN;

        // last time copy data received
        int64_t _last_received_time;
        // last time status was sent
        int64_t _last_status_time;
        // last time data was flushed
        int64_t _last_flushed_time;

        /**
         * @brief Internal helper to call PQexec
         *
         * @param cmd command to execute
         * @return 0 on success -1 otherwise
         */
        int pgExec(std::string cmd);

        int sendStandbyStatusMsg();

        void fastForwardStream();

        // return 1 if data, 0 if no data, -1 error
        int checkDataStream(int timeout_secs);

        int processXlogHeader(const char *buffer, int length);

        int processKeepAlive(const char *buffer, int length);


        static int encodeStandbyStatusMsg(LSN_t last_received_lsn,
                                          LSN_t last_flushed_lsn,
                                          int64_t send_time,
                                          char replybuf[34]);

    public:

        /**
         * @brief Constructor -- does not connect to db
         *
         * @param db_port DB port
         * @param db_host DB hostname
         * @param db_name DB name
         * @param db_user DB user name
         * @param db_pass DB user password
         * @param pub_name Publication name
         * @param slot_name Replication slot name
         */
        PgReplConnection(int db_port,
                         const std::string &db_host,
                         const std::string &db_name,
                         const std::string &db_user,
                         const std::string &db_pass,
                         const std::string &pub_name,
                         const std::string &slot_name);

        /**
         * @brief Destructor -- closed connection if open
         */
        ~PgReplConnection();

        /**
         * @brief Connect to db
         * @return 0 on success
         */
        int connect();

        /**
         * @brief Close db connection
         */
        void close();

        /**
         * @brief Start streaming
         *
         * @param LSN start at specified LSN, or latest if 0 (INVALID_LST)
         * @return 0 on success
         */
        int startStreaming(LSN_t LSN);

        /**
         * @brief Stop streaming
         * @return 0 on success
         */
        int endStreaming();

        /**
         * @brief Check if the slot exists on server
         * @return true if exists; false otherwise
         */
        bool checkSlotExists();

        /**
         * @brief Create the replication slot
         *
         * @param export_snapshot export the snapshot
         * @param temporary temporary slot; per session
         * @return 0 on success
         */
        int createReplicationSlot(bool export_snapshot,
                                  bool temporary);

        /**
         * @brief Drop the replication slot from the server
         * @return 0 on success
         */
        int dropReplicationSlot();

        /**
         * @brief Read WAL data from server; blocks
         *
         * @param dataOut Output data, must be preallocated
         * @return number of bytes read; 0 on EOF, <0 on error
         */
        int readData(PgCopyData &dataOut);

        /**
         * @brief Sets the flushed LSN for ack to server (not required)
         *
         * @param lsn LSN to set as latest flushed
         */
        void setLastFlushedLSN(LSN_t lsn);
    };
}