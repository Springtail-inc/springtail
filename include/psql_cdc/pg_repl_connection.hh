#pragma once

#include <string>
#include <memory>

#include <psql_cdc/pg_types.hh>
#include <psql_cdc/exception.hh>
#include <psql_cdc/libpq_connection.hh>

namespace springtail
{
    /**
     * @brief Output data structure used by read data call
     *        must be allocated and passed in
     */
    struct PgCopyData {
        const char *buffer;  // buffer containing data
        int length;          // length of data in this buffer
        int msg_length;      // length of message; may be larger than buffer
        int msg_offset;      // offset of message for start of buffer
        LSN_t starting_lsn;  // starting LSN for message buffer
    };

    /**
     * @brief Postgres connection class
     * @details Provides interfaces for setting up replication
     *          connection and for streaming replication data.
     *          Creates to libpq connections, 1 for queries, 1 for streaming
     *          issuing queries while streaming is active will end streaming
     */
    class PgReplConnection
    {
    private:
        /** Enumerates the state of the copy */
        enum CopyState {
            NEW_MSG,            // must read in a new header; start of message
            READ_COPY_HEADER,   // finished reading in the header, need to read msg
            STREAMING           // read msg, and got xlog data
        };

        /** timeout between keep alive messages */
        static inline constexpr int64_t STANDBY_MSG_INTERVAL_MSEC = 30000L;
        /** timeout for an idle slot -- no lsn received; fast forward stream */
        static inline constexpr int64_t IDLE_SLOT_TIMEOUT_MSEC = 300000L;
        /** read timeout for copy data */
        static inline constexpr int     READ_TIMEOUT_SEC = 10;
        /** postgres 14 version constant */
        static inline constexpr int     PG_VERS_14 = 140000;
        /** copy buffer size */
        static inline constexpr int     COPY_BUFFER_SIZE = 65536;
        /** copy buffer header size: 1 byte op, 4 bytes length */
        static inline constexpr int     COPY_MSG_HDR_SIZE = 5;
        /** Length of standby message */
        static inline constexpr int     STANDBY_MSG_SIZE = (1 + 8 + 8 + 8 + 8 + 1);

        // Message constants
        /** Standby message identifier */
        static inline constexpr char MSG_STANDBY_STATUS = 'r';
        /** Keep alive message identifier */
        static inline constexpr char MSG_KEEP_ALIVE = 'k';
        /** XLOG data message identifier */
        static inline constexpr char MSG_XLOG_DATA = 'w';
        /** Query message */
        static inline constexpr char MSG_QUERY = 'Q';
        /** Copy data message identifier */
        static inline constexpr char MSG_COPY_DATA = 'd';
        /** Copy done message identifier */
        static inline constexpr char MSG_COPY_DONE = 'c';
        /** Notification response message identifier */
        static inline constexpr char MSG_NOTIFICATION_RESPONSE = 'A';
        /** Notice response message identifier */
        static inline constexpr char MSG_NOTICE_RESPONSE = 'N';
        /** Paramater status (changed) message identifier */
        static inline constexpr char MSG_PARAM_STATUS = 'S';
        /** Error response message identifier */
        static inline constexpr char MSG_ERROR_RESPONSE = 'E';
        /** Copy both message identifier */
        static inline constexpr char MSG_COPY_BOTH = 'W';

        // connection parameters
        int _db_port;
        std::string _db_host;
        std::string _db_name;
        std::string _db_user;
        std::string _db_pass;
        std::string _pub_name;
        std::string _slot_name;
        std::string _export_name;

        /** remote server version */
        int _server_version = -1;

        /** remote pgoutput protocol version; only support 1, 2 for now */
        int _proto_version = -1;

        bool _started_streaming = false;

        /** replication (copy data) streaming connection */
        std::unique_ptr<LibPqConnection> _stream_connection;

        /** query connection; issuing a query on stream conn will stop the copy */
        std::unique_ptr<LibPqConnection> _connection;

        /** streaming socket */
        int _streaming_socket;

        /** buffer allocated for copy data */
        char _copy_buffer[COPY_BUFFER_SIZE];
        int _copy_buffer_length = 0;
        int _copy_buffer_offset = 0;
        int _copy_msg_length = 0;
        int _copy_msg_offset = 0;
        char _msg_type;

        /** simple state machine for where we are in reading in copy data */
        CopyState _copy_state = NEW_MSG;

        /** last flushed lsn */
        LSN_t _last_flushed_lsn = INVALID_LSN;
        /** last received lsn from data copy (from wal_start) */
        LSN_t _last_received_lsn = INVALID_LSN;
        /** servers latest lsn (from wal_end) */
        LSN_t _server_latest_lsn = INVALID_LSN;

        /** last time copy data received */
        int64_t _last_received_time;
        /** last time status was sent */
        int64_t _last_status_time;
        /** last time data was flushed */
        int64_t _last_flushed_time;

        void pgExec(PGconn *connection, std::string cmd);

        void sendStandbyStatusMsg();

        void fastForwardStream();

        // return true if data, false otherwise
        bool checkDataStream(int timeout_secs);

        int processXlogHeader(const char *buffer, int length);

        int processKeepAlive(const char *buffer, int length);

        void readMsgHeader();

        void readMsgData(bool async);

        void readCopyHeader();

        void readCopyData();

        void sendCopyData(const char *buffer, int length, char cmd);

        int recvCopyData(char *buffer, int length, bool async);

        bool handleTimeout();

        void skipMessage();

        void dumpErrorResponse();

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
         */
        void connect();

        /**
         * @brief Close db connection
         */
        void close();

        /**
         * @brief Start streaming
         *
         * @param LSN start at specified LSN, or latest if 0 (INVALID_LST)
         * @throws PgStreamingError if connection is already streaming
         * @throws PgQueryError if replication command failed
         */
        void startStreaming(LSN_t LSN);

        /**
         * @brief Stop streaming; close streaming connection
         * @throws PqQueryError if end streaming command failed
         */
        void endStreaming();

        /**
         * @brief Check if the slot exists on the server
         * @return true if slot exists; false otherwise
         * @throws PgQueryError on error
         */
        bool checkSlotExists();

        /**
         * @brief Check if the slot exists on the server
         *
         * @param restart_lsn_out output param: restart LSN
         * @param flushed_lsn_out output param: last flushed LSN
         *
         * @return true if slot exists, false otherwise
         * @throws PgQueryError on error
         */
        bool checkSlotExists(LSN_t &restart_lsn_out,
                             LSN_t &flushed_lsn_out);


        /**
         * @brief Create the replication slot
         *
         * @param export_snapshot export the snapshot
         * @param temporary temporary slot; per session
         * @throws PgStreamingError if streaming already started
         * @throws PgQueryError on query error
         */
        void createReplicationSlot(bool export_snapshot,
                                   bool temporary);

        /**
         * @brief Drop the replication slot from the server
         * @throws PgQueryError on error
         * @throws PgStreamingError if already streaming
         */
        void dropReplicationSlot();

        /**
         * @brief Read WAL data from server; blocks
         *
         * @param dataOut Output data, must be preallocated
         * @throws PgIOError on receive error
         * @throws PgNotConnectedError if connection has closed
         * @throws PgNotStreamingError if connection is not streaming
         */
        void readData(PgCopyData &dataOut);

        /**
         * @brief Sets the flushed LSN for ack to server (not required)
         *
         * @param lsn LSN to set as latest flushed
         */
        void setLastFlushedLSN(LSN_t lsn) noexcept;

        /**
         * @brief Get server version
         * @return get remote server version; -1 if not set
         */
        int getServerVersion() noexcept;

        /**
         * @brief Get pgoutput protocol version
         * @return pgoutput protocol version (1, 2, 3, 4) -- usually 2; -1 if not set
         */
        int getProtocolVersion() noexcept;
    };
}