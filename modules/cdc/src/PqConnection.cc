#include <iostream>

#include "PgConnection.hh"

#include <replication/logicalproto.h>

#include <sys/select.h>


PqConnection::PqConnection(const std::string& db_host,
                           const std::string& db_port,
                           const std::string& db_name,
                           const std::string& db_user,
                           const std::string& db_pass,
                           const std::string& slot_name,
                           const std::string& sub_name,
                           const std::string& pub_name)
{
    _db_host = db_host;
    _db_port = db_port;
    _db_name = db_name;
    _db_user = db_user;
    _db_pass = db_pass;
    _slot_name = slot_name;
    _sub_name = sub_name;
    _pub_name = pub_name;
}

/**
 * @brief Connect to endpoint specified by params passed to constructor
 * for subscription info:
 * https://www.postgresql.org/docs/current/logical-replication-subscription.html
 * may be able to first check the publications and get the list of tables
 * https://github.com/postgres/postgres/blob/18724af9e83bb9123e0e1cd09751aef4ba30039e/src/backend/commands/subscriptioncmds.c#L740
 */
void PqConnection::connect()
{
    // error string
    char *error;

    // format postgresql://other@localhost:port/otherdb
    std::string conninfo = "postgresql://" + _db_user + "@" + _db_host + ":" + _db_port + "/" + _db_name;

    _connection = walrcv_connect(conninfo.c_str(),   // connection string
                                 true,               // logical replication
                                 true,               // must use password
                                 _sub_name.c_str(),  // subscription name
                                 &error);            // error string

    if (_connection == NULL) {
        std::cout << "Failed to connect to database: " << conninfo << "\nError: " << error;
    }

    /*
     * Saw following comment, not sure if still valid?
     * We don't really use the output identify_system for anything but it
     * does some initializations on the upstream so let's still call it.
     */
    TimeLineID tli;
    (void) walrcv_identify_system(_connection, &tli);
}

/**
 * @brief Get server version
 *
 * @return server version
 */
int PqConnection::serverVersion()
{
    return walrcv_server_version(_connection);
}

/**
 * @brief Start streaming
 *
 * @param uint64, LSN starting LSN, use 0 for current LSN
 */
bool PqConnection::startStreaming(XLogRecPtr LSN)
{
    WalRcvStreamOptions options;

    int server_version = serverVersion();

    options.logical = true;
    options.slotname = (char *)_slot_name.c_str();
    options.startpoint = 0;

    options.proto.logical.proto_version =
        server_version >= 160000 ? 4 : // LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
        server_version >= 150000 ? LOGICALREP_PROTO_TWOPHASE_VERSION_NUM :
        server_version >= 140000 ? LOGICALREP_PROTO_STREAM_VERSION_NUM :
            LOGICALREP_PROTO_VERSION_NUM;

    options.proto.logical.twophase = false;
    options.proto.logical.binary = true;

    if (server_version >= 160000) {
        options.proto.logical.streaming_str = (char *)"parallel";
    } else if (server_version >= 140000) {
        options.proto.logical.streaming_str = (char *)"on";
    } else {
        options.proto.logical.streaming_str = NULL;
    }

    // setup list -- annoying
    List pub_names;
    ListCell cell;
    pub_names.elements = pub_names.initial_elements;
    pub_names.length = 1;
    pub_names.max_length = 1;
    cell.ptr_value = (void *)_pub_name.c_str();

    options.proto.logical.publication_names = &pub_names;

    bool result = false;
    PG_TRY();
    {
        result = walrcv_startstreaming(_connection, &options);
        if (!result) {
            std::cout << "Failed to start streaming.";
        } else {
            _started_streaming = true;
        }
    } PG_CATCH();
    {
        std::cerr << "Caught error starting streaming";
    } PG_END_TRY();

    return result;
}

/**
 * @brief Stop streaming
 */
void PqConnection::endStreaming()
{
    TimeLineID next_tli;
    if (_started_streaming) {
        walrcv_endstreaming(_connection, &next_tli);
    }
}

/**
 * @brief Receive data without waiting
 *
 * @param buffer pointer to buffer is returned
 * @return length of data received; -1 on close
 */
int PqConnection::receiveNoBlock(char **buffer)
{
    pgsocket wait_fd;
    return walrcv_receive(_connection, buffer, &wait_fd);
}

/**
 * @brief Receive data while blocking
 * @details [long description]
 *
 * @param buffer pointer to buffer is returned
 * @param timeout_secs timeout in seconds
 *
 * @return length of data received; -1 on close; 0 on timeout
 */
int PqConnection::receiveBlock(char **buffer, long timeout_secs)
{
    pgsocket wait_fd;
    int res = 0;

    do {
        res = walrcv_receive(_connection, buffer, &wait_fd);
        if (res == 0) {
            fd_set fdread;
            struct timeval tv;
            tv.tv_usec = 0;
            tv.tv_sec = timeout_secs;

            FD_ZERO(&fdread);
            FD_SET(wait_fd, &fdread);
            int status = select(wait_fd+1, &fdread, NULL, NULL, &tv);
            if (status == 0) {
                // timeout
                return 0;
            } else if (status == -1) {
                // error break out of loop
                return -1;
            }
        }
    } while (res == 0);

    return res;
}

/**
 * @brief Close the connection, stop streaming if started
 */
void PqConnection::close()
{
    if (_connection != NULL) {
        //endStreaming();
        walrcv_disconnect(_connection);
    }
}

PqConnection::~PqConnection()
{
    close();
}


int main(void)
{
    try {
        PqConnection *pqconn = new PqConnection("localhost", "5432", "springtail", "springtail",
                                                "springtail", "repl_slot", "repl_sub", "repl_pub");
        pqconn->close();
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        return -1;
    }

    std::cout << "Connected successfully!";

    return 0;
}

