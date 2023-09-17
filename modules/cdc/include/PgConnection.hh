#pragma once

#include <string>

#import <postgres.h>
#include <replication/walreceiver.h>

class PqConnection
{
private:
    std::string _db_host;
    std::string _db_port;
    std::string _db_name;
    std::string _db_user;
    std::string _db_pass;
    std::string _slot_name;
    std::string _sub_name;
    std::string _pub_name;

    bool _started_streaming = false;

    WalReceiverConn *_connection = NULL;
public:
    PqConnection(const std::string& db_host,
                 const std::string& db_port,
                 const std::string& db_name,
                 const std::string& db_user,
                 const std::string& db_pass,
                 const std::string& slot_name,
                 const std::string& sub_name,
                 const std::string& pub_name);
    ~PqConnection();

    void connect();
    void close();

    int serverVersion();

    bool startStreaming(XLogRecPtr LSN);
    void endStreaming();

    int receiveNoBlock(char **buffer);
    int receiveBlock(char **buffer, long timeout_secs);
};