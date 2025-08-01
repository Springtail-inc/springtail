#pragma once

#include <sys/un.h>
#include <unistd.h>

#include <string>

#include <pg_fdw/pg_xid_collector_common.hh>

namespace springtail::pg_fdw {

    /**
     * @brief This is an XID collector client class that is used to send updates to
     *          the XID collector service.
     *
     */
    class PgXidCollectorClient
    {
    public:
        /**
         * @brief Construct a new Pg Xid Collector Client object
         *
         */
        PgXidCollectorClient();

        /**
         * @brief Destroy the Pg Xid Collector Client object
         *
         */
        ~PgXidCollectorClient();

        /**
         * @brief This function sends data to the XID collector service
         *
         * @param db_id - database id
         * @param xid - transaction id
         */
        void send_data(uint64_t db_id, uint64_t xid);

        /**
         * @brief Initialize client by entering its process id into redis
         *
         * @param db_id - database id
         */
        void init(uint64_t db_id);
    private:
        std::string _fdw_id;        ///< FDW id
        std::string _socket_name;   ///< name of the abstract UNIX domain socket
        struct sockaddr_un _addr{}; ///< socket address
        socklen_t _addrlen{0};      ///< address length
        pid_t _process_id;          ///< id of the client process
        int _fd{-1};                ///< client file descriptor
    };
} // springtail::pg_fdw