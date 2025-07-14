#pragma once

#include <sys/un.h>
#include <unistd.h>

#include <string>

#include <pg_fdw/pg_xid_collector_common.hh>

namespace springtail::pg_fdw {

    class PgXidCollectorClient
    {
    public:
        PgXidCollectorClient();
        ~PgXidCollectorClient();

        void send_data(uint64_t db_id, uint64_t xid);
    private:
        std::string _fdw_id;
        std::string _socket_name;
        struct sockaddr_un _addr{};
        socklen_t _addrlen{0};
        PgXidCollectorMsg _msg{};
        int _fd{-1};
    };
} // springtail::pg_fdw