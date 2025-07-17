#include <common/init.hh>
#include <common/json.hh>

#include <pg_fdw/pg_xid_collector_client.hh>

namespace springtail::pg_fdw {
    PgXidCollectorClient::PgXidCollectorClient()
    {
        _fdw_id = Properties::get_fdw_id();
        nlohmann::json fdw_config;
        fdw_config = Properties::get_fdw_config(_fdw_id);
        _socket_name = Json::get_or<std::string_view>(fdw_config, "collector_socket", DEFAULT_SOCKET_NAME);

        _fd = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        PCHECK(_fd != -1) << "Failed to create UNIX domain socket";

        _addr.sun_family = AF_UNIX;

        // abstract unix domain socket indicator
        _addr.sun_path[0] = '\0';
        memcpy(_addr.sun_path + 1, _socket_name.c_str(), _socket_name.size());
        _addrlen = offsetof(struct sockaddr_un, sun_path) + 1 + _socket_name.size();
    }

    void
    PgXidCollectorClient::send_data(uint64_t db_id, uint64_t xid)
    {
        pid_t process_id = getpid();
        PgXidCollectorMsg msg = {db_id, xid};
        ssize_t rc = ::sendto(_fd, (void *)&msg, sizeof(msg), 0, (struct sockaddr *)&_addr, _addrlen);
        if (rc < 0) {
            PCHECK(errno == ECONNREFUSED) << "Failed with unexpected error code";
            LOG_ERROR("Failed to send a message to the server: db_id = {}, xid = {}", db_id, xid);
        } else {
            CHECK(rc == sizeof(PgXidCollectorMsg)) << "Invalid number of bytes sent";
            LOG_DEBUG(LOG_FDW, "Sent a message to the server: db_id = {}, xid = {}, pid = {}", db_id, xid, process_id);
        }
    }

    PgXidCollectorClient::~PgXidCollectorClient()
    {
        ::close(_fd);
    }

} // springtail::pg_fdw