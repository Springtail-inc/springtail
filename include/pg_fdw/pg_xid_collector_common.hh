#pragma once

#include <stdint.h>

#include <string_view>

namespace springtail::pg_fdw {
    static constexpr std::string_view DEFAULT_SOCKET_NAME = "xid_collector_socket";

    struct __attribute__((packed)) PgXidCollectorMsg {
        uint64_t db_id;
        uint64_t xid;
    };

} // springtail::pg_fdw