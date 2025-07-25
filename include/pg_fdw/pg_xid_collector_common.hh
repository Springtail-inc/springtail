#pragma once

#include <stdint.h>

#include <string_view>

namespace springtail::pg_fdw {
    static constexpr std::string_view DEFAULT_SOCKET_NAME = "xid_collector_socket";

    /**
     * @brief Data structure that represents message format sent by FDW process
     *
     */
    struct __attribute__((packed)) PgXidCollectorMsg {
        uint64_t db_id;     ///< database id
        uint64_t xid;       ///< transaction id
    };

} // springtail::pg_fdw