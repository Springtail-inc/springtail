#pragma once

#include <string>
#include <limits>
#include <cstdint>

namespace springtail::constant {
    static constexpr uint64_t UNKNOWN_EXTENT = std::numeric_limits<uint64_t>::max();
    static constexpr uint64_t LATEST_XID = std::numeric_limits<uint64_t>::max();
    static constexpr uint64_t MAX_LSN = std::numeric_limits<uint64_t>::max();
    static constexpr uint64_t MAX_EXTENT_SIZE = 64 * 1024;

    const static std::string BTREE_CHILD_FIELD = "__springtail_child";
}
