#pragma once

#include <absl/log/check.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <cstdint>
#include <memory>
#include <set>
#include <string_view>
#include <vector>

#include <common/common.hh>
#include <storage/xid.hh>

namespace springtail {

/**
 * Serializer for storing a table resync request into Redis.
 */
class TableSyncRequest {
public:
    TableSyncRequest(const std::set<uint32_t> &table_ids, const XidLsn &xid) : _table_ids(table_ids), _xid(xid) {}

    explicit TableSyncRequest(std::string_view serialized) {
        std::vector<std::string> split;
        common::split_string(":", serialized, split);
        CHECK_EQ(split.size(), 3);

        std::vector<std::string> table_id_list;
        common::split_string(",", split[0], table_id_list);
        for (const auto &table_id : table_id_list)
        {
            if (!table_id.empty())
            {
                _table_ids.insert(static_cast<uint32_t>(std::stoul(table_id)));
            }
        }
        _xid = XidLsn(std::stoull(split[1]), std::stoull(split[2]));
    }

    explicit operator std::string() const {
        return fmt::format("{}:{}:{}", fmt::join(_table_ids, ","), _xid.xid, _xid.lsn);
    }

    const std::set<uint32_t> &table_ids() const {
        return _table_ids;
    }

    const XidLsn &xid() const {
        return _xid;
    }

private:
    std::set<uint32_t> _table_ids;
    XidLsn _xid;
};
using TableSyncRequestPtr = std::shared_ptr<TableSyncRequest>;

}
