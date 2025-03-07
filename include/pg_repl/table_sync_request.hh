#pragma once

namespace springtail {

/**
 * Serializer for storing a table resync request into Redis.
 */
class TableSyncRequest {
public:
    TableSyncRequest(uint32_t table_id, const XidLsn &xid) : _table_id(table_id), _xid(xid) {}

    explicit TableSyncRequest(std::string_view serialized) {
        std::vector<std::string> split;
        common::split_string(":", serialized, split);
        CHECK_EQ(split.size(), 3);

        _table_id = std::stoul(split[0]);
        _xid = XidLsn(std::stoull(split[1]), std::stoull(split[2]));
    }

    explicit operator std::string() const {
        return fmt::format("{}:{}:{}", _table_id, _xid.xid, _xid.lsn);
    }

    uint32_t table_id() const {
        return _table_id;
    }

    const XidLsn &xid() const {
        return _xid;
    }

private:
    uint32_t _table_id;
    XidLsn _xid;
};
using TableSyncRequestPtr = std::shared_ptr<TableSyncRequest>;

}
