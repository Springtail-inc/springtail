#pragma once

#include <memory>
#include <string>

#include <fmt/core.h>

namespace springtail {

    /**
     * @brief Row data structure; contains pkey, and data for a row
     */
    struct WriteCacheIndexRow {
        uint64_t eid;
        uint64_t xid;
        uint64_t xid_seq;
        std::string pkey;
        std::string data;

        enum RowOp : uint8_t{
            UPDATE=0,
            INSERT=1,
            DELETE=2
        } op;

        WriteCacheIndexRow(const std::string &&data, const std::string &&pkey,
                           uint64_t eid, uint64_t xid, uint64_t xid_seq, RowOp op)
            : eid(eid), xid(xid), xid_seq(xid_seq), pkey(pkey), data(data), op(op)
        {}

        WriteCacheIndexRow(const std::string &&pkey, uint64_t eid, uint64_t xid,
                           uint64_t xid_seq, RowOp op=RowOp::DELETE)
            : eid(eid), xid(xid), xid_seq(xid_seq), pkey(pkey), op(op)
        {}

        WriteCacheIndexRow(uint64_t eid, uint64_t xid)
            : eid(eid), xid(xid), xid_seq(0), pkey({})
        {}

        std::string dump() {
            return fmt::format("EID:{} XID:{} XID_SEQ:{} PKEY:{}", eid, xid, xid_seq, pkey);
        }
    };
    typedef std::shared_ptr<WriteCacheIndexRow> WriteCacheIndexRowPtr;

    /**
     * @brief Table change operation
     */
    struct WriteCacheIndexTableChange {
        uint64_t tid;
        uint64_t xid;
        uint64_t xid_seq;

        enum TableChangeOp : uint8_t {
            INVALID=0,
            TRUNCATE_TABLE = 1,
            SCHEMA_CHANGE = 2
        } op;

        struct Comparator {
            bool operator()(const std::shared_ptr<WriteCacheIndexTableChange> &lhs,
                            const std::shared_ptr<WriteCacheIndexTableChange> &rhs) const {
                if (lhs->tid < rhs->tid) { return true; }
                if (lhs->tid == rhs->tid && lhs->xid < rhs->xid) { return true; }
                if (lhs->tid == rhs->tid && lhs->xid == rhs->xid && lhs->xid_seq < rhs->xid_seq) { return true; }
                return false;
            }
        };

        WriteCacheIndexTableChange(uint64_t tid, uint64_t xid, uint64_t xid_seq, TableChangeOp op = TableChangeOp::INVALID)
            : tid(tid), xid(xid), xid_seq(xid_seq), op(op)
        {}
    };
    typedef std::shared_ptr<WriteCacheIndexTableChange> WriteCacheIndexTableChangePtr;

} // namespace springtail