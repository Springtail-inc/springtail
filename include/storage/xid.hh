#pragma once

#include <common/constants.hh>

namespace springtail {
    /**
     * Structure to hold a XID/LSN pair.
     */
    struct XidLsn {
    public:
        uint64_t xid;
        uint64_t lsn;

        XidLsn()
            : xid(0), lsn(0)
        { }

        explicit XidLsn(uint64_t xid)
            : xid(xid), lsn(constant::MAX_LSN)
        { }

        XidLsn(uint64_t xid, uint64_t lsn)
            : xid(xid), lsn(lsn)
        { }

        bool operator==(const XidLsn &rhs) const
        {
            return (xid == rhs.xid && lsn == rhs.lsn);
        }

        std::strong_ordering operator<=>(const XidLsn &rhs) const
        {
            return std::tie(xid, lsn) <=> std::tie(rhs.xid, rhs.lsn);
        }
    };

    struct XidRange {
    public:
        XidLsn start;
        XidLsn end;

        XidRange() = default;
        XidRange(const XidLsn &start, const XidLsn &end)
            : start(start), end(end)
        { }

        /** Checks if the provided XID is within the range of [start, end) */
        bool contains(const XidLsn &xid) const {
            return (start <= xid && xid < end);
        }

        // ordering is based on the end of the range
        std::strong_ordering operator<=>(const XidRange &rhs) const
        {
            return std::tie(end, start) <=> std::tie(rhs.end, rhs.start);
        }
    };
}
