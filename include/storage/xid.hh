#pragma once

#include <storage/constants.hh>

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
            if (xid == rhs.xid) {
                return lsn <=> rhs.lsn;
            }
            return xid <=> rhs.xid;
        }
    };
}
