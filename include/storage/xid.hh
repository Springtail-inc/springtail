#pragma once

namespace springtail {
    /**
     * Structure to hold a XID/LSN pair.
     */
    struct XidLsn {
        uint64_t xid;
        uint64_t lsn;

        explicit XidLsn(uint64_t xid)
            : xid(xid), lsn(constant::MAX_LSN)
        { }

        XidLsn(uint64_t xid, uint64_t lsn)
            : xid(xid), lsn(lsn)
        { }

        bool operator<(const XidLsn &rhs)
        {
            return (xid < rhs.xid || (xid == rhs.xid && lsn < rhs.lsn));
        }

        bool operator==(const XidLsn &rhs)
        {
            return (xid == rhs.xid && lsn == rhs.lsn);
        }
    };
}
