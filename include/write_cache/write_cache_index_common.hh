#pragma once

#include <memory>
#include <string>

#include <fmt/core.h>

#include <storage/extent.hh>

namespace springtail {

    /**
     * @brief Row data structure; contains pkey, and data for a row
     */
    struct WriteCacheIndexExtent {
        uint64_t xid;
        uint64_t xid_seq;
        ExtentPtr data;

        WriteCacheIndexExtent(uint64_t xid, uint64_t xid_seq, const ExtentPtr &data)
            : xid(xid), xid_seq(xid_seq), data(data)
        {}
        WriteCacheIndexExtent(const WriteCacheIndexExtent&) = delete;
        WriteCacheIndexExtent(WriteCacheIndexExtent&&) = delete;
        const WriteCacheIndexExtent& operator=(const WriteCacheIndexExtent&) = delete;

        std::string dump() {
            return fmt::format("XID:{} XID_SEQ:{}", xid, xid_seq);
        }
    };
    typedef std::shared_ptr<WriteCacheIndexExtent> WriteCacheIndexExtentPtr;
} // namespace springtail
