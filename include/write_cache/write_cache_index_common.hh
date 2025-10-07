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
        std::filesystem::path file_name; ///< if on disk, the file name
        uint64_t data_offset;            ///< if on disk, the offset within the file
        uint64_t xid;
        uint64_t xid_seq;
        ExtentPtr data; ///< if in memory, the extent data

        WriteCacheIndexExtent(uint64_t xid, uint64_t xid_seq, ExtentPtr data)
            : xid(xid), xid_seq(xid_seq), data{std::move(data)}
        {}
        WriteCacheIndexExtent(std::filesystem::path fn, uint64_t offset, uint64_t xid, uint64_t xid_seq)
            : file_name{std::move(fn)}, data_offset{offset}, xid(xid), xid_seq(xid_seq)
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
