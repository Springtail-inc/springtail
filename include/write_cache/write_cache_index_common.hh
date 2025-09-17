#pragma once

#include <memory>
#include <string>

#include <fmt/core.h>

#include <storage/extent.hh>

#include <pg_repl/pg_repl_instrument.hh>

namespace springtail {

    /**
     * @brief Row data structure; contains pkey, and data for a row
     */
    struct WriteCacheIndexExtent {
        uint64_t xid;
        uint64_t xid_seq;
        ExtentPtr data;

        using clock = std::chrono::steady_clock;
        INSTRUMENT_INGEST_DATA(clock::time_point, ts_created)

        WriteCacheIndexExtent(uint64_t xid, uint64_t xid_seq, const ExtentPtr &data)
            : xid(xid), xid_seq(xid_seq), data(data)
        { 
            INSTRUMENT_INGEST(LOG_LEVEL_OBSERVABILITY_1, { ts_created = clock::now(); })
        }
        WriteCacheIndexExtent(const WriteCacheIndexExtent&) = delete;
        WriteCacheIndexExtent(WriteCacheIndexExtent&&) = delete;
        const WriteCacheIndexExtent& operator=(const WriteCacheIndexExtent&) = delete;

        std::string dump() {
            return fmt::format("XID:{} XID_SEQ:{}", xid, xid_seq);
        }
    };
    typedef std::shared_ptr<WriteCacheIndexExtent> WriteCacheIndexExtentPtr;
} // namespace springtail
