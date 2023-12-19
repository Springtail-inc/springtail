#include <vector>
#include <memory>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{
    void
    WriteCacheIndex::add_row(uint64_t tid, uint64_t eid, std::shared_ptr<WriteCacheIndexRow> data)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->add_row(tid, eid, data);
    }

/*
    std::vector<WriteCacheIndex::XidIdRange>
    WriteCacheIndex::get_tids(uint64_t start_xid, uint64_t end_xid,
                                uint32_t count, uint64_t &cursor)
    {
        return {};
    }

    std::vector<WriteCacheIndex::XidIdRange>
    WriteCacheIndex::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                uint32_t count, uint64_t &cursor)
    {
        return {};
    }

    std::vector<WriteCacheIndex::XidIdRange>
    WriteCacheIndex::get_rids(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid,
                              uint32_t count, uint64_t &cursor)
    {
        return {};
    }

    std::vector<std::shared_ptr<WriteCacheIndex::RowData>>
    WriteCacheIndex::get_rows(std::vector<rid_t> row_ids)
    {
        return {};
    }
     */
}