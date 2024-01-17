#include <vector>
#include <memory>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{
    void
    WriteCacheIndex::add_rows(uint64_t tid, uint64_t eid, const std::vector<WriteCacheIndexRowPtr> &data)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->add_rows(tid, eid, data);
    }

    std::vector<int64_t>
    WriteCacheIndex::get_tids(uint64_t start_xid, uint64_t end_xid, uint32_t count)
    {
        std::vector<int64_t> tids;
        uint32_t target_size = count;
        for (auto &p: _partitions) {
            p->get_tids(start_xid, end_xid, count, tids);
            count = target_size - tids.size();
        }
        return tids;
    }

    std::vector<int64_t>
    WriteCacheIndex::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                              uint32_t count, uint64_t &cursor)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        std::vector<int64_t> eids;
        partition->get_eids(tid, start_xid, end_xid, count, eids);
        return eids;
    }

    std::vector<std::shared_ptr<WriteCacheIndexRow>>
    WriteCacheIndex::get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid, int count)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        std::vector<std::shared_ptr<WriteCacheIndexRow>> rows;
        partition->get_rows(tid, eid, start_xid, end_xid, count, rows);
        return rows;
    }

    void
    WriteCacheIndex::evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->evict_table(tid, start_xid, end_xid);
    }

    void
    WriteCacheIndex::add_table_change(WriteCacheIndexTableChangePtr change)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(change->tid);
        partition->add_table_change(change);
    }

    std::vector<WriteCacheIndexTableChangePtr>
    WriteCacheIndex::get_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        std::vector<WriteCacheIndexTableChangePtr> changes;
        partition->get_table_changes(tid, start_xid, end_xid, changes);
        return changes;
    }

    void
    WriteCacheIndex::evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->evict_table_changes(tid, start_xid, end_xid);
    }
}