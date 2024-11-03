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
    WriteCacheIndex::get_tids(uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor)
    {
        std::vector<int64_t> tids;
        uint32_t target_size = count;
        uint64_t start_offset = cursor;
        uint64_t end_offset;
        uint64_t new_cursor = 0;

        // iterate through partitions building a resultset of desired size
        for (auto &p: _partitions) {
            p->get_tids(start_xid, end_xid, count, start_offset, end_offset, tids);

            // update offsets for new partition
            if (start_offset > end_offset) {
                // if the provided cursor is further than the partition, update it
                start_offset -= end_offset;
            } else {
                start_offset = 0;
            }
            new_cursor += end_offset;

            // see where we are
            count = target_size - tids.size();
            if (count <= 0) {
                break;
            }
        }
        // set new cursor based on sum of end_offset's
        cursor = new_cursor;

        return tids;
    }

    std::vector<int64_t>
    WriteCacheIndex::get_eids(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                              uint32_t count, uint64_t &cursor)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        std::vector<int64_t> eids;
        partition->get_eids(tid, start_xid, end_xid, count, cursor, eids);
        return eids;
    }

    std::vector<std::shared_ptr<WriteCacheIndexRow>>
    WriteCacheIndex::get_rows(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid,
                              uint32_t count, uint64_t &cursor)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        std::vector<std::shared_ptr<WriteCacheIndexRow>> rows;
        partition->get_rows(tid, eid, start_xid, end_xid, count, cursor, rows);
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

    void
    WriteCacheIndex::set_clean_flag(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->set_clean_flag(tid, eid, start_xid, end_xid);
    }

    void
    WriteCacheIndex::reset_clean_flag(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->reset_clean_flag(tid, start_xid, end_xid);
    }
}
