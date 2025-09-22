#include <vector>
#include <memory>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{
    void
    WriteCacheIndex::add_extent(uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data)
    {
        WriteCacheTableSetPtr partition = _get_partition(tid);
        partition->add_extent(tid, pg_xid, lsn, data);
    }

    void
    WriteCacheIndex::commit(uint64_t pg_xid, uint64_t xid, PostgresTimestamp commit_ts)
    {
        for (auto &p: _partitions) {
            p->commit(pg_xid, xid, commit_ts);
        }
    }

    void
    WriteCacheIndex::commit(const std::vector<uint64_t>& pg_xids, uint64_t xid, PostgresTimestamp commit_ts)
    {
        for (auto &p: _partitions) {
            p->commit(pg_xids, xid, commit_ts);
        }
    }

    void
    WriteCacheIndex::drop_table(uint64_t tid, uint64_t pg_xid)
    {
        WriteCacheTableSetPtr partition = _get_partition(tid);
        partition->drop_table(tid, pg_xid);
    }

    void
    WriteCacheIndex::abort(uint64_t pg_xid)
    {
        for (auto &p: _partitions) {
            p->abort(pg_xid);
        }
    }

    void
    WriteCacheIndex::abort(std::vector<uint64_t> pg_xids)
    {
        for (auto &p: _partitions) {
            for (auto &pg_xid: pg_xids) {
                p->abort(pg_xid);
            }
        }
    }

    std::vector<uint64_t>
    WriteCacheIndex::get_tids(uint64_t xid, uint32_t count, uint64_t &cursor)
    {
        std::vector<uint64_t> tids;
        uint32_t target_size = count;
        uint64_t start_offset = cursor;
        uint64_t partition_cursor;
        uint64_t new_cursor = 0;

        // iterate through partitions building a resultset of desired size
        for (auto &p: _partitions) {
            p->get_tids(xid, count, start_offset, partition_cursor, tids);

            // update offsets for new partition
            if (start_offset > partition_cursor) {
                // if the provided cursor is further than the partition, update it
                start_offset -= partition_cursor;
            } else {
                start_offset = 0;
            }
            new_cursor += partition_cursor;

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

    std::vector<WriteCacheIndexExtentPtr>
    WriteCacheIndex::get_extents(uint64_t tid, uint64_t xid,
                                 uint32_t count, uint64_t &cursor, PostgresTimestamp &commit_ts)
    {
        WriteCacheTableSetPtr partition = _get_partition(tid);
        std::vector<WriteCacheIndexExtentPtr> extents;
        uint64_t start_offset = cursor;
        partition->get_extents(tid, xid, count, start_offset, cursor, extents, commit_ts);
        return extents;
    }

    void
    WriteCacheIndex::evict_table(uint64_t tid, uint64_t xid)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        partition->evict_table(tid, xid);
    }

    void
    WriteCacheIndex::evict_xid(uint64_t xid)
    {
        // iterate through partitions building a resultset of desired size
        for (auto &p: _partitions) {
            p->evict_xid(xid);
        }
    }
}
