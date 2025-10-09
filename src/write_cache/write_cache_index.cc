#include <vector>
#include <memory>

#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

namespace springtail
{
    void
    WriteCacheIndex::add_extent(uint64_t tid, uint64_t pg_xid, uint64_t lsn, const ExtentPtr data, bool on_disk)
    {
        if (!on_disk) {
            _memory_in_use += data->byte_count();
        }
        WriteCacheTableSetPtr partition = _get_partition(tid);
        partition->add_extent(tid, pg_xid, lsn, data, on_disk);
    }

    void
    WriteCacheIndex::commit(uint64_t pg_xid, uint64_t xid, WriteCacheTableSet::Metadata md)
    {
        for (auto &p: _partitions) {
            p->commit(pg_xid, xid, std::move(md));
        }
    }

    void
    WriteCacheIndex::commit(const std::vector<uint64_t>& pg_xids, uint64_t xid, WriteCacheTableSet::Metadata md)
    {
        for (auto &p: _partitions) {
            p->commit(pg_xids, xid, std::move(md));
        }
    }

    void
    WriteCacheIndex::drop_table(uint64_t tid, uint64_t pg_xid, uint64_t &memory_removed)
    {
        WriteCacheTableSetPtr partition = _get_partition(tid);
        partition->drop_table(tid, pg_xid, memory_removed);
        _memory_in_use -= memory_removed;
    }

    void
    WriteCacheIndex::abort(uint64_t pg_xid, uint64_t &memory_removed)
    {
        uint64_t stored_size = 0;

        for (auto &p: _partitions) {
            p->abort(pg_xid, stored_size);
        }

        _memory_in_use -= stored_size;
        memory_removed += stored_size;
        std::filesystem::path file_name = _db_dir_path / std::to_string(pg_xid);
        IOMgr::get_instance()->remove(file_name);
    }

    void
    WriteCacheIndex::abort(const std::vector<uint64_t>& pg_xids, uint64_t &memory_removed)
    {
        uint64_t stored_size = 0;

        for (auto &p: _partitions) {
            for (auto pg_xid: pg_xids) {
                p->abort(pg_xid, stored_size);
            }
        }

        _memory_in_use -= stored_size;
        memory_removed += stored_size;
        for (auto pg_xid: pg_xids) {
            std::filesystem::path file_name = _db_dir_path / std::to_string(pg_xid);
            if (std::filesystem::exists(file_name)) {
                IOMgr::get_instance()->remove(file_name);
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
                                 uint32_t count, uint64_t &cursor, WriteCacheTableSet::Metadata &md)
    {
        WriteCacheTableSetPtr partition = _get_partition(tid);
        std::vector<WriteCacheIndexExtentPtr> extents;
        uint64_t start_offset = cursor;
        partition->get_extents(tid, xid, count, start_offset, cursor, extents, md);
        return extents;
    }

    void
    WriteCacheIndex::evict_table(uint64_t tid, uint64_t xid, uint64_t &memory_removed)
    {
        std::shared_ptr<WriteCacheTableSet> partition = _get_partition(tid);
        uint64_t stored_size = 0;
        partition->evict_table(tid, xid, stored_size);
        _memory_in_use -= stored_size;
        memory_removed += stored_size;
    }

    void
    WriteCacheIndex::evict_xid(uint64_t xid, uint64_t &memory_removed)
    {
        uint64_t stored_size = 0;
        std::set<uint64_t> all_pg_xids;

        // evict xid in each partition
        for (auto &p: _partitions) {
            p->evict_xid(xid, stored_size, all_pg_xids);
        }

        // cleanup files
        for (auto pg_xid: all_pg_xids) {
            std::filesystem::path file_name = _db_dir_path / std::to_string(pg_xid);
            if (std::filesystem::exists(file_name)) {
                IOMgr::get_instance()->remove(file_name);
            }
        }

        // adjust used memory size
        _memory_in_use -= stored_size;
        memory_removed += stored_size;
    }

    nlohmann::json
    WriteCacheIndex::get_partition_stats() const
    {
        nlohmann::json stats = nlohmann::json::array();
        for (const auto &p: _partitions) {
            nlohmann::json partition_stats = p->get_stats();
            stats.push_back(partition_stats);
        }
        return stats;
    }

}
