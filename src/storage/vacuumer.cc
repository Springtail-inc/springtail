#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <storage/vacuumer.hh>
#include <storage/interval_tree.hh>

namespace springtail {

void
Vacuumer::expire_extent(const std::filesystem::path &file,
                        uint64_t extent_id,
                        uint32_t size,
                        uint64_t xid)
{
    // XXX -- Deepak -- we should page these to disk for later processing on a per-file basis, but
    //                  continue to keep an in-memory record of mutated files so that we can know
    //                  what to vacuum.  The bookkeeping for that might just be the files mutated at
    //                  each XID?

    std::unique_lock lock(_mutex);
    _extent_map[file][xid].emplace_back(extent_id, size);
}

void
Vacuumer::expire_snapshot(const std::filesystem::path &table_dir,
                          uint64_t xid)
{
    // XXX -- Deepak -- you'll need to call this when we perform the table swap in the committer.

    std::unique_lock lock(_mutex);
    _snapshot_map[xid].emplace_back(table_dir);
}

std::vector<HoleInfo>
Vacuumer::hole_punch_file(const std::string& file,
                          const std::vector<HoleInfo>& input_extents)
{
    std::vector<HoleInfo> punched;
    int fd = open(file.c_str(), O_RDWR);
    if (fd < 0) {
        LOG_ERROR("Unable to open file for vacuuming: {}", file.c_str());
        return punched;
    }

    for (const auto& [aligned_start, aligned_size] : input_extents) {
        int ret = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, aligned_start, aligned_size);
        if (ret == 0) {
            punched.emplace_back(aligned_start, aligned_size);
        } else {
            LOG_ERROR("fallocate() failed: {} -- offset {} len {}: {}", file.c_str(), aligned_start, aligned_size);
        }
    }

    close(fd);
    return punched;
}

uint64_t get_vacuum_cutoff_xid(const std::string& file)
{
    // XXX -- Deepak -- just realizing we need to retrieve the target XID for each DB separately
    //                  since the XIDs are independent for each DB.  We may need to separate the
    //                  individual tracking of the files per db and then use the information in
    //                  the file path to determine which DB XID is appropriate for it, meaning
    //                  we'll need to switch up the map structures to index on file first.
    //                  Might not matter if we shift to a more on-disk approach.

    // check the progress of the XID at the FDWs
    uint64_t target_xid = _redis_ddl.min_schema_xid(_db_id);

    // XXX -- Deepak -- check the progress of the XID in the indexer
    //                  we can't expire data if the indexer needs it

    return target_xid;
}

void
Vacuumer::_internal_run()
{
    while (!_is_shutting_down()) {
        // sleep for 1 second before trying to expire more data
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // lock while accessing the maps
        std::unique_lock lock(_mutex);

        for (auto& [file, xid_map] : extent_map) {
            uint64_t cutoff_xid = get_vacuum_cutoff_xid(file);  // Get safest XID to vacuum till that point

            // Iterate over all xids in sorted order
            for (auto it = xid_map.begin(); it != xid_map.end(); ) {
                uint64_t xid = it->first;

                // Stop processing if xid is beyond cutoff
                if (xid >= cutoff_xid) {
                    break;
                }

                // Step 1: Merge all extents into an interval tree
                IntervalTree<uint64_t> itree;
                for (const auto& [offset, size] : it->second) {
                    itree.insert(offset, offset + size);
                }

                std::vector<Extent> merged_extents;    // punchable aligned blocks
                std::vector<Extent> leftover_extents;  // unaligned fragments to keep

                // Step 2: For each merged extent, extract aligned + unaligned regions
                for (const auto& [start, end] : itree.to_vector()) {
                    uint64_t aligned_start = align_up(start, kPunchAlign);
                    uint64_t aligned_end   = align_down(end, kPunchAlign);

                    // Add aligned portion to punch list
                    if (aligned_start < aligned_end) {
                        merged_extents.emplace_back(aligned_start, aligned_end - aligned_start);
                    }

                    // Preserve prefix before aligned start
                    if (start < aligned_start) {
                        leftover_extents.emplace_back(start, aligned_start - start);
                    }

                    // Preserve suffix after aligned end
                    if (aligned_end < end) {
                        leftover_extents.emplace_back(aligned_end, end - aligned_end);
                    }
                }

                // Step 3: Punch the aligned regions
                // unlock and expire the extents in the list
                lock.unlock();
                auto punched_extents = hole_punch_file(file, merged_extents);

                // Step 4: If nothing remains, erase this xid; otherwise update it
                // reacquire the lock before accessing the extent map
                lock.lock();

                if (leftover_extents.empty()) {
                    it = xid_map.erase(it);
                } else {
                    it->second = std::move(leftover_extents);
                    ++it;
                }
            }
        }


        // expire snapshots through the min XID
        while (true) {
            auto it = _snapshot_map.begin();

            // if nothing to expire, stop
            if (it == _snapshot_map.end()) {
                break;
            }

            // if everything to expire still in-use, stop
            if (it->first > target_xid) {
                break;
            }

            // retrieve a list of extents to expire
            auto snapshot_list = std::move(it->second);
            _snapshot_map.erase(it);

            // unlock and expire the extents in the list
            lock.unlock();

            // clear the table directories associated with the expired snapshots
            for (auto &dir : snapshot_list) {
                std::error_code ec;
                std::filesystem::remove_all(dir, ec);
                if (ec) {
                    LOG_ERROR("remove_all() failed: {}", ec.message());
                }
            }

            // reacquire the lock before proceeding
            lock.lock();
        }
    }
}

} // namespace springtail
