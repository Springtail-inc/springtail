#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <storage/vacuumer.hh>
#include <storage/interval_tree.hh>

namespace springtail {

void
Vacuumer::init()
{
    _vacuumer_thread = std::thread(&Vacuumer::_internal_run, this);
}

void
Vacuumer::_internal_shutdown()
{
    _shutdown = true;
    _vacuumer_thread.join();
}

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

std::vector<Vacuumer::HoleInfo>
Vacuumer::hole_punch_file(const std::string& file,
                          const std::vector<Vacuumer::HoleInfo>& input_extents)
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
            LOG_ERROR("fallocate() failed: {} -- offset {} len {}", file.c_str(), aligned_start, aligned_size);
        }
    }

    close(fd);
    return punched;
}

uint64_t
Vacuumer::get_vacuum_cutoff_xid(const std::string& file)
{
    // XXX -- Deepak -- just realizing we need to retrieve the target XID for each DB separately
    //                  since the XIDs are independent for each DB.  We may need to separate the
    //                  individual tracking of the files per db and then use the information in
    //                  the file path to determine which DB XID is appropriate for it, meaning
    //                  we'll need to switch up the map structures to index on file first.
    //                  Might not matter if we shift to a more on-disk approach.

    // check the progress of the XID at the FDWs
    RedisDDL _redis_ddl;
    uint64_t target_xid = _redis_ddl.min_schema_xid(1);

    // XXX -- Deepak -- check the progress of the XID in the indexer
    //                  we can't expire data if the indexer needs it

    return target_xid;
}

void
Vacuumer::_internal_run()
{
    while(!_shutdown) {
        // sleep for 1 second before trying to expire more data
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // lock while accessing the maps
        std::unique_lock lock(_mutex);

        for (auto file_it = _extent_map.begin(); file_it != _extent_map.end(); ) {
            const std::string& file = file_it->first;
            auto& xid_map = file_it->second;
            //uint64_t cutoff_xid = get_vacuum_cutoff_xid(file);  // Get safest XID to vacuum till that point

            // 1. Build a merged interval tree for all xids < cutoff_xid
            IntervalTree<uint64_t> tree;
            std::vector<uint64_t> xids_to_process;

            for (const auto& [xid, extents] : xid_map) {
                //if (xid >= cutoff_xid) {
                //    break;
                //}
                for (const auto& [offset, size] : extents) {
                    tree.insert(offset, offset + size);
                }
                xids_to_process.push_back(xid);  // store for later cleanup
            }

            if (tree.empty()) {
                ++file_it;
                continue;
            }

            // 2. Identify punchable (aligned) extents only
            std::vector<Vacuumer::HoleInfo> punchable;
            for (const auto& [start, end] : tree.to_vector()) {
                uint64_t aligned_start = align_up(start, kPunchAlign);
                uint64_t aligned_end = align_down(end, kPunchAlign);
                if (aligned_start < aligned_end) {
                    punchable.emplace_back(aligned_start, aligned_end - aligned_start);
                }
            }

            // 3. If nothing is punchable, skip all modification
            if (punchable.empty()) {
                ++file_it;
                continue;
            }

            // unlock and expire the extents in the list
            lock.unlock();

            // 3. Punch the aligned extents
            auto punched_extents = hole_punch_file(file, punchable);

            // reacquire the lock before accessing the extent map
            lock.lock();

            if (punched_extents.empty()) {
                ++file_it;
                continue;
            }

            // 5. Subtract punched extents from each original XID
            IntervalTree<uint64_t> punched_tree;
            for (const auto& [offset, size] : punched_extents) {
                punched_tree.insert(offset, offset + size);
            }

            for (uint64_t xid : xids_to_process) {
                auto it = xid_map.find(xid);
                if (it == xid_map.end()) {
                    continue;
                }

                IntervalTree<uint64_t> original;
                for (const auto& [offset, size] : it->second) {
                    original.insert(offset, offset + size);
                }

                for (const auto& [start, end] : punched_tree.to_vector()) {
                    original.subtract(start, end);
                }

                auto remaining = original.to_vector();
                if (remaining.empty()) {
                    xid_map.erase(it);
                } else {
                    it->second.clear();
                    for (const auto& [start, end] : remaining) {
                        it->second.emplace_back(start, end - start);
                    }
                }
            }

            // 6. Cleanup file entry if no xids remain
            if (xid_map.empty()) {
                file_it = _extent_map.erase(file_it);
            } else {
                ++file_it;
            }
        }

        // expire snapshots through the min XID
        //while (true) {
        //    auto it = _snapshot_map.begin();

        //    // if nothing to expire, stop
        //    if (it == _snapshot_map.end()) {
        //        break;
        //    }

        //    // if everything to expire still in-use, stop
        //    if (it->first > cutoff_xid) {
        //        break;
        //    }

        //    // retrieve a list of extents to expire
        //    auto snapshot_list = std::move(it->second);
        //    _snapshot_map.erase(it);

        //    // unlock and expire the extents in the list
        //    lock.unlock();

        //    // clear the table directories associated with the expired snapshots
        //    for (auto &dir : snapshot_list) {
        //        std::error_code ec;
        //        std::filesystem::remove_all(dir, ec);
        //        if (ec) {
        //            LOG_ERROR("remove_all() failed: {}", ec.message());
        //        }
        //    }

        //    // reacquire the lock before proceeding
        //    lock.lock();
        //}
    }
}

} // namespace springtail
