#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <storage/vacuumer.hh>

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
    _extent_map[xid][file].emplace_back(extent_id, size);
}

void
Vacuumer::expire_snapshot(const std::filesystem::path &table_dir,
                          uint64_t xid)
{
    // XXX -- Deepak -- you'll need to call this when we perform the table swap in the committer.

    std::unique_lock lock(_mutex);
    _snapshot_map[xid].emplace_back(table_dir);
}

void
Vacuumer::_internal_run()
{
    while (!_is_shutting_down()) {
        // sleep for 1 second before trying to expire more data
        std::this_thread::sleep_for(std::chrono::seconds(1));

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

        // lock while accessing the maps
        std::unique_lock lock(_mutex);

        // expire extents through the min XID
        while (true) {
            auto it = _extent_map.begin();

            // if nothing to expire, stop
            if (it == _extent_map.end()) {
                break;
            }

            // if everything to expire still in-use, stop
            if (it->first > target_xid) {
                break;
            }

            // retrieve a list of extents to expire
            auto file_map = std::move(it->second);
            _extent_map.erase(it);

            // unlock and expire the extents in the list
            lock.unlock();

            // XXX -- Deepak -- instead of the following code we need to find contiguous ZFS blocks
            //                  and hole punch those, leaving any partial holes to be picked up
            //                  later.  We'll need to combine what we have in-memory with any
            //                  partial holes we've recorded on-disk.  Then we can punch any new
            //                  complete blocks and write out the newly outstanding partial blocks.

            // create holes for all extents to expire
            for (auto &entry : file_map) {
                int fd = open(entry.first.c_str(), O_RDWR | O_CREAT, 0644);
                if (fd < 0) {
                    LOG_ERROR("Unable to open file for vacuuming: {}", entry.first);
                    continue;
                }

                for (auto &info : entry.second) {
                    int ret = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, info.offset, info.size);
                    if (ret < 0) {
                        LOG_ERROR("fallocate() failed: {} -- offset {} len {}", errno, info.offset, info.size);
                    }
                }
                close(fd);
            }

            // reacquire the lock before accessing the extent map
            lock.lock();
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
