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

    // get the base directory for table data
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "vacuum_dir", _vacuum_data_base);
    _vacuum_data_base = Properties::make_absolute_path(_vacuum_data_base);
    std::filesystem::create_directories(_vacuum_data_base);
    _global_vacuum_file = _vacuum_data_base / "0.global";

    std::vector<SchemaColumn> global_vacuum_columns({
            { "file", 0, SchemaType::TEXT, 0, false, 0 },
            { "offset", 1, SchemaType::UINT64, 0, false, 1 },
            { "size", 2, SchemaType::UINT64, 0, false }
            });
    _global_vacuum_schema = std::make_shared<ExtentSchema>(global_vacuum_columns);

    std::vector<SchemaColumn> vacuum_file_columns({
            { "offset", 0, SchemaType::UINT64, 0, false, 0 },
            { "size", 1, SchemaType::UINT64, 0, false }
            });
    _vacuum_file_schema = std::make_shared<ExtentSchema>(vacuum_file_columns);
}

void
Vacuumer::_internal_shutdown()
{
    _shutdown = true;
    _vacuumer_thread.join();
}

void
Vacuumer::commit_expired_extents()
{
    std::unique_lock lock(_mutex);

    auto handle = IOMgr::get_instance()->open(_global_vacuum_file, IOMgr::IO_MODE::APPEND, true);
    for (const auto& [file, xid_map] : _extent_map) {
        for (const auto& [xid, extents] : xid_map) {
            ExtentHeader header(ExtentType(), xid, _global_vacuum_schema->row_size(), _global_vacuum_schema->field_types());
            auto extent = std::make_shared<Extent>(std::move(header));

            for (const auto& hole_info: extents) {
                auto fields = std::make_shared<FieldArray>(4);
                fields->at(0) = std::make_shared<ConstTypeField<std::string>>(file);
                fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.offset);
                fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.size);
                auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

                // insert the tuple into the extent
                auto row = extent->append();
                MutableTuple(_global_vacuum_schema->get_mutable_fields(), &row).assign(std::move(tuple));

            }
            auto response = extent->async_flush(handle);
            response.wait();
        }
    }
    _extent_map.clear();
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
Vacuumer::_truncate_file(const std::filesystem::path &file, uint64_t offset)
{
    int fd = ::open(file.c_str(), O_WRONLY);
    if (fd == -1) {
        LOG_ERROR("Failed to open file {} for truncation: {}", file, errno);
        throw;
    }

    if (::ftruncate(fd, offset) == -1) {
        LOG_ERROR("Failed to truncate file {} to offset {}: {}", file, offset, errno);
        ::close(fd);
        throw;
    }

    ::close(fd);
}

int64_t
Vacuumer::_get_file_size(const std::filesystem::path& path) {
    try {
        if (std::filesystem::exists(path) && std::filesystem::is_regular_file(path)) {
            return static_cast<int64_t>(std::filesystem::file_size(path));
        } else {
            LOG_ERROR("Given {} doesn't exist or not a regular file", path);
            return -1;
        }
    } catch (const std::filesystem::filesystem_error& e) {
        LOG_ERROR("_get_file_size exception: {}", e.what());
        return -1;
    }
}

void
Vacuumer::_internal_run()
{
    while(!_shutdown) {
        // sleep for 1 second before trying to expire more data
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // lock while accessing the maps
        std::unique_lock lock(_mutex);

        bool processing_vacuum_file = false;

        if (_extent_map.empty() && _get_file_size(_global_vacuum_file) > VACUUM_THRESHOLD_SIZE) {
            auto handle = IOMgr::get_instance()->open(_global_vacuum_file, IOMgr::IO_MODE::READ, true);
            int start_offset = 0;
            auto response = handle->read(start_offset);

            auto file_f = _global_vacuum_schema->get_field("file");
            auto offset_f = _global_vacuum_schema->get_field("offset");
            auto size_f = _global_vacuum_schema->get_field("size");

            while (!response->data.empty()) {
                auto extent = std::make_shared<Extent>(response->data);
                for (auto &row : *extent) {
                    auto file = file_f->get_text(&row);
                    auto offset = offset_f->get_uint64(&row);
                    auto size = size_f->get_uint64(&row);
                    auto xid = extent->header().xid;
                    _extent_map[file][xid].emplace_back(offset, size);
                }
                start_offset = response->next_offset;
                response = handle->read(start_offset);
            }
            processing_vacuum_file = true;
        }

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
            //lock.unlock();

            // 3. Punch the aligned extents
            auto punched_extents = hole_punch_file(file, punchable);

            // reacquire the lock before accessing the extent map
            //lock.lock();

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

        if (processing_vacuum_file) {
            _truncate_file(_global_vacuum_file, 0);
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
        lock.unlock();
    }
}

} // namespace springtail
