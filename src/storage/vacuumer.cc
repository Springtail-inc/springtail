#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <storage/vacuumer.hh>
#include <storage/interval_tree.hh>

#include <xid_mgr/xid_mgr_client.hh>

namespace springtail {

void
Vacuumer::_init()
{
    // Get the namespace for the vacuumer
    // as it can run in different daemons
    std::string vacuum_global_namespace = springtail_retreive_argument<std::string>(
            ServiceId::VacuumerId, "vacuum_global_ns", false).value_or("default_vacuum");

    // get the configs for vacuum - size threshold, block size and base dir
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);

    nlohmann::json vacuum_config_json;
    Json::get_to<nlohmann::json>(json, "vacuum_config", vacuum_config_json);

    // Global vacuum size threshold to trigger vacuum
    if (vacuum_config_json.contains("global_file_size_threshold")) {
        Json::get_to<uint64_t>(vacuum_config_json, "global_file_size_threshold", _global_file_size_threshold);
    } else {
        _global_file_size_threshold = VacuumConfig::GLOBAL_FILE_SIZE_THRESHOLD;
    }

    // Block size to hole-punch
    if (vacuum_config_json.contains("hole_punch_block_size")) {
        Json::get_to<uint64_t>(vacuum_config_json, "hole_punch_block_size", _hole_punch_block_size);
    } else {
        _hole_punch_block_size = VacuumConfig::HOLE_PUNCH_BLOCK_SIZE;
    }

    // Vacuum base dir
    Json::get_to<std::filesystem::path>(vacuum_config_json, "vacuum_dir", _vacuum_data_base);
    _vacuum_data_base = Properties::make_absolute_path(_vacuum_data_base) / vacuum_global_namespace;
    std::filesystem::create_directories(_vacuum_data_base);

    // Global vacuum file
    _global_vacuum_file = _vacuum_data_base / fmt::format("{}{}", 0, VacuumConfig::GLOBAL_FILE_SUFFIX);

    // Define schema for persisting expired extents
    // Global vacuum schema
    std::vector<SchemaColumn> global_vacuum_columns({
            { "file", 0, SchemaType::TEXT, 0, false, 0 },
            { "offset", 1, SchemaType::UINT64, 0, false, 1},
            { "size", 2, SchemaType::UINT64, 0, false},
            { "file_dropped", 3, SchemaType::BOOLEAN, 0, false}
            });
    _global_vacuum_schema = std::make_shared<ExtentSchema>(global_vacuum_columns);

    // individual vacuum file schema
    std::vector<SchemaColumn> vacuum_file_columns({
            { "offset", 0, SchemaType::UINT64, 0, false, 0 },
            { "size", 1, SchemaType::UINT64, 0, false }
            });
    _vacuum_file_schema = std::make_shared<ExtentSchema>(vacuum_file_columns);

    // Vacuum enabled - true/false
    Json::get_to<bool>(vacuum_config_json, "enabled", _vacuum_start_enabled);

    if (_vacuum_start_enabled) {
        // Core thread of the vacuumer
        _vacuumer_thread = std::thread(&Vacuumer::_internal_run, this);
        LOG_INFO("Vacuumer thread started");
    }
}

void
Vacuumer::_internal_shutdown()
{
    // Set flag and wait for the thread to join
    _shutdown = true;

    if (_vacuum_start_enabled) {
        _vacuumer_thread.join();
        LOG_INFO("Vacuumer thread joined");
    }
}

void
Vacuumer::commit_expired_extents(uint64_t db_id, uint64_t committed_xid)
{
    std::unique_lock lock(_mutex);

    // Lets persist expired extents
    auto handle = IOMgr::get_instance()->open(_global_vacuum_file, IOMgr::IO_MODE::APPEND, true);

    if (!_extent_map.empty()) {
        // iterate files
        for (auto file_it = _extent_map.begin(); file_it != _extent_map.end(); )
        {
            // ordered map< uint64_t, Extents >
            auto& xid_map = file_it->second;
            auto  xid_it  = xid_map.begin();

            // iterate xids for this file
            while (xid_it != xid_map.end())
            {
                // Process only till committed XID
                if (xid_it->first > committed_xid) {
                    break;
                }

                // Create extent with hole list
                auto extent = _create_extent_using_hole_list(xid_it->first, file_it->first, xid_it->second);

                // Lets get the dropped snapshots at this XID
                // and add to the same extent
                auto snapshot_db_entry = _snapshot_map.find(db_id);
                if (snapshot_db_entry != _snapshot_map.end()) {
                    auto& snapshot_xid_map = snapshot_db_entry->second;

                    auto snapshot_xid_entry = snapshot_xid_map.find(xid_it->first);
                    if (snapshot_xid_entry != snapshot_xid_map.end()) {
                        SnapshotList& snapshot_list = snapshot_xid_entry->second;
                        _upsert_extent_using_snapshot_list(xid_it->first, snapshot_list, extent);
                    }
                }

                // Flush to disk
                auto response = extent->async_flush(handle);
                response.wait();

                // erase and advance in one step
                xid_it = xid_map.erase(xid_it);
            }

            // if everything for this file was committed, remove the file entry too
            if (xid_map.empty()) {
                file_it = _extent_map.erase(file_it);
            } else {
                ++file_it;
            }
        }
    } else {
        // If only drop happened at an XID, there is a possibility
        // that only snapshot map will have details assuming
        // all records up to XID-1 are flushed to disk
        auto snapshot_db_entry = _snapshot_map.find(db_id);
        if (snapshot_db_entry != _snapshot_map.end()) {
            auto& snapshot_xid_map = snapshot_db_entry->second;
            auto snapshot_xid_entry = snapshot_xid_map.begin();

            // Flush only the snapshot deletion up to the committed XID
            while (snapshot_xid_entry != snapshot_xid_map.end()) {
                if (snapshot_xid_entry->first > committed_xid) {
                    break;
                }
                SnapshotList& snapshot_list = snapshot_xid_entry->second;

                // Create extent with snapshot deletion records
                auto extent = _upsert_extent_using_snapshot_list(snapshot_xid_entry->first, snapshot_list);

                // Flush to disk
                auto response = extent->async_flush(handle);
                response.wait();

                // Erase and advance
                snapshot_xid_entry = snapshot_xid_map.erase(snapshot_xid_entry);
            }
        }
    }
}

std::shared_ptr<Extent>
Vacuumer::_create_empty_extent_with_header(uint64_t xid)
{
    // Create extent header with XID
    // and return empty extent
    ExtentHeader header(ExtentType(), xid, _global_vacuum_schema->row_size(), _global_vacuum_schema->field_types());
    auto extent = std::make_shared<Extent>(std::move(header));
    return extent;
}

std::shared_ptr<Extent>
Vacuumer::_create_extent_using_hole_list(uint64_t xid, const std::filesystem::path& file, std::vector<HoleInfo> hole_list)
{
    auto extent = _create_empty_extent_with_header(xid);

    // Write the extents using the global vacuum schema
    for (const auto& hole_info: hole_list) {
        auto fields = std::make_shared<FieldArray>(4);
        fields->at(0) = std::make_shared<ConstTypeField<std::string>>(file);
        fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.offset);
        fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.size);
        fields->at(3) = std::make_shared<ConstTypeField<bool>>(false);

        auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

        // insert the tuple into the extent
        auto row = extent->append();
        MutableTuple(_global_vacuum_schema->get_mutable_fields(), &row).assign(std::move(tuple));
    }
    return extent;
}

std::shared_ptr<Extent>
Vacuumer::_upsert_extent_using_snapshot_list(uint64_t xid, SnapshotList snapshot_list, std::shared_ptr<Extent> extent)
{
    // If extent was not passed, lets create new extent
    if (extent == nullptr) {
        extent = _create_empty_extent_with_header(xid);
    }

    // Write the snapshot deletion details using the global vacuum schema
    for (const auto& snapshot_path : snapshot_list) {
        auto fields = std::make_shared<FieldArray>(4);
        fields->at(0) = std::make_shared<ConstTypeField<std::string>>(snapshot_path);
        fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(0);
        fields->at(2) = std::make_shared<ConstTypeField<uint64_t>>(0);
        fields->at(3) = std::make_shared<ConstTypeField<bool>>(true);
        auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

        // insert the tuple into the extent
        auto row = extent->append();
        MutableTuple(_global_vacuum_schema->get_mutable_fields(), &row).assign(std::move(tuple));
    }

    return extent;
}


void
Vacuumer::_update_global_vacuum_file(ExtentMap& expired_extents_map,
                                     SnapshotMap& expired_snapshots_map)
{
    // Lets persist expired extents
    // Creating unique runfile everytime to make sure we get new handle everytime
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    auto global_vacuum_runfile = _vacuum_data_base / fmt::format("{}{}", timestamp, VacuumConfig::GLOBAL_RUNFILE_SUFFIX);
    auto handle = IOMgr::get_instance()->open(global_vacuum_runfile, IOMgr::IO_MODE::WRITE, true);

    if (!expired_extents_map.empty()) {
        for (const auto& [file, xid_map] : expired_extents_map) {

            // If the file was removed as part of table deletion
            // skip maintaining in the global vacuum log
            if (!std::filesystem::exists(file)) {
                continue;
            }
            for (const auto& [xid, extents] : xid_map) {

                // Create extent with hole list
                auto extent = _create_extent_using_hole_list(xid, file, extents);

                // Lets get the dropped snapshots at this XID
                auto db_id = _get_db_id_from_path(file);
                auto snapshot_db_entry = expired_snapshots_map.find(db_id);
                if (snapshot_db_entry != expired_snapshots_map.end()) {
                    auto& snapshot_xid_map = snapshot_db_entry->second;
                    auto snapshot_xid_entry = snapshot_xid_map.find(xid);

                    // Update above extent with snapshot deletion list
                    if (snapshot_xid_entry != snapshot_xid_map.end()) {
                        auto& snapshot_list = snapshot_xid_entry->second;

                        _upsert_extent_using_snapshot_list(xid, snapshot_list, extent);

                        snapshot_xid_map.erase(snapshot_xid_entry);
                    }
                }

                // Flush to disk
                auto response = extent->async_flush(handle);
                response.wait();
            }
        }
    }

    // Process pending snapshot deletion entries
    for (const auto& [snapshot_db_id, snapshot_xid_map] : expired_snapshots_map) {
        for (const auto& [snapshot_xid, snapshot_list] : snapshot_xid_map) {

            // Create extent with snapshot deletion list and flush to disk
            auto extent = _upsert_extent_using_snapshot_list(snapshot_xid, snapshot_list);
            auto response = extent->async_flush(handle);
            response.wait();
        }
    }

    // Move runfile on to the global file
    if (std::filesystem::exists(global_vacuum_runfile)) {
        std::filesystem::rename(global_vacuum_runfile, _global_vacuum_file);
    }
}

void
Vacuumer::expire_extent(const std::filesystem::path &file,
                        uint64_t extent_id,
                        uint32_t size,
                        uint64_t xid)
{
    std::unique_lock lock(_mutex);
    _extent_map[file][xid].emplace_back(extent_id, size);
}

void
Vacuumer::expire_snapshot(uint64_t db_id,
                          const std::filesystem::path &table_dir,
                          uint64_t xid)
{
    std::unique_lock lock(_mutex);
    _snapshot_map[db_id][xid].emplace_back(table_dir);
}

std::vector<Vacuumer::HoleInfo>
Vacuumer::hole_punch_file(const std::string& file,
                          const std::vector<Vacuumer::HoleInfo>& input_extents)
{
    // Before punching, check if the file is operable
    std::vector<HoleInfo> not_punched;
    int fd = open(file.c_str(), O_RDWR);
    if (fd < 0) {
        LOG_ERROR("Unable to open file for vacuuming: {}", file.c_str());
        return not_punched;
    }

    // Hole_punch on the expired blocks
    for (const auto& [aligned_start, aligned_size] : input_extents) {
        int ret = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, aligned_start, aligned_size);
        if (ret != 0) {
            LOG_ERROR("fallocate() failed: {} -- offset {} len {}", file.c_str(), aligned_start, aligned_size);
            not_punched.emplace_back(aligned_start, aligned_size);
        }
    }

    close(fd);
    return not_punched;
}

uint64_t
Vacuumer::_get_vacuum_cutoff_xid(uint64_t db_id)
{
    RedisDDL _redis_ddl;

    // Cutoff XID for Vacuum = Minimum XID from (fdw, last_committed, index-build/drop, pending_ddl_xid)
    uint64_t min_fdw_xid = _redis_ddl.min_fdw_xid(db_id);
    uint64_t last_committed_xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);
    uint64_t min_index_xid = _redis_ddl.min_index_xid(db_id);
    uint64_t min_fdw_pending_schema_xid = _redis_ddl.min_fdw_pending_schema_xid(db_id);

    return std::min({min_fdw_xid, last_committed_xid, min_index_xid, min_fdw_pending_schema_xid});
}

std::string
Vacuumer::_generate_flat_filename(const std::filesystem::path& filepath)
{
    // Construct the file name in a flat fashion of the form:
    // table_dir_name_filename_partials
    // Eg: 1030830-0_0.idx_partials
    std::filesystem::path parent = filepath.parent_path().filename();
    std::filesystem::path file = filepath.filename();
    return parent.string() + "_" + file.string() + VacuumConfig::PARTIAL_FILE_SUFFIX;
}

uint64_t
Vacuumer::_get_db_id_from_path(const std::filesystem::path& path,
                               const std::string& keyword)
{
    for (auto it = path.begin(); it != path.end(); ++it) {
        if (*it == keyword) {
            auto next = std::next(it);
            if (next != path.end()) {
                try {
                    return static_cast<uint64_t>(std::stoull(next->string()));
                } catch (...) {
                    return std::numeric_limits<uint64_t>::max();
                }
            } else {
                return std::numeric_limits<uint64_t>::max();
            }
        }
    }
    return std::numeric_limits<uint64_t>::max();
}

void
Vacuumer::_cleanup_partial_files(const std::filesystem::path &path, bool is_directory)
{
    // If its table dir, delete all its partials
    // else delete only its partial file eg: index deletion case
    if (is_directory) {
        auto last_dir = path.filename().string();
        auto pattern_prefix = last_dir + "_";
        for (const auto& entry : std::filesystem::directory_iterator(_vacuum_data_base)) {
            const auto& filename = entry.path().filename().string();
            if (filename.starts_with(pattern_prefix) && filename.ends_with(VacuumConfig::PARTIAL_FILE_SUFFIX)) {
                std::filesystem::remove(_vacuum_data_base / filename);
            }
        }
    } else {
        auto partial_filename = _generate_flat_filename(path);
        std::filesystem::remove(_vacuum_data_base / partial_filename);
    }
}

void
Vacuumer::_update_vacuumed_partials_file(
        const std::filesystem::path &path,
        std::vector<Vacuumer::HoleInfo> partials)
{
    // Lets persist partials on the individual file using individual schema
    // If partials are empty, lets remove the file as the same is cosidered for every vacuum run
    std::string partial_filename = _generate_flat_filename(path);
    std::filesystem::path partial_file = _vacuum_data_base / partial_filename;

    if (partials.empty()) {
        if (std::filesystem::exists(partial_file)) {
            std::filesystem::remove(partial_file);
        }
    } else {
        // Creating unique runfile everytime to make sure we get new handle everytime
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        std::string partial_runfilename = fmt::format("{}.{}.run", timestamp, partial_filename);
        std::filesystem::path partial_runfile = _vacuum_data_base / partial_runfilename;

        // Create header with max XID as we dont rely on individual XID for partial
        ExtentHeader header(ExtentType(), constant::LATEST_XID, _vacuum_file_schema->row_size(), _vacuum_file_schema->field_types());
        auto extent = std::make_shared<Extent>(std::move(header));
        auto handle = IOMgr::get_instance()->open(partial_runfile, IOMgr::IO_MODE::WRITE, true);

        // Write the partials using individual file schema
        for (const auto& hole_info: partials) {
            auto fields = std::make_shared<FieldArray>(2);
            fields->at(0) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.offset);
            fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(hole_info.size);
            auto tuple = std::make_shared<FieldTuple>(fields, nullptr);

            // insert the tuple into the extent
            auto row = extent->append();
            MutableTuple(_vacuum_file_schema->get_mutable_fields(), &row).assign(std::move(tuple));
        }
        auto response = extent->async_flush(handle);
        response.wait();

        // Move runfile on to the partials file
        if (std::filesystem::exists(partial_runfile)) {
            std::filesystem::rename(partial_runfile, partial_file);
        }
    }
}

std::vector<Vacuumer::HoleInfo>
Vacuumer::_get_partials_from_file(const std::filesystem::path &path) {
    // Lets get the partials for an individual file
    std::vector<Vacuumer::HoleInfo> partials;
    std::filesystem::path partial_file = _vacuum_data_base / _generate_flat_filename(path);

    // Partial file = Hashed(input_file_path)
    // If it exists, lets get the partials, otherwise empty vector
    if (std::filesystem::exists(partial_file)) {
        auto handle = IOMgr::get_instance()->open(partial_file, IOMgr::IO_MODE::READ, true);
        int start_offset = 0;
        auto response = handle->read(start_offset);

        auto offset_f = _vacuum_file_schema->get_field("offset");
        auto size_f = _vacuum_file_schema->get_field("size");

        while (!response->data.empty()) {
            auto extent = std::make_shared<Extent>(response->data);
            for (auto &row : *extent) {
                auto offset = offset_f->get_uint64(&row);
                auto size = size_f->get_uint64(&row);
                partials.emplace_back(offset, size);
            }
            start_offset = response->next_offset;
            response = handle->read(start_offset);
        }
    }

    return partials;
}

void
Vacuumer::_recover_global_vacuum_file()
{
    // Map: db_id -> last_committed_xid
    std::map<uint64_t, uint64_t> committed_xid_map;

    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    auto global_vacuum_runfile = _vacuum_data_base / fmt::format("{}{}", timestamp, VacuumConfig::GLOBAL_RUNFILE_SUFFIX);

    // Read from global vacuum, write on runfile
    auto handle = IOMgr::get_instance()->open(_global_vacuum_file, IOMgr::IO_MODE::READ, true);
    auto runhandle = IOMgr::get_instance()->open(global_vacuum_runfile, IOMgr::IO_MODE::WRITE, true);

    int start_offset = 0;
    auto response = handle->read(start_offset);
    auto file_f = _global_vacuum_schema->get_field("file");

    while (!response->data.empty()) {

        // Get extent by extent, get XID and check if its <= last_committed_xid
        // if so, copy that to runfile
        auto extent = std::make_shared<Extent>(response->data);
        CHECK_GT(extent->row_count(), 0);

        auto xid = extent->header().xid;

        // Since different extent can have different DB records
        // get the db_id from the file path in the extent record
        auto i = extent->begin();
        auto &&row = *i;
        auto file = file_f->get_text(&row);
        auto db_id = _get_db_id_from_path(file);

        CHECK_GT(db_id, 0);

        // Get the last_committed_xid from the cache
        // else get from XID server and cache it
        auto db_it = committed_xid_map.find(db_id);
        uint64_t last_committed_xid;
        if (db_it != committed_xid_map.end()) {
            last_committed_xid = db_it->second;
        } else {
            last_committed_xid = XidMgrClient::get_instance()->get_committed_xid(db_id, 0);
            committed_xid_map[db_id] = last_committed_xid;
        }

        /* skip writing to runfile if xid > last_committed_xid */
        if (xid <= last_committed_xid) {
            // Flush to runfile
            auto write_response = extent->async_flush(runhandle);
            write_response.wait();
        }

        // Move to the next extent
        start_offset = response->next_offset;
        response = handle->read(start_offset);
    }

    // Move runfile on to the global file
    if (std::filesystem::exists(global_vacuum_runfile)) {
        std::filesystem::rename(global_vacuum_runfile, _global_vacuum_file);
    }
}

void
Vacuumer::_run_recovery()
{
    /* ----------------------------------------------------------------------
     * Various failure points and recovery noted below:
     *
     * State A: Empty state:
     *   Global: Empty
     *   Partials: Empty
     * Recovery:
     *   None
     *
     * State B: Partial run 1:
     *   Global: Not-empty, Runfile present
     *   Partials: No partials runfile
     * Recovery:
     *   Move global runfile to global file
     *   Truncate global records with XID > committed_xid
     *
     * State C: Partial run 2:
     *   Global: Not-empty, No runfile
     *   Partials: partials runfile present
     * Recovery:
     *   Remove partials runfile
     *   Truncate global records with XID > committed_xid
     *
     * State D: Start/End of run:
     *   Global: Not-empty, no runfile present
     *   Partials: Not-empty, no runfile present
     * Recovery:
     *   Truncate global records with XID > committed_xid
    ----------------------------------------------------------------------*/

    std::unique_lock lock(_mutex);
    LOG_INFO("Vacuum Recovery started");

    auto global_runfile_exists = false;
    std::filesystem::path global_vacuum_runfile;
    auto global_file_exists = std::filesystem::exists(_global_vacuum_file);

    for (const auto& entry : std::filesystem::directory_iterator(_vacuum_data_base)) {
        if (entry.is_regular_file()) {
            const std::string filename = entry.path().filename().string();
            if (filename.ends_with(VacuumConfig::GLOBAL_RUNFILE_SUFFIX)) {
                global_runfile_exists = true;
                global_vacuum_runfile = entry;
                break;
            }
        }
    }

    // State B
    if (global_runfile_exists) {
        std::filesystem::rename(global_vacuum_runfile, _global_vacuum_file);
    }

    // State C: Remove any partials runfile
    for (const auto& entry : std::filesystem::directory_iterator(_vacuum_data_base)) {
        if (entry.is_regular_file()) {
            const std::string filename = entry.path().filename().string();
            if (filename.ends_with(VacuumConfig::PARTIAL_RUNFILE_SUFFIX)) {
                LOG_INFO("Removing {}", entry.path());
                std::filesystem::remove(entry.path());
            }
        }
    }

    //State: B / C / D: Recover global vacuum file upto committed_xid
    if (global_file_exists) {
        _recover_global_vacuum_file();
    }

    LOG_INFO("Vacuum Recovery completed");
}

void
Vacuumer::run_vacuum_once()
{
    _do_vacuum_run();
}

void
Vacuumer::_do_vacuum_run()
{
    // lock while accessing the maps
    std::unique_lock lock(_mutex);

    ExtentMap expired_extents_map;
    SnapshotMap expired_snapshots_map;

    // Lets read from global vacuum file if no expired extents is memory (that got written to the global file)

    if (fs::get_file_size(_global_vacuum_file) > _global_file_size_threshold) {
        LOG_INFO("Running vacuum as the global log crossed threshold: {}", _global_vacuum_file);

        /* --------------------------------- Populate maps from global vacuum log -------------------------- */
        auto handle = IOMgr::get_instance()->open(_global_vacuum_file, IOMgr::IO_MODE::READ, true);
        int start_offset = 0;
        auto response = handle->read(start_offset);

        auto file_f = _global_vacuum_schema->get_field("file");
        auto offset_f = _global_vacuum_schema->get_field("offset");
        auto size_f = _global_vacuum_schema->get_field("size");
        auto file_dropped_f = _global_vacuum_schema->get_field("file_dropped");

        while (!response->data.empty()) {
            auto extent = std::make_shared<Extent>(response->data);
            for (auto &row : *extent) {
                auto file_dropped = file_dropped_f->get_bool(&row);
                auto file = file_f->get_text(&row);
                auto xid = extent->header().xid;
                // Add to snapshot_map to process outside hole-punch if the file is dropped,
                // otherwise add to extents map
                if (file_dropped) {
                    auto db_id = _get_db_id_from_path(file);
                    expired_snapshots_map[db_id][xid].emplace_back(file);
                } else {
                    auto offset = offset_f->get_uint64(&row);
                    auto size = size_f->get_uint64(&row);
                    expired_extents_map[file][xid].emplace_back(offset, size);
                }
            }
            start_offset = response->next_offset;
            response = handle->read(start_offset);
        }
        /* ----------------------------------End of reading global vacuum log ------------------------------ */

        /* --------------------------------- Hole punch flow ----------------------------------------------- */
        for (auto file_it = expired_extents_map.begin(); file_it != expired_extents_map.end(); ) {
            const std::string& file = file_it->first;
            auto& xid_map = file_it->second;
            auto db_id = _get_db_id_from_path(file);
            auto cutoff_xid = _get_vacuum_cutoff_xid(db_id);  // Get safest XID to vacuum till that point

            IntervalTree<uint64_t> itree;
            std::vector<uint64_t> xids_to_process;

            // Step 1: Add leftover partials from previous runs
            auto partial_extents = _get_partials_from_file(file);
            for (const auto& hole_info: partial_extents) {
                itree.insert(hole_info.offset, hole_info.offset + hole_info.size);
            }

            // Step 2: Collect all extents from xids < cutoff_xid
            for (const auto& [xid, extents] : xid_map) {
                if (xid >= cutoff_xid) {
                    break;
                }
                for (const auto& [offset, size] : extents) {
                    itree.insert(offset, offset + size);
                }
                xids_to_process.push_back(xid);
            }

            if (itree.empty()) {
                ++file_it;
                continue;
            }

            // Step 3: Identify punchable and leftover unaligned regions
            std::vector<Vacuumer::HoleInfo> punchable;
            std::vector<Vacuumer::HoleInfo> new_partials;

            for (const auto& [start, end] : itree.to_vector()) {
                uint64_t aligned_start = align_up(start, _hole_punch_block_size);
                uint64_t aligned_end = align_down(end, _hole_punch_block_size);

                if (aligned_start < aligned_end) {
                    punchable.emplace_back(aligned_start, aligned_end - aligned_start);
                }
                if (start < aligned_start) {
                    new_partials.emplace_back(start, aligned_start - start);
                }
                if (aligned_end < end) {
                    new_partials.emplace_back(aligned_end, end - aligned_end);
                }
            }

            // Step 4: Punch and detect failures
            auto not_punched_extents = hole_punch_file(file, punchable);

            // Insert failed extents back to the partials
            // so that it will be attempted in the next run
            new_partials.insert(new_partials.end(),
                    not_punched_extents.begin(),
                    not_punched_extents.end());

            // Step 5: Clean up processed XIDs
            for (uint64_t xid : xids_to_process) {
                xid_map.erase(xid);
            }

            // Step 6: Update _partial_extents with leftovers
            _update_vacuumed_partials_file(file, new_partials);

            // Step 7: Clean up file entry if all xids are processed
            if (xid_map.empty()) {
                file_it = expired_extents_map.erase(file_it);
            } else {
                ++file_it;
            }
        }
        /* -------------- End of Hole punch flow ------------------------------------------------------------*/

        /* --------------- Snapshot deletion flow -----------------------------------------------------------*/
        // expire snapshots through the min XID
        for (auto db_it = expired_snapshots_map.begin(); db_it != expired_snapshots_map.end(); ) {
            uint64_t cutoff_xid = _get_vacuum_cutoff_xid(db_it->first);  // Get safest XID to vacuum till that point
            auto& xid_map = db_it->second;

            for (auto xid_it = xid_map.begin(); xid_it != xid_map.end(); ) {
                // if everything to expire still in-use, stop
                if (xid_it->first >= cutoff_xid) {
                    break;
                }
                // retrieve a list of snapshots to expire
                SnapshotList& dirs = xid_it->second;

                bool all_deleted = true;

                // clear the table directories associated with the expired snapshots
                for (const auto& dir : dirs) {
                    std::error_code ec;

                    if (!std::filesystem::exists(dir, ec)) {
                        continue;  // skip missing dir
                    }

                    // recursively delete
                    bool is_directory = std::filesystem::is_directory(dir);
                    std::filesystem::remove_all(dir, ec);
                    if (ec) {
                        LOG_ERROR("remove_all() failed: {}", ec.message());
                        all_deleted = false;
                    } else {
                        _cleanup_partial_files(dir, is_directory);
                    }
                }

                if (all_deleted) {
                    dirs.clear();
                    xid_it = xid_map.erase(xid_it);
                } else {
                    ++xid_it;  // skip erasing; keep the entry for next attempt
                }
            }

            // If the db entry is now empty, erase it; otherwise advance.
            if (xid_map.empty()) {
                db_it = expired_snapshots_map.erase(db_it);
            } else {
                ++db_it;
            }
        }
        /*------------- End of snapshot deletion flow ---------------------------------------------------*/


        /*------------- Rotate Global vacuum log -------------------------------------------------------- */
        // Truncate if everything is processed,
        // otherwise overwritted global log with remaining records
        if (expired_extents_map.empty() && expired_snapshots_map.empty()) {
            fs::truncate_file(_global_vacuum_file, 0);
        } else {
            _update_global_vacuum_file(expired_extents_map, expired_snapshots_map);
        }
        /*------------- End of log rotation ------------------------------------------------------------- */

        LOG_INFO("Vacuum completed");
    }
    lock.unlock();
}

void
Vacuumer::_internal_run()
{
    // Run recovery for first time
    _run_recovery();

    while(!_shutdown) {
        // sleep for 1 second before trying to expire more data
        std::this_thread::sleep_for(std::chrono::seconds(1));

        if (!_vacuum_run_enabled) {
            // Vacuum turned off for run
            continue;
        }

        // Run vacuum once
        _do_vacuum_run();

    }
}

} // namespace springtail
