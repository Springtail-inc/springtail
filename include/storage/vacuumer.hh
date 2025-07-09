#pragma once

#include <filesystem>
#include <list>
#include <map>
#include <thread>

#include <common/singleton.hh>
#include <redis/redis_ddl.hh>
#include <storage/schema.hh>
#include <common/properties.hh>
#include <common/json.hh>
#include <storage/io_mgr.hh>
#include <storage/cache.hh>
#include <storage/extent.hh>

namespace springtail {

constexpr uint64_t kPunchAlign = 4 * 1024;  // 64KB punch alignment

constexpr uint64_t VACUUM_THRESHOLD_SIZE = 20 * 1024;
/**
 * Vacuumer to clear dead extents and table snapshots.
 */
class Vacuumer final : public SingletonWithThread<Vacuumer> {
public:
    /**
     * Register an extent to be expired by the vacuumer.
     * @param file The file containing the extent
     * @param extent_id The offset of the extent
     * @param size The on-disk size of the extent data in the file
     * @param xid The XID at which the extent was overwritten
     */
    void expire_extent(const std::filesystem::path &file, uint64_t extent_id, uint32_t size, uint64_t xid);

    /**
     * Register a table snapshot to be expired by the vacuumer.
     * @param file The directory containing the snapshot
     * @param xid The XID at which the snapshot was replaced / dropped
     */
    void expire_snapshot(const std::filesystem::path &table_dir, uint64_t xid);

    void init();

    void commit_expired_extents();

    void _internal_shutdown();

protected:
    /**
     * The main loop of the vacuumer.
     */
    void _internal_run();


private:
    struct HoleInfo {
        uint64_t offset;
        uint64_t size;
    };
    using HoleList = std::map<uint64_t, std::vector<HoleInfo>>;
    using ExtentMap = std::map<std::filesystem::path, HoleList>;

    using SnapshotList = std::list<std::filesystem::path>;
    using SnapshotMap = std::map<uint64_t, SnapshotList>;

    std::mutex _mutex; ///< Protects the internal maps
    ExtentMap _extent_map; ///< Maps XID -> File -> list of expired extent
    SnapshotMap _snapshot_map; ///< Maps XID -> list of table snapshot directories
    ExtentSchemaPtr _global_vacuum_schema;
    ExtentSchemaPtr _vacuum_file_schema;
    std::filesystem::path _vacuum_data_base; ///< The base directory for vacuum directories
    std::filesystem::path _global_vacuum_file;

    RedisDDL _redis_ddl; ///< Interface to the DDL structures in Redis.

    std::thread _vacuumer_thread;           ///< Vacuumer thread

    std::atomic<bool> _shutdown{false};   ///< shutdown flag
    // Round a value up to the nearest multiple of `align`
    inline uint64_t align_up(uint64_t val, uint64_t align) {
        return ((val + align - 1) / align) * align;
    }

    // Round a value down to the nearest multiple of `align`
    inline uint64_t align_down(uint64_t val, uint64_t align) {
        return (val / align) * align;
    }

    /**
     * @brief Perform actual hole punching using fallocate.
     *
     * @param file File to punch.
     * @param input_extents Aligned extents to punch
     * @return List of successfully punched extents
     */
    std::vector<HoleInfo> hole_punch_file(const std::string& file,
                                          const std::vector<HoleInfo>& input_extents);

    /**
     * @brief Get vacuum-safe XID for a DB
     *
     * @param file File to punch
     * @return XID until which extents can be punched
     */
    uint64_t get_vacuum_cutoff_xid(const std::string& file);

    void _truncate_file(const std::filesystem::path &file, uint64_t offset);

    int64_t _get_file_size(const std::filesystem::path& path);

    void _update_vaccumed_partials_file(const std::filesystem::path &file,
                std::vector<HoleInfo> partials);

    std::vector<HoleInfo> _get_partials_from_file(const std::filesystem::path &file);
};

class VacuumerRunner : public ServiceRunner {
    public:
        VacuumerRunner() : ServiceRunner("Vacuumer") {}

        bool start() override
        {
            Vacuumer::get_instance()->init();
            return true;
        }

        void stop() override
        {
            Vacuumer::shutdown();
        }
};
}
