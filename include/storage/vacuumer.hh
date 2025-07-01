#pragma once

#include <filesystem>
#include <list>
#include <map>
#include <thread>

#include <common/singleton.hh>
#include <redis/redis_ddl.hh>

namespace springtail {

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
    using HoleList = std::map<uint64_t, std::list<HoleInfo>>;
    using ExtentMap = std::map<std::filesystem::path, HoleList>;

    using SnapshotList = std::list<std::filesystem::path>;
    using SnapshotMap = std::map<uint64_t, SnapshotList>;

    std::mutex _mutex; ///< Protects the internal maps
    ExtentMap _extent_map; ///< Maps XID -> File -> list of expired extent
    SnapshotMap _snapshot_map; ///< Maps XID -> list of table snapshot directories

    RedisDDL _redis_ddl; ///< Interface to the DDL structures in Redis.

    constexpr uint64_t kPunchAlign = 64 * 1024;  // 64KB punch alignment

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
    std::vector<HoleInfo> Vacuumer::hole_punch_file(const std::string& file,
                                                    const std::vector<HoleInfo>& input_extents);

    /**
     * @brief Get vacuum-safe XID for a DB
     *
     * @param file File to punch
     * @return XID until which extents can be punched
     */
    uint64_t get_vacuum_cutoff_xid(const std::string& file);
};

}
