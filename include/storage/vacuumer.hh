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

/**
 * Vacuum config defaults
 */
constexpr uint64_t HOLE_PUNCH_BLOCK_SIZE = 4 * 1024;
constexpr uint64_t VACUUM_THRESHOLD_SIZE = 20 * 1024;

/**
 * Vacuumer to clear dead extents and table snapshots.
 */
class Vacuumer final : public Singleton<Vacuumer> {
        friend class Singleton<Vacuumer>;
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
     * @param db_id Database ID
     * @param file The directory containing the snapshot
     * @param xid The XID at which the snapshot was replaced / dropped
     */
    void expire_snapshot(uint64_t db_id, const std::filesystem::path &table_dir, uint64_t xid);

    /**
     * @brief Persist expired extents
     */
    void commit_expired_extents();


protected:
    /**
     * @brief Constructor, that inits the vacuumer thread
     */
    Vacuumer() : Singleton<Vacuumer>(ServiceId::VacuumerId)
    {
        _init();
    }

    /**
     * @brief Destroy the Vacuumer object
     */
    ~Vacuumer() override = default;

    /**
     * @brief Initializes vacuumer thread
     */
    void _init();

    /**
     * @brief Core runner of the vacuum that
     *        - hole-punches files,
     *        removes dropped table and index paths
     */
    void _internal_run() override;

    /**
     * @brief Graceful shutdown of vacummer thread
     */
    void _internal_shutdown() override;


private:
    /**
     * Holds an unit of extent - offset and size
     */
    struct HoleInfo {
        uint64_t offset;
        uint64_t size;
    };

    /**
     * Hole-punch block size
     */
    uint64_t _hole_punch_block_size;

    /**
     * Global vacuum file threshold above which vacuum runs
     */
    uint64_t _vacuum_global_threshold;

    /**
     * Expired extents map
     * file -> xid -> HoleInfo vector
     */
    using HoleList = std::map<uint64_t, std::vector<HoleInfo>>;
    using ExtentMap = std::map<std::filesystem::path, HoleList>;

    /**
     * Expired snapshots map
     * db_id -> xid -> list of filesystem paths
     */
    using SnapshotList = std::list<std::filesystem::path>;
    using SnapshotMap = std::map<uint64_t, std::map<uint64_t, SnapshotList>>;

    std::mutex _mutex; ///< Protects the internal maps
    ExtentMap _extent_map; ///< Maps XID -> File -> list of expired extent
    SnapshotMap _snapshot_map; ///< Maps XID -> list of table snapshot directories

    /**
     * Schema to create global and partial vacuum files
     */
    ExtentSchemaPtr _global_vacuum_schema;
    ExtentSchemaPtr _vacuum_file_schema;

    std::filesystem::path _vacuum_data_base; ///< The base directory for vacuum directories
    std::filesystem::path _global_vacuum_file; ///< Global vacuum file

    RedisDDL _redis_ddl; ///< Interface to the DDL structures in Redis.

    std::thread _vacuumer_thread;           ///< Vacuumer thread

    std::atomic<bool> _shutdown{false};   ///< shutdown flag
    /**
     * @brief Round a value up to the nearest multiple of `align`
     */
    inline uint64_t align_up(uint64_t val, uint64_t align) {
        return ((val + align - 1) / align) * align;
    }

    /**
     * @brief Round a value down to the nearest multiple of `align`
     */
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
    uint64_t _get_vacuum_cutoff_xid(const std::string& file);

    /**
     * @brief Truncates file from the given offset
     *
     * @param file File to truncate
     * @param offset Offset above which will be truncated
     */
    void _truncate_file(const std::filesystem::path &file, uint64_t offset);

    /**
     * @brief Get the size of the given file
     *
     * @param file File to get its size
     * @return File size
     */
    int64_t _get_file_size(const std::filesystem::path& path);

    /**
     * @brief Update hole partials to a file hashed by the given file path
     *
     * @param file     partials belong to this file
     * @param partials Hole partials
     */
    void _update_vacuumed_partials_file(const std::filesystem::path &file,
                std::vector<HoleInfo> partials);

    /**
     * brief Get hold partials from the file hashed by the given file path
     *
     * @param file     partials belong to this file
     * @return Partials vector
     */
    std::vector<HoleInfo> _get_partials_from_file(const std::filesystem::path &file);
};
}
