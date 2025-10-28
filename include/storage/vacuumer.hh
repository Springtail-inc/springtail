#pragma once

#include <condition_variable>
#include <list>
#include <map>
#include <thread>

#include <common/filesystem.hh>
#include <common/json.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>
#include <common/singleton.hh>

#include <redis/redis_ddl.hh>
#include <storage/cache.hh>
#include <storage/extent.hh>
#include <storage/io_mgr.hh>
#include <storage/schema.hh>

namespace springtail {

/**
 * Vacuum config defaults
 */
    namespace VacuumConfig {
        constexpr uint64_t HOLE_PUNCH_BLOCK_SIZE = 4 * 1024;
        constexpr uint64_t GLOBAL_FILE_SIZE_THRESHOLD = 20 * 1024;
        constexpr uint64_t MAX_ENTRIES_IN_MEMORY = 10000;

        constexpr char GLOBAL_FILE_SUFFIX[] = ".global.vcm";
        constexpr char GLOBAL_RUNFILE_SUFFIX[] = ".global.vcm.run";

        constexpr char PARTIAL_FILE_SUFFIX[] = "_partials.vcm";
        constexpr char PARTIAL_RUNFILE_SUFFIX[] = "_partials.vcm.run";
    }

/**
 * @brief This class is used for getting information about the Vacuumer
 *      without having to create a Vacuumer object.
 *
 */
class VacuumerUtils {
public:
    /**
     * @brief Construct a VacuumerUtils object
     *
     */
    VacuumerUtils() {
        // Initialize redis hash to save cutoff XIDs
        uint64_t db_instance_id = Properties::get_db_instance_id();
        _vacuum_cutoff_xid_redis_hash = fmt::format(redis::VACUUM_CUTOFF_XID, db_instance_id);

        // get the configs for vacuum - size threshold, block size and base dir
        nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);

        nlohmann::json vacuum_config_json;
        Json::get_to<nlohmann::json>(json, "vacuum_config", vacuum_config_json);

        // Vacuum enabled - true/false
        Json::get_to<bool>(vacuum_config_json, "enabled", _vacuum_start_enabled);
    }

    /**
     * @brief Return whether the vacuumer is enabled
     *
     * @return true - enabled
     * @return false - disabled
     */
    bool is_enabled() const { return _vacuum_start_enabled; }

    /**
     * @brief Get last seen cutoff XID for the DB
     *
     * @param db_id Database ID
     * @return cutoff_xid for the DB, or 0 if nothing found
     */
    uint64_t
    get_last_seen_cutoff_xid(uint64_t db_id)
    {
        RedisClientPtr client = RedisMgr::get_instance()->get_client();
        auto val = client->hget(_vacuum_cutoff_xid_redis_hash, std::to_string(db_id));
        if (val) {
            return std::stoull(*val);
        } else {
            return 0;
        }
    }

private:
    bool _vacuum_start_enabled{false};          ///< Flag indicating if the vacuumer is enabled
    std::string _vacuum_cutoff_xid_redis_hash;  ///< Name of the redis hash holding last seen vacuum cutoff XIDs

};
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
     * @brief Persist expired extents upto committed_xid
     *
     * @param db_id Database ID
     * @param committed_xid XID upto which expired extents will
     *        be written to the disk
     */
    void commit_expired_extents(uint64_t db_id, uint64_t committed_xid);

    /**
     * @brief Enable vacuumer to collect extents/snapshosts to be vacuumed
     */
    void enable_tracking_extents() {
        _extents_tracking_enabled = true;
    }

    /**
     * @brief Disable vacuumer to collect extents/snapshosts to be vacuumed
     */
    void disable_tracking_extents() {
        _extents_tracking_enabled = false;
    }

    /**
     * @brief Run vacuum once, primarily called from test
     */
    void run_vacuum_once();

    /**
     * @brief Set global threshold for vacuum to process
     *        used mainly from the tests
     *
     * @param size Global vacuum file size threshold
     */
    void set_global_vacuum_threshold(uint64_t size) {
        _global_file_size_threshold = size;
    }

    /**
     * @brief Cleanup DB's entries from vacuum storage
     *        - memory, global vacuum file and partials
     *
     * @param cleanup_db_id DB to be cleaned up
     */
    void cleanup_db(uint64_t cleanup_db_id);

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
     * @brief Graceful shutdown of vacuumer thread
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
     * Enum to indicate the type of cleanup on the global vacuum file
     */
    enum class CleanupOperation { DB_CLEANUP, RECOVERY };

    /**
     * Hole-punch block size
     */
    uint64_t _hole_punch_block_size;

    /**
     * Global vacuum file threshold above which vacuum runs
     */
    uint64_t _global_file_size_threshold;

    /**
     * Max entries in memory to be held before triggering flush to disk
     * Ideally, on every commit, entries will be flushed upto the committed XID
     */
    uint64_t _max_entries_in_memory;

    /**
     * Flag to start vacuum while booting
     */
    bool _vacuum_start_enabled = false;

    /**
     * Flag to control extents/snapshots tracking
     */
    bool _extents_tracking_enabled = true;

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
    ExtentMap _extent_map; ///< Maps File -> XID -> list of expired extent
    SnapshotMap _snapshot_map; ///< Maps XID -> list of table snapshot directories
    std::condition_variable _cv; ///< Condition variable to wake up the vacuumer thread

    /**
     * Schema to create global and partial vacuum files
     */
    ExtentSchemaPtr _global_vacuum_schema;
    ExtentSchemaPtr _vacuum_file_schema;

    std::filesystem::path _vacuum_data_base; ///< The base directory for vacuum directories
    std::filesystem::path _global_vacuum_file; ///< Global vacuum file
    std::filesystem::path _global_vacuum_runfile; ///< Global vacuum file for current run
    std::string _vacuum_cutoff_xid_redis_hash;    ///< name of the redis hash holding last seen vacuum cutoff XIDs

    std::atomic<int64_t> _entries_count_in_memory = 0;  ///< To track count of entries in the memory

    /**
     * @brief Vacuumer thread shutdown
     */
    void _internal_thread_shutdown() override;

    /**
     * @brief Round a value up to the nearest multiple of `align`
     */
    inline uint64_t _align_up(uint64_t val, uint64_t align) {
        return ((val + align - 1) / align) * align;
    }

    /**
     * @brief Round a value down to the nearest multiple of `align`
     */
    inline uint64_t _align_down(uint64_t val, uint64_t align) {
        return (val / align) * align;
    }

    /**
     * @brief Perform actual hole punching using fallocate.
     *
     * @param file File to punch.
     * @param input_extents Aligned extents to punch
     * @return List of extents which is not punched
     */
    std::vector<HoleInfo> _hole_punch_file(const std::string& file,
                                           const std::vector<HoleInfo>& input_extents);

    /**
     * @brief Get vacuum-safe XID for a DB
     *
     * @param db_id Database ID
     * @return XID until which vacuum can run
     */
    uint64_t _get_vacuum_cutoff_xid(uint64_t db_id);

    /**
     * @brief Returns db_id following a keyword in a filesystem path.
     *
     * Searches for the given keyword (default: "table") in the path and returns
     * the next component as a uint64_t. Returns UINT64_MAX if not found or invalid.
     *
     * @param path     Filesystem path to search.
     * @param keyword  Keyword to look for (default is "table").
     * @return db_id after the keyword, or UINT64_MAX on failure.
     */
    uint64_t _get_db_id_from_path(const std::filesystem::path& path,
                                 const std::string& keyword = "table");
    /**
     * @brief Generate file name using the input file name and parent dir
     *
     * @param Input file name
     * @return File name as: "input file's parent_dir _ filename"
     */
    std::string _generate_flat_filename(const std::filesystem::path& filepath);

    /**
     * @brief Update hole partials to a file hashed by the given file path
     *
     * @param file     partials belong to this file
     * @param partials Hole partials
     */
    void _update_vacuumed_partials_file(const std::filesystem::path &file,
                std::vector<HoleInfo> partials);

    /**
     * @brief Get hold partials from the file hashed by the given file path
     *
     * @param file     partials belong to this file
     * @return Partials vector
     */
    std::vector<HoleInfo> _get_partials_from_file(const std::filesystem::path &file);

    /**
     * @brief Overwrites global vacuum file with the given extents map
     *
     * @param expired_extents_map   Expired extents map to be written
     * @param expired_snapshots_map Expired snapshots map to be written
     */
    void _update_global_vacuum_file(ExtentMap& expired_extents_map, SnapshotMap& expired_snapshots_map);

    /**
     * @brief Create empty extent with xid in the header
     *
     * @param xid XID to be added in the extent header
     * @return shared_ptr<Extent>
     */
    std::shared_ptr<Extent> _create_empty_extent_with_header(uint64_t xid);

    /*
     * @brief Create extent and populate hole list
     *
     * @param xid       XID to be added in the extent header
     * @param file      File for which extents to be hole-punched
     * @param hole_list extents to be hole-punched, to be added in the extent
     *
     * @return shared_ptr<Extent>
     */
    std::shared_ptr<Extent> _create_extent_using_hole_list(uint64_t xid, const std::filesystem::path& file, std::vector<HoleInfo> hole_list);

    /*
     * @brief Create extent and populate snapshot deletion list
     *
     * @param xid           XID to be added in the extent header
     * @param snapshot_list snpshot deletion list to be added to the extent
     * @param extent        Extent if present, will be used to add the snapshot details
     *
     * @return shared_ptr<Extent>
     */
    std::shared_ptr<Extent> _upsert_extent_using_snapshot_list(uint64_t xid, SnapshotList snapshot_list, std::shared_ptr<Extent> extent=nullptr);

    /**
     * @brief Recovery - to restore state to the last committed XID
     */
    void _run_recovery();

    /**
     * @brief Recover global vacuum file upto last committed XID
     */
    void _recover_global_vacuum_file();

    /**
     * @brief Run vacuum once
     */
    void _do_vacuum_run();

    /**
     * @brief Cleanup partial files when the source files are dropped
     * @param path         Path for which partials to be dropped
     * @param is_directory Indicates if the path was dir or file
     *                     (since this method will be called after deleting the path)
     */
    void _cleanup_partial_files(const std::filesystem::path &path, bool is_directory);

    /**
     * @brief Flush all the expired entries in the memory to global vacuum file
     */
    void _flush_all_expired_entries();

    /**
     * @brief Persist expired extents upto committed_xid
     *
     * @param db_id Database ID
     * @param committed_xid XID upto which expired extents will
     *        be written to the disk
     */
    void _commit_expired_extents(uint64_t db_id, uint64_t committed_xid);

    /**
     * @brief Cleans up global vacuum file - db_cleanup or recovery till last committed XID
     *
     * @param cleanup_db_id ID to cleanup entries for db_cleanup
     */
    template <CleanupOperation op>
    void _cleanup_global_vacuum_file(uint64_t cleanup_db_id=-1);

    /**
     * @brief Save last seen cutoff xid in redis per db
     *
     * @param db_id      Database ID
     * @param cutoff_xid Cutoff XID
     */
    void _save_last_seen_cutoff_xid(uint64_t db_id, uint64_t cutoff_xid);

    /**
     * @brief Clean up expired roots files for system tables older than cutoff XID
     *
     * @param cutoff_xid XID below which roots files can be removed
     * @param table_dir  Directory containing the system table
     */
    void _cleanup_expired_roots_files(uint64_t cutoff_xid, const std::filesystem::path& table_dir);
};
}
