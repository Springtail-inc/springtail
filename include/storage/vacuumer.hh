#pragma once

#include <condition_variable>
#include <list>
#include <map>
#include <set>
#include <thread>

#include <common/filesystem.hh>
#include <common/json.hh>
#include <common/redis_types.hh>
#include <common/properties.hh>
#include <common/singleton.hh>

#include <redis/redis_ddl.hh>
#include <storage/cache.hh>
#include <storage/extent.hh>
#include <storage/field.hh>
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

        /** architecture.md "btree" redesign Sec 7.1 / Decisions log entry 6: background
            space-reclaim compaction's dead-byte-*fraction* candidacy threshold (a segment with
            this fraction or more of its on-disk bytes already superseded, but not yet fully
            dead, is worth compacting) and the per-cycle extent-copy budget (phase-7.md's
            Throttling task -- small batches, never one large pass). Both tuning decisions, not
            measured values, following the same "pick a sane default, expose as config" pattern
            segment_seal_threshold_bytes used (phase 6). */
        constexpr double COMPACTION_DEAD_FRACTION_THRESHOLD = 0.5;
        constexpr uint64_t COMPACTION_ITEM_BUDGET = 32;

        /** architecture.md "btree" redesign Sec 7.1 / phase-8.md OQ5: background structural-
            reclaim (merge) occupancy threshold and per-cycle item budget. In the same
            storage.vacuum_config namespace as compaction's own knobs above (consumed by
            BtreeWriteStrategy via Vacuumer::underfull_occupancy_fraction(), not read directly
            by the strategy -- same indirection max_extent_size itself already uses). The budget
            default is deliberately smaller than COMPACTION_ITEM_BUDGET: unlike compaction's
            phase-1 bulk copy (a cheap sequential scan), each merge item does a fresh root-to-
            node descent *plus* a sibling lookup *plus* the combine itself, so a smaller per-
            cycle cap keeps one cycle's wall-clock cost comparable despite the larger per-item
            cost. Both tuning decisions, not measured values (same posture as every other
            VacuumConfig default). */
        constexpr double UNDERFULL_OCCUPANCY_FRACTION = 0.25;
        constexpr uint64_t MERGE_ITEM_BUDGET = 8;

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
 * Vacuumer to clear dead extents and table snapshots. Since phase 7 (architecture.md "btree"
 * redesign Sec 7.1), also tracks background space-reclaim compaction candidacy for new-engine
 * sealed segments (dead-byte fraction, re-derived fresh every run alongside the existing
 * whole-file-unlink dead-byte sum, same crash-consistency posture) -- see
 * get_compaction_candidates()/compaction_dead_offsets()/compaction_item_budget(). Deliberately
 * stays schema/tree-shape-unaware doing so, same as every other responsibility here: it reports
 * *which* segments and offsets, never decodes or moves bytes itself -- that's
 * IndexWriter::bulk_copy_for_compaction()/apply_compaction_batch()'s job, orchestrated by
 * MutableTable::run_compaction_cycle().
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
     * @brief Mark `file` as a sealed (never written to again) segment of a new-engine tree
     *        (architecture.md "btree" redesign Sec 7, phase-6's SegmentSet), eligible for
     *        whole-file reclamation once nothing in it is reachable, instead of the legacy
     *        hole-punch path's "punch holes in an ever-growing file, never delete it" model.
     *        Idempotent: safe to call again (e.g. every time a later generation's writer
     *        resumes into an already-sealed segment) with no effect beyond re-affirming
     *        membership -- "sealed" is otherwise never persisted anywhere else (see
     *        SegmentSet's class comment).
     * @param file The sealed segment file's path.
     */
    void register_sealed_segment(const std::filesystem::path &file);

    /**
     * @brief Segments currently judged partially-dead-enough to warrant background
     *        space-reclaim compaction (architecture.md "btree" redesign Sec 7.1): dead-byte
     *        fraction (computed against the reclaim-safe cutoff, same as the whole-file-unlink
     *        decision just above it in _do_vacuum_run()) at or above
     *        compaction_dead_fraction_threshold, but not yet fully dead (a fully-dead segment
     *        is unlinked outright and never appears here). Re-derived fresh every vacuum run
     *        from the same global vacuum log the whole-file-unlink decision already reads --
     *        no separate persisted counter, for the same crash-consistency reason documented at
     *        the call site -- so this is a point-in-time snapshot, not a durable queue: a
     *        segment a caller doesn't compact away this call will simply be offered again (or
     *        not, if it changed) on a later call. Deliberately unbounded (not capped to a small
     *        "top N"): the one singleton Vacuumer instance is shared across every table/db in
     *        the process (and, in tests, across the whole test binary's lifetime) and each
     *        caller only ever acts on the single candidate relevant to the tree it owns, so
     *        capping here would just risk an unrelated tree's candidate crowding out this one's.
     * @return Every currently-candidate segment's path.
     */
    std::vector<std::filesystem::path> get_compaction_candidates();

    /**
     * @brief The set of in-segment byte offsets within `file` already known (as of the most
     *        recent vacuum run) to be superseded -- i.e. no longer reachable from this tree's
     *        *current* structure, regardless of whether they're yet safe to physically reclaim
     *        (architecture.md Sec 4: superseded is superseded the moment a newer version
     *        exists; reclaim-safety is a separate, later question). Used by a tree's own
     *        phase-1 bulk copy (IndexWriter::bulk_copy_for_compaction()) to skip extents that
     *        are already dead rather than copying them into the dense replacement -- a stale
     *        (slightly out-of-date) snapshot only ever errs toward *not* skipping something
     *        that became dead moments ago, never the reverse, so this is safe to use without
     *        re-deriving it per call.
     * @param file A segment path previously returned by get_compaction_candidates().
     * @return The known-dead offsets, or an empty set if `file` isn't a tracked candidate.
     */
    std::set<uint64_t> compaction_dead_offsets(const std::filesystem::path &file);

    /**
     * @brief The configured per-cycle extent-copy budget for background space-reclaim
     *        compaction's phase-1 bulk copy (phase-7.md's Throttling task) -- see
     *        VacuumConfig::COMPACTION_ITEM_BUDGET for the default and
     *        "storage.vacuum_config.compaction_item_budget" for the config knob.
     */
    uint64_t compaction_item_budget() const { return _compaction_item_budget; }

    /**
     * @brief One underfull-node flag raised by the delete path (architecture.md "btree"
     *        redesign Sec 7.1, phase 8: background structural-reclaim/merge). Mirrors
     *        IndexWriter::CompactionItem's shape exactly, for the same reason CompactionItem
     *        has that shape: a real id plus a search key is enough for a later, independent
     *        lookup to re-find and re-validate a specific node, and nothing about storing that
     *        pair needs tree-shape knowledge -- only *consuming* one (ask the parent for a
     *        sibling) does, which is why consumption lives in IndexWriter/MutableTable, not
     *        here (see phases/phase-8.md's "Design notes" for the full reasoning).
     */
    struct UnderfullFlag {
        uint64_t old_id;
        ValueTuplePtr search_key;
    };

    /**
     * @brief Raise a "node at old_id is underfull" flag for `tree_file` (architecture.md Sec
     *        7.1, phase 8). Called inline by IndexWriter::_flush_node() the moment a non-root,
     *        non-empty node it flushes is below the strategy's occupancy threshold -- never
     *        called directly by anything else. In-memory only, deliberately *not* given
     *        _compaction_candidates's recompute-from-log crash-consistency treatment: losing a
     *        flag (process restart) only delays a merge that would otherwise eventually have
     *        happened, it never under-protects anything irreversible the way losing a dead-byte
     *        count could (that gates an actual whole-file delete) -- see phases/phase-8.md's
     *        Design notes for the full argument.
     * @param tree_file The tree's own base file path (IndexWriter::get_file_path()), *not* a
     *        segment path -- a flagged node's real id can land in any segment this tree ever
     *        writes to over its lifetime, so flags are grouped per tree, not per segment.
     */
    void raise_underfull_flag(const std::filesystem::path &tree_file, uint64_t old_id,
                              ValueTuplePtr search_key);

    /**
     * @brief Every underfull flag currently raised for `tree_file`, in raise order. Read-only --
     *        does not retire anything (see clear_underfull_flag()). Call once per merge cycle,
     *        not once per candidate node (mirrors compaction_dead_offsets()'s own usage
     *        contract -- round-1 review finding 2 is the precedent for why that distinction
     *        matters: this is the same process-wide-singleton mutex the vacuum thread itself
     *        takes).
     * @param tree_file A tree's base file path, as passed to raise_underfull_flag().
     * @return The currently-raised flags, or empty if none.
     */
    std::vector<UnderfullFlag> get_underfull_flags(const std::filesystem::path &tree_file);

    /**
     * @brief Retire exactly the one flag matching `old_id` for `tree_file` -- never a broader
     *        clear (e.g. "every flag for this tree"), mirroring the lesson that a tracker's
     *        retirement path needs the same ownership gating as its claim path. Called by the
     *        background merge consumer once it has resolved `old_id`'s flag, whatever the
     *        outcome (merged, found stale, or no sibling had room) -- all three are terminal
     *        for that one flag; a no-op if already cleared or never raised.
     */
    void clear_underfull_flag(const std::filesystem::path &tree_file, uint64_t old_id);

    /**
     * @brief The configured occupancy threshold (as a fraction of a tree's max extent size)
     *        below which background structural-reclaim (merge) considers a node underfull --
     *        see VacuumConfig::UNDERFULL_OCCUPANCY_FRACTION for the default and
     *        "storage.vacuum_config.underfull_occupancy_fraction" for the config knob.
     */
    double underfull_occupancy_fraction() const { return _underfull_occupancy_fraction; }

    /**
     * @brief The configured per-cycle item budget for background structural-reclaim (merge) --
     *        see VacuumConfig::MERGE_ITEM_BUDGET for the default and
     *        "storage.vacuum_config.merge_item_budget" for the config knob.
     */
    uint64_t merge_item_budget() const { return _merge_item_budget; }

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

    /**
     * @brief Get vacuum-safe XID for a DB
     *
     * @param db_id Database ID
     * @return XID until which vacuum can run
     */
    uint64_t get_vacuum_cutoff_xid(uint64_t db_id);

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

    /**
     * New-engine sealed segment files (SegmentSet), registered via register_sealed_segment().
     * Gates which files in _extent_map are eligible for whole-file reclamation (phase 6) rather
     * than the legacy hole-punch path -- membership here is the one signal Vacuumer has for
     * "this is a segment, never hole-punch it," since nothing about a bare file path alone
     * distinguishes a sealed segment from a legacy ever-growing file.
     */
    std::set<std::filesystem::path> _sealed_segments;

    /**
     * Background space-reclaim compaction candidacy (architecture.md "btree" redesign Sec 7.1):
     * segment path -> the dead-offset snapshot computed for it the last time _do_vacuum_run()
     * judged its dead-byte fraction at or above _compaction_dead_fraction_threshold. See
     * get_compaction_candidates()/compaction_dead_offsets()'s doc comments for why this is a
     * recomputed-fresh-every-run snapshot, not a durable queue.
     */
    std::map<std::filesystem::path, std::set<uint64_t>> _compaction_candidates;

    /** architecture.md Sec 7.1 dead-byte-*fraction* candidacy threshold -- see
        VacuumConfig::COMPACTION_DEAD_FRACTION_THRESHOLD. */
    double _compaction_dead_fraction_threshold = VacuumConfig::COMPACTION_DEAD_FRACTION_THRESHOLD;

    /** Per-cycle extent-copy budget -- see VacuumConfig::COMPACTION_ITEM_BUDGET. */
    uint64_t _compaction_item_budget = VacuumConfig::COMPACTION_ITEM_BUDGET;

    /**
     * Background structural-reclaim (merge) flags (architecture.md "btree" redesign Sec 7.1,
     * phase 8): tree base file path -> every currently-raised UnderfullFlag for that tree, in
     * raise order. Unlike _compaction_candidates (a point-in-time snapshot recomputed fresh
     * every vacuum run), these are discrete events raised inline by IndexWriter::_flush_node()
     * and explicitly claimed/retired one at a time by clear_underfull_flag() -- see that
     * method's doc comment. Guarded by `_mutex`, same as every other map here; raise/get/clear
     * are all plain, O(1)-ish bookkeeping -- no sibling lookup or merge ever runs while holding
     * it.
     */
    std::map<std::filesystem::path, std::vector<UnderfullFlag>> _underfull_flags;

    /** architecture.md Sec 7.1 occupancy threshold -- see
        VacuumConfig::UNDERFULL_OCCUPANCY_FRACTION. */
    double _underfull_occupancy_fraction = VacuumConfig::UNDERFULL_OCCUPANCY_FRACTION;

    /** Per-cycle merge-item budget -- see VacuumConfig::MERGE_ITEM_BUDGET. */
    uint64_t _merge_item_budget = VacuumConfig::MERGE_ITEM_BUDGET;

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
