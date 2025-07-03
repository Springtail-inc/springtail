#pragma once

#include <shared_mutex>
#include <filesystem>

#include <common/init.hh>
#include <grpc/grpc_server_manager.hh>
#include <redis/redis_ddl.hh>
#include <xid_mgr/pg_xact_log_writer.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail::xid_mgr {

class XidMgrServer : public Singleton<XidMgrServer>
{
    friend class Singleton<XidMgrServer>;

public:
    /** SYNC interval */
    static constexpr int XIG_MGR_MIN_SYNC_MS = 500;

    /**
     * @brief commit up to and including given xid
     * @param db_id database id
     * @param xid xid to commit
     */
    void commit_xid(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes);

    /**
     * @brief Record a DDL change without doing a commit.  Used for table sync operations.
     * @param db_id database id
     * @param xid xid to commit
     */
    void record_mapping(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes);

    /**
     * @brief Get the latest committed xid object
     * @param db_id database id
     * @param schema_xid last known schema xid
     * @return uint64_t
     */
    uint64_t get_committed_xid(uint64_t db_id, uint64_t schema_xid);

    /**
     * @brief Cleanup xact logs with timestamp id less than given timestamp
     *
     * @param db_id database id
     * @param min_timestamp minimum timestamp id of xact logs
     */
    void cleanup(uint64_t db_id, uint64_t min_timestamp);

    /**
     * @brief Rotate database xact log to the new timestamp id
     *
     * @param db_id database id
     * @param timestamp timestamp id
     */
    void rotate(uint64_t db_id, uint64_t timestamp);

    /**
     * @brief Set clean up flag so that for cleaning up xid files.
     *
     */
    void cleanup_on_shutdown() { _cleanup_on_shutdown = true; }

private:
    XidMgrServer();
    ~XidMgrServer() override = default;

    std::unique_ptr<GrpcXidMgrService> _service;

    springtail::GrpcServerManager _grpc_server_manager;

    /** base path */
    std::filesystem::path _base_path;

    /**
     * @brief Class for keeping transaction log data and managing access to them
     *
     */
    class DBXactLogData {
    public:
        /**
         * @brief Construct a new DBXactLogData object
         *
         * @param db_id - database id
         * @param base_dir - parent directory of all transaction logs
         */
        DBXactLogData(uint64_t db_id, const std::filesystem::path &base_dir) :
                      _xact_log(base_dir / std::to_string(db_id)),
                      _db_id(db_id) {}

        /**
         * @brief Record mapping from pg_xid to xid with some attributes
         *
         * @param pg_xid - Postgres xid
         * @param xid - Springtail xid
         * @param has_schema_changes - this flag indicates if transaction has schema changes
         * @param real_commit - thi flag indicates if this is a real commit
         */
        void
        record_log_entry(uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit);

        /**
         * @brief Cleanup committed history of schema changes
         *
         * @param redis_ddl - redis DDL object
         */
        void
        cleanup_history_and_flush(RedisDDL &redis_ddl);

        /**
         * @brief Get the value of the last committed xid
         *
         * @param schema_xid - schema xid
         * @return uint64_t - last committed xid value
         */
        uint64_t
        get_committed_xid(uint64_t schema_xid);

        /**
         * @brief Start logging xids in a file with the new timestamp
         *
         * @param timestamp - file timestamp
         */
        void
        rotate(uint64_t timestamp);

        /**
         * @brief Cleanup files below the given timestamp
         *
         * @param min_timestamp - minimum allowed timestamp
         * @param archive_logs - archive logs flag
         */
        void
        cleanup(uint64_t min_timestamp, bool archive_logs);

    private:
        PgXactLogWriter _xact_log;              ///< log writer object
        std::shared_mutex _mutex;               ///< mutex for access control
        std::vector<uint64_t> _xact_history;    ///< schema changes xids
        uint64_t _db_id;                        ///< database id
        bool _dirty_history{false};             ///< dirty history flag
    };

    std::shared_mutex _mutex;                   ///< mutex for access control to transaction log data
    std::map<uint64_t, DBXactLogData> _xact_log_data;   ///< map of database id to transaction log data
    bool _archive_logs;                         ///< log archiving flag
    bool _cleanup_on_shutdown{false};           ///< clean up flag

    /**
     * @brief Start xid manager
     *
     */
     void _startup();

    /**
     * @brief Record new xid for given database
     *
     * @param db_id - database id
     * @param pg_xid - Postgress xid
     * @param xid - Springtail xid
     * @param has_schema_changes - schema changes flag
     * @param real_commit - real commit flag
     */
    void
    _record_xid_change(uint64_t db_id, uint32_t pg_xid, uint64_t xid,
                       bool has_schema_changes, bool real_commit);

    /**
     * @brief Find database for the give database idand if it is not there, add it
     *
     * @param db_id - database id
     * @param read_lock - read lock to be used for read access to transaction log data
     * @return std::map<uint64_t, DBXactLogData>::iterator - iterator to the map pair
     */
    std::map<uint64_t, DBXactLogData>::iterator
    _find_or_add(uint64_t db_id, std::shared_lock<std::shared_mutex> &read_lock);

    /**
     * @brief Instance shutdown function
     *
     */
    void _internal_shutdown() override;

    /**
     * @brief Run function for singleton thread
     *
     */
    void _internal_run() override;
};

}  // namespace springtail::xid_mgr
