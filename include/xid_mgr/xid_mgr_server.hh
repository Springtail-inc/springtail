#pragma once

#include <shared_mutex>
#include <filesystem>

#include <common/service_register.hh>
#include <common/singleton.hh>
#include <grpc/grpc_server_manager.hh>
#include <redis/redis_ddl.hh>
#include <xid_mgr/pg_xact_log_writer.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail::xid_mgr {

class XidMgrServer : public Singleton<XidMgrServer> {
    friend class Singleton<XidMgrServer>;

public:
    void startup();

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

    void cleanup(uint64_t db_id, uint64_t min_timestamp, bool archive_logs);
    void rotate(uint64_t db_id, uint64_t timestamp);

private:
    XidMgrServer();
    ~XidMgrServer() override = default;

    std::unique_ptr<GrpcXidMgrService> _service;

    springtail::GrpcServerManager _grpc_server_manager;

    /** base path */
    std::filesystem::path _base_path;

    class DBXactLogData {
    public:
        DBXactLogData(uint64_t db_id, const std::filesystem::path &base_dir):
                _xact_log(base_dir / std::to_string(db_id)),
                _db_id(db_id) {
            std::filesystem::create_directories(base_dir);
            LOG_INFO("Creating directory {} for db_id={}", base_dir, _db_id);
        }

        void
        record_mapping(uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit)
        {
            std::unique_lock lock(_mutex);
            _xact_log.log(pg_xid, xid, real_commit);
            if (has_schema_changes) {
                _xact_history.push_back(xid);
            }
        }

        void
        cleanup_history(RedisDDL redis_ddl)
        {
            std::unique_lock lock(_mutex);
            if (!_xact_history.empty()) {
                uint64_t min_schema_xid = redis_ddl.min_schema_xid(_db_id);
                auto it = std::ranges::lower_bound(_xact_history.begin(), _xact_history.end(), min_schema_xid);
                _xact_history.erase(_xact_history.begin(), it);
                if (_xact_history.empty()) {
                    LOG_DEBUG(LOG_XID_MGR, "The history for db_id={} is now empty", _db_id);
                } else {
                    LOG_DEBUG(LOG_XID_MGR, "The history for db_id={} now starts with xid={}", _db_id, _xact_history.front());
                }

            }
        }

        uint64_t
        get_committed_xid(uint64_t schema_xid)
        {
            std::shared_lock lock(_mutex);
            uint64_t last_xid = _xact_log.get_last_xid();

            // if schema XID is zero or there is no history for this database,
            // then we always return the most recent committed XID
            if (schema_xid == 0 || _xact_history.empty()) {
                LOG_DEBUG(LOG_XID_MGR, "Get committed xid for db_id={}: {}", _db_id, last_xid);
                return last_xid;
            }

            auto pos_i = std::ranges::upper_bound(_xact_history, schema_xid);
            if (pos_i == _xact_history.end()) {
                // if the schema XID is ahead of the history, return the most recent commited XID
                LOG_DEBUG(LOG_XID_MGR, "Get committed xid for db_id={}: {}", _db_id, last_xid);
                return last_xid;
            }

            // if the history is ahead of the commit, return the committed xid
            auto target_xid = (*pos_i) - 1;
            if (target_xid > last_xid) {
                LOG_DEBUG(LOG_XID_MGR, "Get committed xid for db_id={}: {}; ahead of history {}", _db_id, last_xid, target_xid);
                return last_xid;
            }

            // if we found an entry in the history, return the XID directly before that
            LOG_DEBUG(LOG_XID_MGR, "Get committed xid for db_id={}: xid limited by schema_xid: {} -> {}", _db_id, schema_xid, target_xid);
            return target_xid;
        }

        void
        rotate(uint64_t timestamp)
        {
            std::unique_lock lock(_mutex);
            _xact_log.rotate(timestamp);
        }

        void
        cleanup(uint64_t min_timestamp, bool archive_logs)
        {
            std::unique_lock lock(_mutex);
            _xact_log.cleanup(min_timestamp, archive_logs);
        }

    private:
        PgXactLogWriter _xact_log;
        std::shared_mutex _mutex;
        std::vector<uint64_t> _xact_history;
        uint64_t _db_id;
    };

    std::shared_mutex _mutex;
    std::map<uint64_t, DBXactLogData> _xact_log_data;

    void _record_xid_change(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit);
    std::pair<const uint64_t, DBXactLogData> *_find_or_add(uint64_t db_id, std::shared_lock<std::shared_mutex> &read_lock);

    void _internal_shutdown() override;
};

class XidMgrRunner : public ServiceRunner {
public:
    XidMgrRunner() :
        ServiceRunner("XidMgr") {}

    bool start() override {
        xid_mgr::XidMgrServer::get_instance()->startup();
        return true;
    }

    void stop() override {
        xid_mgr::XidMgrServer::shutdown();
    }
};

}  // namespace springtail::xid_mgr
