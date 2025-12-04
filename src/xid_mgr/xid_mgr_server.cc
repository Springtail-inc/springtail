#include <algorithm>

#include <common/common.hh>
#include <common/exception.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>


#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>
#include <sys_tbl_mgr/shm_cache.hh>

namespace springtail::xid_mgr {

XidMgrServer::XidMgrServer() : Singleton<XidMgrServer>(ServiceId::XidMgrServerId)
{
    nlohmann::json json = Properties::get(Properties::LOG_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the xid mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("XID Mgr RPC settings are not found");
    }

    std::string base_path;
    Json::get_to<std::string>(json, "transaction_log_path", base_path);
    _base_path = Properties::make_absolute_path(base_path);

    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "XidMgrServer: base_path: {}", _base_path.string());

    if (!std::filesystem::exists(_base_path)) {
        std::filesystem::create_directories(_base_path);
    }

    _grpc_server_manager.init(rpc_json);

    _service = std::make_unique<GrpcXidMgrService>(*this);
    _grpc_server_manager.addService(_service.get());

    _archive_logs = Json::get_or<bool>(json, "archive_logs", false);

    // Just for initialization, otherwise it might lock up a unit test because of a race condition
    // between shutdown sequence and XID manager thread instantiating RedisDDL for the first time.
    RedisDDL::get_instance();

    _startup();
}

void
XidMgrServer::_startup()
{
    start_thread();
    _grpc_server_manager.startup();
}

void
XidMgrServer::_internal_shutdown()
{
    _service->shutdown();
    _grpc_server_manager.shutdown();
    if (_cleanup_on_shutdown) {
        for (const auto &db_pair: _xact_log_data) {
            cleanup(db_pair.first, std::numeric_limits<uint64_t>::max());
        }
    }
    std::unique_lock lock(_mutex);
    _xact_log_data.clear();
}

void
XidMgrServer::_internal_run()
{
    while (!_is_shutting_down()) {
        // sleep for at least XIG_MGR_MIN_SYNC_MS
        std::this_thread::sleep_for(std::chrono::milliseconds(XIG_MGR_MIN_SYNC_MS));
        std::shared_lock lock(_mutex);
        for (auto &it: _xact_log_data) {
            it.second.cleanup_history_and_flush();
        }
    }
}

uint64_t
XidMgrServer::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
{
    auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    uint64_t xid = db_id_to_log_data->second.get_committed_xid(schema_xid);
    return xid;
}

void
XidMgrServer::commit_xid(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes,
        uint64_t timestamp, pg_log_mgr::WalProgressTrackerPtr tracker)
{
    _record_xid_change(db_id, pg_xid, xid, has_schema_changes, true, timestamp, tracker);
}

void
XidMgrServer::commit_xid_no_xlog(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit,
        uint64_t timestamp, pg_log_mgr::WalProgressTrackerPtr tracker, const std::vector<uint64_t>& table_ids)
{
    DCHECK(tracker);
    DCHECK(table_ids.empty() || !real_commit);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.record_log_entry(pg_xid, xid, has_schema_changes, real_commit, timestamp, tracker, false);
    if (!real_commit && table_ids.empty()) {
        // notify subscribers about not real xids only if there are table IDs
        return;
    }
    _service->notify_subscriber(db_id, xid, has_schema_changes, real_commit, table_ids);
}

void 
XidMgrServer::commit_xlog(uint64_t db_id, uint64_t xid)
{
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.write_log_entry(xid);
}

void
XidMgrServer::record_mapping(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes,
        uint64_t timestamp, pg_log_mgr::WalProgressTrackerPtr tracker, const std::vector<uint64_t>& table_ids)
{
    _record_xid_change(db_id, pg_xid, xid, has_schema_changes, false, timestamp, tracker);
    // notify subscribers about not real xid's  only if there are table IDs
    if (!table_ids.empty()) {
        _service->notify_subscriber(db_id, xid, has_schema_changes, false, table_ids);
    }
}

void
XidMgrServer::_record_xid_change(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit,
        uint64_t timestamp, pg_log_mgr::WalProgressTrackerPtr tracker)
{
    auto token = open_telemetry::OpenTelemetry::get_instance()->set_context_variables({
        {"db_id", std::to_string(db_id)},
        {"pg_xid", std::to_string(pg_xid)},
        {"xid", std::to_string(xid)}
    });
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.record_log_entry(pg_xid, xid, has_schema_changes, real_commit, timestamp, tracker, true);
    if (real_commit) {
        _service->notify_subscriber(db_id, xid, has_schema_changes, true);
    }
}

void
XidMgrServer::cleanup(uint64_t db_id, uint64_t min_timestamp)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Cleaning up database {} with min_timestamp {}", db_id, min_timestamp);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.cleanup(min_timestamp, _archive_logs);
}

void
XidMgrServer::cleanup(uint64_t db_id)
{
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Cleaning up database {}", db_id);
    std::unique_lock read_lock(_mutex);
    _xact_log_data.erase(db_id);

    // Remove database directory and everything inside it
    std::filesystem::path path = _base_path / std::to_string(db_id);
    fs::remove_dir(path);
}

std::map<uint64_t, XidMgrServer::DBXactLogData>::iterator
XidMgrServer::_find_or_add(uint64_t db_id, std::shared_lock<std::shared_mutex> &read_lock)
{
    auto it = _xact_log_data.find(db_id);
    if (it == _xact_log_data.end()) {
        read_lock.unlock();

        std::unique_lock write_lock(_mutex);
        // this will sanitize existing logs for recovery, if there are any records at the end with
        // real_commit set to false, it will remove those, so that the log can be written from
        // the last real commit
        uint64_t recovered_xid = PgXactLogWriter::set_last_xid_in_storage(_base_path / std::to_string(db_id), std::numeric_limits<uint64_t>::max(), _archive_logs);
        auto result = _xact_log_data.emplace(std::piecewise_construct, std::forward_as_tuple(db_id), std::forward_as_tuple(db_id, _base_path, recovered_xid));
        it = result.first;
        write_lock.unlock();

        read_lock.lock();
    }
    return it;
}

void
XidMgrServer::DBXactLogData::record_log_entry(uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit,
        uint64_t timestamp, pg_log_mgr::WalProgressTrackerPtr wal_tracker, bool write_log)
{
    DCHECK(timestamp >= _last_timestamp) << "timestamp: " << timestamp << " last_timestamp: " << _last_timestamp;

    std::unique_lock lock(_mutex);
    // When we write a log entry we use the the current timestamp
    // to decide whether we need to rotate the log file. 
    // We call rotate only when we actually write to the log file, _xact_log.log(...).
    // Otherwise we store the timestamp in pending log entries for later use.
    if (write_log) {
        if (_pending_log_entries.empty()) {
            if (timestamp > _last_timestamp) {
                _rotate(timestamp);
                _last_timestamp = timestamp;
            }
            _xact_log.log(pg_xid, xid, real_commit);
            if (wal_tracker) {
                wal_tracker->remove_xid(xid);
            }
        } else {
            DCHECK(std::prev(_pending_log_entries.end())->first < xid);
            if (real_commit) {
                _xact_log.set_last_xid(xid);
            }
            // add to the pending log entries and mark it as ready to be written
            _pending_log_entries[xid] = {pg_xid, real_commit, wal_tracker, true, timestamp};
        }
    } else { // not writing to log yet
        DCHECK(_pending_log_entries.empty() || std::prev(_pending_log_entries.end())->first < xid) 
            << "pending: " << std::prev(_pending_log_entries.end())->first << " new xid:" << xid;
        if (real_commit) {
            _xact_log.set_last_xid(xid);
        }
        _pending_log_entries[xid] = {pg_xid, real_commit, wal_tracker, false, timestamp};
    }

    if (has_schema_changes) {
        _xact_history.push_back({xid, _last_committed_xid});
        _dirty_history = true;
    }

    if (real_commit) {
        _last_committed_xid = xid;
    }
}

void XidMgrServer::DBXactLogData::write_log_entry(uint64_t xid)
{
    std::unique_lock lock(_mutex);

    for (auto pending_it = _pending_log_entries.begin(); pending_it != _pending_log_entries.end();) {
        // go over that with previoius xid's or ready entries
        if (pending_it->first > xid && !pending_it->second.ready) {
            break;
        }

        // Here we are about to write a log entry withe add_log_entry.
        // We use the timestamp stored in the pending log entry
        // to decide whether we need to rotate the log file. 
        if (pending_it->second.timestamp > _last_timestamp) {
            _rotate(pending_it->second.timestamp);
            _last_timestamp = pending_it->second.timestamp;
        }

        _xact_log.add_log_entry(pending_it->second.pg_xid, pending_it->first, pending_it->second.real_commit);

        if (pending_it->second.wal_tracker) {
            pending_it->second.wal_tracker->remove_xid(pending_it->first);
        }
        pending_it =  _pending_log_entries.erase(pending_it);
    }
}

void
XidMgrServer::DBXactLogData::cleanup_history_and_flush()
{
    std::unique_lock lock(_mutex);
    if (!_xact_history.empty() && _dirty_history) {
        // get min schema xid
        uint64_t min_schema_xid = RedisDDL::get_instance()->min_schema_xid(_db_id);

        // find position lower than min_schema_xid
        auto it = std::lower_bound(
            _xact_history.begin(),
            _xact_history.end(),
            min_schema_xid,
            XactHistoryComparator{}
        );
        // erase all smaller xids
        _xact_history.erase(_xact_history.begin(), it);

        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "The history for db_id={} {}",
            _db_id, (_xact_history.empty())? "is now empty" : fmt::format("now starts with schema_xid={}, latest_xid={}",
            _xact_history.front().schema_xid, _xact_history.front().latest_real_commit_xid));

        _dirty_history = false;
    }
    _xact_log.flush();
}

uint64_t
XidMgrServer::DBXactLogData::get_committed_xid(uint64_t schema_xid)
{
    std::shared_lock lock(_mutex);
    uint64_t last_xid = _xact_log.get_last_xid();

    // if schema XID is zero or there is no history for this database,
    // then we always return the most recent committed XID
    if (schema_xid == 0 || _xact_history.empty()) {
        LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Get committed xid for db_id={}: {}", _db_id, last_xid);
        return last_xid;
    }

    auto xid = sys_tbl_mgr::get_committed_xid_from_history(_xact_history, schema_xid, last_xid);
    LOG_DEBUG(LOG_XID_MGR, LOG_LEVEL_DEBUG1, "Get committed xid for db_id={}: {}; {}", _db_id, last_xid, xid);
    return xid;
}

void
XidMgrServer::DBXactLogData::_rotate(uint64_t timestamp)
{
    _xact_log.rotate(timestamp);
}

void
XidMgrServer::DBXactLogData::cleanup(uint64_t min_timestamp, bool archive_logs)
{
    std::unique_lock lock(_mutex);
    _xact_log.cleanup(min_timestamp, archive_logs);
}

}  // namespace springtail::xid_mgr
