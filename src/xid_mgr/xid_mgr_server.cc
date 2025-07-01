#include <common/common.hh>
#include <common/exception.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/open_telemetry.hh>
#include <common/properties.hh>

#include <xid_mgr/xid_mgr_server.hh>
#include <xid_mgr/xid_mgr_service.hh>

namespace springtail::xid_mgr {

XidMgrServer::XidMgrServer() {
    nlohmann::json json = Properties::get(Properties::LOG_MGR_CONFIG);
    nlohmann::json rpc_json;

    // fetch RPC properties for the xid mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("XID Mgr RPC settings are not found");
    }

    std::string base_path;
    Json::get_to<std::string>(json, "transaction_log_path", base_path);
    _base_path = Properties::make_absolute_path(base_path);

    LOG_DEBUG(LOG_XID_MGR, "XidMgrServer: base_path: {}", _base_path.string());

    if (!std::filesystem::exists(_base_path)) {
        std::filesystem::create_directories(_base_path);
    }

    _grpc_server_manager.init(rpc_json);

    _service = std::make_unique<GrpcXidMgrService>(*this);
    _grpc_server_manager.addService(_service.get());

    _archive_logs = Json::get_or<bool>(json, "archive_logs", false);

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
    RedisDDL redis_ddl;
    while (!_is_shutting_down()) {
        // sleep for at least XIG_MGR_MIN_SYNC_MS
        std::this_thread::sleep_for(std::chrono::milliseconds(XIG_MGR_MIN_SYNC_MS));
        std::shared_lock lock(_mutex);
        for (auto &it: _xact_log_data) {
            it.second.cleanup_history_and_flush(redis_ddl);
        }
    }
}

uint64_t
XidMgrServer::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
{
    auto token = open_telemetry::OpenTelemetry::set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    uint64_t xid = db_id_to_log_data->second.get_committed_xid(schema_xid);
    return xid;
}

void
XidMgrServer::commit_xid(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes)
{
    _record_xid_change(db_id, pg_xid, xid, has_schema_changes, true);
}

void
XidMgrServer::record_mapping(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes)
{
    _record_xid_change(db_id, pg_xid, xid, has_schema_changes, false);
}

void
XidMgrServer::_record_xid_change(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit)
{
    auto token = open_telemetry::OpenTelemetry::set_context_variables({
        {"db_id", std::to_string(db_id)},
        {"pg_xid", std::to_string(pg_xid)},
        {"xid", std::to_string(xid)}
    });
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.record_log_entry(pg_xid, xid, has_schema_changes, real_commit);
    if (real_commit) {
        _service->notify_subscriber(db_id, xid);
    }
}

void
XidMgrServer::cleanup(uint64_t db_id, uint64_t min_timestamp)
{
    LOG_DEBUG(LOG_XID_MGR, "Cleaning up database {} with min_timestamp {}", db_id, min_timestamp);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.cleanup(min_timestamp, _archive_logs);
}

void
XidMgrServer::rotate(uint64_t db_id, uint64_t timestamp)
{
    LOG_DEBUG(LOG_XID_MGR, "Rotate log for database {}, timestamp {}", db_id, timestamp);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.rotate(timestamp);
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
        PgXactLogWriter::set_last_xid_in_storage(_base_path / std::to_string(db_id), std::numeric_limits<uint64_t>::max(), _archive_logs);
        auto result = _xact_log_data.emplace(std::piecewise_construct, std::forward_as_tuple(db_id), std::forward_as_tuple(db_id, _base_path));
        it = result.first;
        write_lock.unlock();

        read_lock.lock();
    }
    return it;
}

void
XidMgrServer::DBXactLogData::record_log_entry(uint32_t pg_xid, uint64_t xid, bool has_schema_changes, bool real_commit)
{
    std::unique_lock lock(_mutex);
    _xact_log.log(pg_xid, xid, real_commit);
    if (has_schema_changes) {
        _xact_history.push_back(xid);
        _dirty_history = true;
    }
}

void
XidMgrServer::DBXactLogData::cleanup_history_and_flush(RedisDDL &redis_ddl)
{
    std::unique_lock lock(_mutex);
    if (!_xact_history.empty() && _dirty_history) {
        // get min schema xid
        uint64_t min_schema_xid = redis_ddl.min_schema_xid(_db_id);

        // find position lower than min_schema_xid
        auto it = std::ranges::lower_bound(_xact_history.begin(), _xact_history.end(), min_schema_xid);
        // erase all smaller xids
        _xact_history.erase(_xact_history.begin(), it);

        LOG_DEBUG(LOG_XID_MGR, "The history for db_id={} {}",
            _db_id, (_xact_history.empty())? "is now empty" : fmt::format("now starts with xid={}", _xact_history.front()));
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
XidMgrServer::DBXactLogData::rotate(uint64_t timestamp)
{
    std::unique_lock lock(_mutex);
    _xact_log.rotate(timestamp);
}

void
XidMgrServer::DBXactLogData::cleanup(uint64_t min_timestamp, bool archive_logs)
{
    std::unique_lock lock(_mutex);
    _xact_log.cleanup(min_timestamp, archive_logs);
}

}  // namespace springtail::xid_mgr
