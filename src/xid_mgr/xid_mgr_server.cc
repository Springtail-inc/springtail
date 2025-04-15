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
    // iterate over all files in the base path creating partitions
    // _load_partitions();

    _service = std::make_unique<GrpcXidMgrService>(*this);
    _grpc_server_manager.addService(_service.get());
}

void
XidMgrServer::startup()
{
    _grpc_server_manager.startup();
}

void
XidMgrServer::_internal_shutdown()
{
    _service->shutdown();
    _grpc_server_manager.shutdown();
    std::unique_lock lock(_mutex);
    _xact_log_data.clear();
}

uint64_t
XidMgrServer::get_committed_xid(uint64_t db_id, uint64_t schema_xid)
{
    // std::cout << __PRETTY_FUNCTION__ << "(" << std::this_thread::get_id() << "): db_id = " << db_id << "; schema_id = " << schema_xid << std::endl;
    auto token = open_telemetry::OpenTelemetry::set_context_variables({{"db_id", std::to_string(db_id)}, {"xid", std::to_string(schema_xid)}});
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    uint64_t xid = db_id_to_log_data->second.get_committed_xid(schema_xid);
    // std::cout << __PRETTY_FUNCTION__ << "(" << std::this_thread::get_id() << "): returning xid = " << xid << std::endl;
    return xid;
}

void
XidMgrServer::commit_xid(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes)
{
    // std::cout << __PRETTY_FUNCTION__ << "(" << std::this_thread::get_id() << "): db_id = " << db_id << "; pg_xid = " << pg_xid
    //         << "; xid = " << xid << "; has_schema_changes = " << has_schema_changes << std::endl;
    _record_xid_change(db_id, pg_xid, xid, has_schema_changes, true);
}

void
XidMgrServer::record_mapping(uint64_t db_id, uint32_t pg_xid, uint64_t xid, bool has_schema_changes)
{
    // std::cout << __PRETTY_FUNCTION__ << "(" << std::this_thread::get_id() << "): db_id = " << db_id << "; pg_xid = " << pg_xid
    //         << "; xid = " << xid << "; has_schema_changes = " << has_schema_changes << std::endl;
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
    db_id_to_log_data->second.record_mapping(pg_xid, xid, has_schema_changes, real_commit);
    // TODO: not sure if it should be called when pg_xid == 0
    _service->notify_subscriber(db_id, xid);
}

void
XidMgrServer::cleanup(uint64_t db_id, uint64_t min_timestamp, bool archive_logs)
{
    LOG_DEBUG(LOG_XID_MGR, "Cleaning up database {} with min_timestamp {}", db_id, min_timestamp);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.cleanup(min_timestamp, archive_logs);
}

void
XidMgrServer::rotate(uint64_t db_id, uint64_t timestamp)
{
    LOG_DEBUG(LOG_XID_MGR, "Rotate log for database {}, timestamp {}", db_id, timestamp);
    std::shared_lock read_lock(_mutex);
    auto db_id_to_log_data = _find_or_add(db_id, read_lock);
    db_id_to_log_data->second.rotate(timestamp);
}

std::pair<const uint64_t, XidMgrServer::DBXactLogData> *
XidMgrServer::_find_or_add(uint64_t db_id, std::shared_lock<std::shared_mutex> &read_lock)
{
    auto it = _xact_log_data.find(db_id);
    if (it == _xact_log_data.end()) {
        read_lock.unlock();
        std::unique_lock write_lock(_mutex);
        _xact_log_data.emplace(std::piecewise_construct, std::forward_as_tuple(db_id), std::forward_as_tuple(db_id, _base_path));
        write_lock.unlock();
        read_lock.lock();
        it = _xact_log_data.find(db_id);
    }
    std::pair<const uint64_t, DBXactLogData> *pair_ptr = &*it;
    return pair_ptr;
}

}  // namespace springtail::xid_mgr
