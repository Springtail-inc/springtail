#include <nlohmann/json.hpp>

#include <postgresql/server/catalog/pg_type_d.h>

#include <common/common.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/properties.hh>
#include <storage/vacuumer.hh>
#include <sys_tbl_mgr/request_helper.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/system_table_mgr.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::sys_tbl_mgr {

Server::Server() : Singleton<Server>(ServiceId::SysTblMgrServerId)
{
    auto json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    nlohmann::json rpc_json;
    uint64_t cache_size = 0;

    if (!Json::get_to<uint64_t>(json, "cache_size", cache_size)) {
        throw Error("Sys tbl mgr cache size settings not found");
    }

    // fetch RPC properties for the sys_tbl_mgr server
    if (!Json::get_to(json, "rpc_config", rpc_json)) {
        throw Error("SysTblMgr RPC settings are not found");
    }

    _service = std::make_unique<Service>(*this);
    _grpc_server_manager.init(rpc_json);
    _grpc_server_manager.addService(_service.get());

    _schema_object_cache = std::make_shared<SchemaCache>(cache_size);
    _grpc_server_manager.startup();
}

void
Server::_internal_shutdown()
{
    _grpc_server_manager.shutdown();
}

std::string
Server::create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
{
    auto request = RequestHelper::gen_table_request(db_id, xid, msg);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got create_table() -- db {} table {} xid {} lsn {}", db_id,
            request.table().id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // perform the CREATE TABLE
    auto&& ddl = _create_table(request);

    // serialize the JSON and return
    return nlohmann::to_string(ddl);
}

std::string
Server::alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
{
    auto request = RequestHelper::gen_table_request(db_id, xid, msg);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got alter_table() -- db {} table {} xid {} lsn {}", db_id,
            request.table().id(), xid.xid, xid.lsn);

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // retrieve the id of the namespace
    // this function is called by drop table, so we don't check if
    // the namespace exists
    auto ns_info = _get_namespace_info(db_id, request.table().namespace_name(), xid, false);
    CHECK(ns_info);

    nlohmann::json ddl;
    ddl["tid"] = request.table().id();
    ddl["xid"] = request.xid();
    ddl["lsn"] = request.lsn();
    ddl["schema"] = request.table().namespace_name();
    ddl["table"] = request.table().name();
    ddl["rls_enabled"] = request.table().rls_enabled();
    ddl["rls_forced"] = request.table().rls_forced();

    // retrieve the name of the table at the point of alteration
    auto table_info = _get_table_info(db_id, request.table().id(), xid);

    // note: table should always exist when calling alter_table()
    assert(table_info != nullptr);

    // check if the table is a partitioned table
    std::optional<uint64_t> parent_table_id = std::nullopt;
    std::optional<std::string> partition_key = std::nullopt;
    std::optional<std::string> partition_bound = std::nullopt;
    if (request.table().has_parent_table_id() && request.table().parent_table_id() != constant::INVALID_TABLE) {
        parent_table_id = request.table().parent_table_id();
    }
    if (request.table().has_partition_key() && !request.table().partition_key().empty()) {
        partition_key = request.table().partition_key();
    }
    if (request.table().has_partition_bound() && !request.table().partition_bound().empty()) {
        partition_bound = request.table().partition_bound();
    }

    // update the partition details
    if (partition_key.has_value()) {
        ddl["partition_key"] = partition_key.value();
    }
    if (partition_bound.has_value()) {
        ddl["partition_bound"] = partition_bound.value();
    }
    if (parent_table_id.has_value()) {
        ddl["parent_table_id"] = parent_table_id.value();
    }

    auto new_info = std::make_shared<TableCacheRecord>(request.table().id(), request.xid(),
                                                       request.lsn(), ns_info->id,
                                                       request.table().name(),
                                                       parent_table_id, partition_key,
                                                       partition_bound, request.table().rls_enabled(),
                                                       request.table().rls_forced(), true);

    if (table_info->namespace_id != ns_info->id) {
        // if the schema/namespace changed then update the table_names table
        // insert the new name for this oid

        _set_table_info(db_id, new_info);

        // set the DDL statement
        ddl["action"] = "set_namespace";

        auto old_ns_info = _get_namespace_info(db_id, table_info->namespace_id, xid);
        CHECK(old_ns_info);
        ddl["old_schema"] = old_ns_info->name;

    } else if (table_info->name != request.table().name()) {
        // if the name is changed, update the name in the table_names table
        // insert the new name for this oid
        _set_table_info(db_id, new_info);

        // set the DDL statement
        ddl["action"] = "rename";
        ddl["old_table"] = table_info->name;

        if (table_info->namespace_id != ns_info->id) {
            auto old_ns_info = _get_namespace_info(db_id, table_info->namespace_id, xid);
            CHECK(old_ns_info);
            ddl["old_schema"] = old_ns_info->name;
        } else {
            ddl["old_schema"] = request.table().namespace_name();
        }

        _set_primary_index(db_id, ns_info->id, request.table().id(), table_info->name,
                           ns_info->name, xid);

    } else if (table_info->rls_enabled != request.table().rls_enabled()) {
        // if the RLS flags are changed, update the table_names table
        LOG_INFO("RLS enabled changed for table {}@{}:{} {} -> {}",
                  db_id, request.xid(), request.table().id(),
                  table_info->rls_enabled, request.table().rls_enabled());
        _set_table_info(db_id, new_info);

        // set the DDL statement
        ddl["action"] = "set_rls_enabled";
        ddl["rls_enabled"] = request.table().rls_enabled();
    } else if (table_info->rls_forced != request.table().rls_forced()) {
        // if the RLS forced flags are changed, update the table_names table
        _set_table_info(db_id, new_info);

        // set the DDL statement
        ddl["action"] = "set_rls_forced";
        ddl["rls_forced"] = request.table().rls_forced();
    } else {
        // get the schema prior to this change
        auto info = _get_schema_info(db_id, request.table().id(), xid, xid);

        // generate a tuple for the change
        // note: _generate_update() sets the necessary elements of the ddl
        auto history = _generate_update(info->columns(), request.table().columns(),
                                        xid, ddl);

        // If the partition key is not empty, generate the history events for the the child partition tables
        if (partition_key.value_or("") != "") {
            ddl = _generate_partition_updates(request, history);
        }

        // we won't apply any changes to the system tables in these cases
        if (history.update_type() != static_cast<int8_t>(SchemaUpdateType::NO_CHANGE) &&
            history.update_type() != static_cast<int8_t>(SchemaUpdateType::RESYNC)) {
            // write the column change to the schemas table and update the cache
            _set_schema_info(db_id, request.table().id(), ns_info->id,
                            request.table().name(), {history});
        }

        _set_primary_index(db_id, ns_info->id, request.table().id(),
                    request.table().name(), request.table().namespace_name(), xid);
    }

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.oid, xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Alter table {}@{}:{} action: {}", db_id, xid.xid,
                            request.table().id(), ddl["action"].get<std::string>());

    return nlohmann::to_string(ddl);
}

std::string
Server::drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg)
{

    proto::DropTableRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(msg.oid);
    request.set_namespace_name(msg.namespace_name);
    request.set_name(msg.table);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got drop_table() -- {}@{}:{}", request.table_id(), request.xid(), request.lsn());

    // hold a shared lock to prevent a concurrent finalize()
    boost::shared_lock lock(_write_mutex);

    // perform the DROP TABLE
    auto&& ddl = _drop_table(request);

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.oid, xid);

    return nlohmann::to_string(ddl);
}

std::string
Server::create_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got create_namespace() -- db {} namespace_id {} name {} xid {} lsn {}",
                db_id, request.namespace_id(), request.name(), request.xid(),
                request.lsn());


    nlohmann::json ddl;

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    auto ns_info = _get_namespace_info(db_id, request.namespace_id(), xid);
    if (ns_info) {
        LOG_INFO("Namespace exists -- db {} namespace_id {} name {} xid {} lsn {}",
                db_id, request.namespace_id(), request.name(), request.xid(),
                request.lsn());
        ddl["action"] = "no_change";
    } else {
        // update the namespace_names table
        ddl = _mutate_namespace(db_id, request.namespace_id(), request.name(), xid, true);
        ddl["action"] = "ns_create";
    }
    return nlohmann::to_string(ddl);
}

std::string
Server::alter_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got alter_namespace() -- db {} namespace_id {} name {} xid {} lsn {}",
                db_id, request.namespace_id(), request.name(), request.xid(),
                request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // retrieve the old namespace name
    auto ns_info = _get_namespace_info(db_id, request.namespace_id(), xid);
    CHECK(ns_info);

    // update the namespace_names table
    auto ddl =
        _mutate_namespace(db_id, request.namespace_id(), request.name(), xid, true);
    ddl["action"] = "ns_alter";
    ddl["old_name"] = ns_info->name;

    return nlohmann::to_string(ddl);
}

std::string
Server::drop_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got drop_namespace() -- db {} namespace_id {} xid {} lsn {}", db_id,
                request.namespace_id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // update the namespace_names table
    auto ddl =
        _mutate_namespace(db_id, request.namespace_id(), request.name(), xid, false);
    ddl["action"] = "ns_drop";
    ddl["name"] = request.name();

    return nlohmann::to_string(ddl);
}

std::string
Server::create_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_type_id(msg.oid);
    request.set_namespace_id(msg.namespace_id);
    request.set_name(msg.name);
    request.set_namespace_name(msg.namespace_name);
    request.set_value_json(msg.value_json);
    request.set_type(msg.type);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got create_usertype() -- db {} namespace_id {} type_id {} name {} xid {} lsn {}",
                db_id, request.namespace_id(), request.type_id(), request.name(), request.xid(),
                request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // update the user_types table
    auto ddl = _mutate_usertype(db_id, request.type_id(), request.name(),
        request.namespace_id(), request.type(), request.value_json(), xid, true);

    ddl["action"] = "ut_create";
    ddl["schema"] = request.namespace_name();

    return nlohmann::to_string(ddl);
}

std::string
Server::alter_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_type_id(msg.oid);
    request.set_namespace_id(msg.namespace_id);
    request.set_name(msg.name);
    request.set_namespace_name(msg.namespace_name);
    request.set_value_json(msg.value_json);
    request.set_type(msg.type);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got alter_usertype() -- db {} namespace_id {} type_id {} name {} xid {} lsn {}",
                db_id, request.namespace_id(), request.type_id(), request.name(), request.xid(),
                request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // retrieve the old user type name
    auto user_type_info = _get_usertype_info(db_id, request.type_id(), xid);
    CHECK(user_type_info != nullptr);

    // update the user defined types table
    auto ddl = _mutate_usertype(db_id, request.type_id(), request.name(),
        request.namespace_id(), request.type(), request.value_json(), xid, true);

    ddl["action"] = "ut_alter";
    ddl["schema"] = request.namespace_name();
    ddl["old_name"] = user_type_info->name;
    ddl["old_value"] = user_type_info->value_json;

    // need to get old namespace id and check if it has changed
    if (user_type_info->namespace_id != request.namespace_id()) {
        auto old_ns_info = _get_namespace_info(db_id, user_type_info->namespace_id, xid);
        ddl["old_schema"] = old_ns_info->name;
    } else {
        ddl["old_schema"] = request.namespace_name();
    }

    return nlohmann::to_string(ddl);
}

std::string
Server::drop_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_type_id(msg.oid);
    request.set_name(msg.name);
    request.set_namespace_name(msg.namespace_name);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got drop_usertype() -- db {} namespace_id {} type_id {} xid {} lsn {}", db_id,
                request.namespace_id(), request.type_id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    nlohmann::json ddl;
    auto user_type_info = _get_usertype_info(db_id, request.type_id(), xid);
    if (user_type_info == nullptr) {
        // drop could for a type we don't support, so ignore it here
        LOG_WARN("User type {} not found", request.type_id());
        ddl["action"] = "no_change";
    } else {
        // update the user defined types table
        ddl = _mutate_usertype(db_id, request.type_id(), request.name(),
            request.namespace_id(), request.type(), request.value_json(), xid, false);

        ddl["action"] = "ut_drop";
        ddl["schema"] = request.namespace_name();
    }

    return nlohmann::to_string(ddl);
}

std::string
Server::attach_partition(uint64_t db_id, const XidLsn &xid, const PgMsgAttachPartition &msg)
{
    proto::AttachPartitionRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(msg.table_id);
    request.set_table_name(msg.table_name);
    request.set_namespace_name(msg.namespace_name);
    request.set_partition_key(msg.partition_key);

    for (const auto &partition_data : msg.partition_data) {
        auto *info = request.add_partition_data();
        RequestHelper::set_partition_data(info, partition_data);
    }

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got attach_partition() -- db {} table {} xid {} lsn {}", db_id,
              request.table_id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    std::unordered_map<uint64_t, std::pair<std::string, std::string>> partition_map;
    auto attached_partitions = _get_modified_partition_details(db_id, xid, request.table_id(), request.partition_data(), &partition_map, true);

    // Update the system table for the attached partition
    std::string partition_name = "";
    std::string partition_bound = "";
    std::string partition_schema = "";
    for (const auto& attached_table_id : attached_partitions) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Attaching Partition for table ID: {}, to parent table ID: {}", attached_table_id, request.table_id());
        auto table_info = _get_table_info(db_id, attached_table_id, xid);

        // note: table should always exist when calling alter_table()
        assert(table_info != nullptr);

        std::optional<std::string> partition_key = std::nullopt;
        if ( table_info->partition_key != "" ) {
            partition_key = table_info->partition_key;
        }

        // update the system table
        partition_bound = partition_map[attached_table_id].first;

        // update the partition key
        if ( !partition_map[attached_table_id].second.empty() ) {
            partition_key = partition_map[attached_table_id].second;
        }

        auto updated_table_info =
            std::make_shared<TableCacheRecord>(attached_table_id, request.xid(), request.lsn(),
                                               table_info->namespace_id, table_info->name,
                                               request.table_id(), partition_key, partition_bound,
                                               table_info->rls_enabled, table_info->rls_forced, true);
        _set_table_info(db_id, updated_table_info);

        // get the namespace info
        auto ns_info = _get_namespace_info(db_id, table_info->namespace_id, xid);
        assert(ns_info != nullptr);

        partition_schema = ns_info->name;
        partition_name = table_info->name;
    }

    nlohmann::json ddl;

    ddl["action"] = "attach_partition";
    ddl["partition_schema"] = partition_schema;
    ddl["partition_name"] = partition_name;
    ddl["partition_bound"] = partition_bound;
    ddl["schema"] = request.namespace_name();
    ddl["table"] = request.table_name();
    ddl["xid"] = request.xid();
    ddl["lsn"] = request.lsn();

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Attach partition DDL: {}", ddl.dump());

    return nlohmann::to_string(ddl);
}

std::string
Server::detach_partition(uint64_t db_id, const XidLsn &xid, const PgMsgDetachPartition &msg)
{
    proto::DetachPartitionRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(msg.table_id);
    request.set_namespace_name(msg.namespace_name);
    request.set_table_name(msg.table_name);
    request.set_partition_key(msg.partition_key);

    for (const auto &partition_data : msg.partition_data) {
        auto *info = request.add_partition_data();
        RequestHelper::set_partition_data(info, partition_data);
    }

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got detach_partition() -- db {} table {} xid {} lsn {}", db_id,
              request.table_id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    auto detached_partitions = _get_modified_partition_details(db_id, xid, request.table_id(), request.partition_data(), nullptr, false);

    // Update the system table for the detached table
    std::string partition_name = "";
    std::string partition_schema = "";
    for (const auto& detached_table_id : detached_partitions) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Detaching Partition for table ID: {}, from parent table ID: {}", detached_table_id, request.table_id());
        auto table_info = _get_table_info(db_id, detached_table_id, xid);

        // note: table should always exist when calling alter_table()
        assert(table_info != nullptr);

        std::optional<std::string> partition_key = std::nullopt;
        if ( table_info->partition_key != "" ) {
            partition_key = table_info->partition_key;
        }

        // update the system table
        auto updated_table_info =
            std::make_shared<TableCacheRecord>(detached_table_id, request.xid(), request.lsn(),
                                               table_info->namespace_id, table_info->name,
                                               std::nullopt, partition_key, std::nullopt,
                                               table_info->rls_enabled, table_info->rls_forced, true);
        _set_table_info(db_id, updated_table_info);

        // get the namespace info
        auto ns_info = _get_namespace_info(db_id, table_info->namespace_id, xid);
        assert(ns_info != nullptr);

        partition_schema = ns_info->name;
        partition_name = table_info->name;
    }

    nlohmann::json ddl;

    ddl["action"] = "detach_partition";
    ddl["partition_schema"] = partition_schema;
    ddl["partition_name"] = partition_name;
    ddl["schema"] = request.namespace_name();
    ddl["table"] = request.table_name();
    ddl["xid"] = request.xid();
    ddl["lsn"] = request.lsn();

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Detach partition DDL: {}", ddl.dump());

    return nlohmann::to_string(ddl);
}

proto::IndexProcessRequest
Server::create_index(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg, sys_tbl::IndexNames::State state)
{
    auto request = RequestHelper::gen_index_request(db_id, xid, msg);
    auto *index = request.mutable_index();
    index->set_state(static_cast<int32_t>(state));

    proto::IndexProcessRequest response;

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got create_index() -- db {}, table {}, index {}, xid {}:{}", db_id,
                request.index().table_id(), request.index().id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // perform the CREATE INDEX
    bool created = false;
    const auto &index_info = _create_index(request, created);
    if (created) {
        response.set_action("create_index");
    } else {
        response.set_action("no_op");
    }
    *response.mutable_index() = index_info;

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.table_oid, xid);

    return response;
}

void
Server::set_index_state(uint64_t db_id, const XidLsn &xid, uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state)
{
    proto::SetIndexStateRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(table_id);
    request.set_index_id(index_id);
    request.set_state(static_cast<int32_t>(state));

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got set_index_state() -- db {}, xid {}:{}, table_id {}, index_id {}, state {}",
                db_id, xid.xid, xid.lsn, table_id, index_id, (uint32_t)(state));

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    _set_index_state(request);

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, table_id, xid);
}

proto::IndexInfo
Server::get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid, std::optional<uint64_t> tid)
{
    proto::GetIndexInfoRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_index_id(index_id);
    if (tid) {
        request.set_table_id(*tid);
    }
    proto::IndexInfo response;

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got get_index_info() -- db {}, xid {}:{}, table_id {}, index_id {}",
                db_id, xid.xid, xid.lsn, (tid.has_value()? tid.value() : 0), index_id);

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_read_mutex);

    response = _get_index_info(request);
    return response;
}

proto::IndexesInfo
Server::get_unfinished_indexes_info(uint64_t db_id)
{
    proto::GetUnfinishedIndexesInfoRequest request;
    request.set_db_id(db_id);

    proto::IndexesInfo response;
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got get_unfinished_indexes_info() -- {}", db_id);

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_read_mutex);

    response = _get_unfinished_indexes_info(db_id);

    return response;
}

proto::IndexProcessRequest
Server::drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg)
{
    proto::DropIndexRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_index_id(msg.oid);
    request.set_namespace_name(msg.namespace_name);

    proto::IndexProcessRequest response;

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got drop_index() -- db {}, index {}, xid {}:{}", db_id,
                request.index_id(), request.xid(), request.lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // Create response for the dropped index
    proto::IndexInfo dropped_index;

    // perform the DROP INDEX
    _drop_index(xid, db_id, request.index_id(), std::nullopt, sys_tbl::IndexNames::State::BEING_DELETED, std::ref(dropped_index));

    // Automatically invalidate the schema cache from the provided XID
    _schema_object_cache->invalidate_by_index(db_id, msg.oid, xid);

    dropped_index.set_id(request.index_id());
    dropped_index.set_name(request.name());
    dropped_index.set_namespace_name(request.namespace_name());
    response.set_action("drop_index");
    *response.mutable_index() = dropped_index;

    return response;
}

void
Server::update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid, const TableMetadata &metadata)
{
    proto::UpdateRootsRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);
    request.set_table_id(table_id);

    for (auto const &[index_id, extent_id] : metadata.roots) {
        auto *root = request.add_roots();
        root->set_index_id(index_id);
        root->set_extent_id(extent_id);
    }

    auto *stats = request.mutable_stats();
    stats->set_row_count(metadata.stats.row_count);
    stats->set_end_offset(metadata.stats.end_offset);
    stats->set_last_internal_row_id(metadata.stats.last_internal_row_id);
    request.set_snapshot_xid(metadata.snapshot_xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got update_roots() -- db {}, table {}, xid {}",
            db_id, table_id, xid);

    // hold a shared lock to prevent a concurrent finalize()
    boost::shared_lock lock(_write_mutex);

    // update the metadata and return
    _update_roots(request);
}

void
Server::finalize(uint64_t db_id, uint64_t xid, bool call_sync)
{
    proto::FinalizeRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got finalize() -- db {}, xid {}", db_id, xid);

    // block all mutations
    boost::unique_lock wlock(_write_mutex);

    // note: it is safe to pre-write data from later XIDs into the system tables during a finalize
    //       since if there is a failure they will simply be overwritten during recovery
    auto write_xid = _get_write_xid(db_id);
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Finalize system tables: {}@{} >= {}", db_id,
                        request.xid(), write_xid);
    CHECK_GE(request.xid(), write_xid);

    // finalize the mutated tables at the write_xid
    // XXX we currently don't store the metadata, but re-read it from the roots file each time
    std::map<uint64_t, TableMetadata> md_map;
    for (const auto& entry : _write[db_id]) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Finalize table {}@{}", entry.first, request.xid());
        md_map[entry.first] = entry.second->finalize(call_sync);
    }
    if (md_map.empty()) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Nothing to finalize: {}@{} >= {}", db_id, request.xid(),
                    write_xid);
        return;
    }

    // block all read access while we swap access roots
    boost::unique_lock rlock(_read_mutex);

    // move the read_xid to the request xid, and move the write_xid to just beyond the
    // provided request xid
    _set_xids(db_id, XidLsn(request.xid()), request.xid() + 1);

    // note: we could update the table pointers?
    //       or maybe cache the roots to avoid reading the roots file?
    _read[db_id].clear();
    _write[db_id].clear();
    _clear_table_info(db_id);
    _clear_roots_info(db_id);
    _clear_schema_info(db_id);
    _clear_namespace_info(db_id, request.xid());



    // Commit expired extents in Vacuumer
    Vacuumer::get_instance()->commit_expired_extents(db_id, request.xid());
}

void 
Server::sync(uint64_t db_id, uint64_t xid)
{
    // block all mutations
    boost::unique_lock wlock(_write_mutex);

    for (const auto& entry : _write[db_id]) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Sync table {}@{}", entry.first, xid);
        entry.second->sync_data_and_indexes();
    }
}

void
Server::revert(uint64_t db_id, uint64_t xid)
{
    proto::RevertRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got revert() -- db {} xid {}", db_id,
                request.xid());

    // get the base directory for table data
    std::filesystem::path table_base;
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", table_base);
    table_base = Properties::make_absolute_path(table_base);

    boost::shared_lock write_lock(_write_mutex);
    boost::shared_lock read_lock(_write_mutex);

    // ensure that we don't have a partially committed XID currently in-memory
    CHECK(_write[db_id].empty());

    // go through each system table and adjust it's roots symlink to point to the correct file for
    // the committed XID
    for (auto table_id : sys_tbl::TABLE_IDS) {
        // get the table directory path
        auto table_dir = table_helpers::get_table_dir(table_base, db_id, table_id, 1);

        // find all roots files and extract their XIDs
        std::map<uint64_t, std::filesystem::path> roots_files;
        if (!std::filesystem::exists(table_dir)) {
            LOG_WARN("Table directory {} does not exist", table_dir.string());
            continue;
        }

        for (const auto& entry : std::filesystem::directory_iterator(table_dir)) {
            // only process files
            if (!entry.is_regular_file()) {
                continue;
            }

            // only process "roots.xid" files
            auto filename = entry.path().filename().string();
            if (!filename.starts_with("roots.")) {
                continue;
            }

            try {
                uint64_t filename_xid = std::stoull(filename.substr(6)); // skip "roots."
                roots_files.try_emplace(filename_xid, entry.path());
            } catch (...) {
                // Skip files with invalid numbers
                continue;
            }
        }

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found {} root files for system table {}",
                  roots_files.size(), table_id);

        // find the largest valid XID
        if (!roots_files.empty()) {
            // find the first XID beyond the committed XID
            auto del_i = roots_files.upper_bound(request.xid());
            auto root_i = std::make_reverse_iterator(del_i);

            if (root_i == roots_files.rend()) {
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Clear system table {}", root_i->second, table_id);
                // there's no valid roots, clear *all* of the system table data
                for (const auto& entry : std::filesystem::directory_iterator(table_dir)) {
                    std::filesystem::remove_all(entry.path());
                }
            } else {
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Picked root file {} for system table {}", root_i->second,
                          table_id);

                std::filesystem::create_symlink(root_i->second,
                                                table_dir / constant::ROOTS_TMP_FILE);
                std::filesystem::rename(table_dir / constant::ROOTS_TMP_FILE,
                                        table_dir / constant::ROOTS_FILE);

                // remove any roots files with larger XIDs
                for (; del_i != roots_files.end(); ++del_i) {
                    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Delete root file {} for system table {}",
                              del_i->second, table_id);
                    std::filesystem::remove(del_i->second);
                }
            }
        }

        // remove rows from the table that are beyond the committed XID
        auto mtable = _get_mutable_system_table(db_id, table_id);
        auto table = _get_system_table(db_id, table_id);
        auto schema = table->extent_schema();
        FieldPtr xid_f = schema->get_field("xid");
        auto primary_fields = schema->get_fields(table->primary_key());
        for (auto row : *table) {
            if (xid_f->get_uint64(&row) > request.xid()) {
                mtable->remove(std::make_shared<FieldTuple>(primary_fields, &row),
                               constant::UNKNOWN_EXTENT);
            }
        }
    }
}

// TODO: some common code with GetRoots implementation in Service, fix it
TableMetadataPtr
Server::get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    proto::GetRootsResponse response;
    proto::GetRootsRequest request;
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.set_xid(xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got get_roots() -- db {} tid {} xid {}", db_id, request.table_id(), request.xid());

    boost::shared_lock lock(_read_mutex);

    XidLsn xid_lsn(request.xid(), constant::MAX_LSN);

    // make sure that the table exists at this XID
    auto table_info = _get_table_info(db_id, request.table_id(), xid_lsn);
    if (table_info == nullptr) {
        // We just return an empty response if the table doesn't exist
        return nullptr;
    }

    // get the roots
    auto info = _get_roots_info(db_id, request.table_id(), xid_lsn);

    response = *info;

    auto metadata = std::make_shared<TableMetadata>();

    for (const auto &root : response.roots()) {
        metadata->roots.push_back({root.index_id(), root.extent_id()});
    }
    metadata->stats.row_count = response.stats().row_count();
    metadata->stats.end_offset = response.stats().end_offset();
    metadata->stats.last_internal_row_id = response.stats().last_internal_row_id();
    metadata->snapshot_xid = response.snapshot_xid();

    return metadata;
}

std::shared_ptr<const SchemaMetadata>
Server::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    auto populate = [this](uint64_t db_id, uint64_t table_id, const XidLsn &xid) {
        proto::GetSchemaRequest request;
        request.set_db_id(db_id);
        request.set_table_id(table_id);
        request.set_xid(xid.xid);
        request.set_lsn(xid.lsn);

        proto::GetSchemaResponse response;
        LOG_INFO("got get_schema(): db {} tid {} xid {} lsn {}", db_id,
                request.table_id(), request.xid(), request.lsn());

        boost::shared_lock lock(_read_mutex);

        auto info = _get_schema_info(db_id, request.table_id(), xid, xid);

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Returning start_xid {}:{}, end_xid {}:{}",
                            info->access_xid_start(), info->access_lsn_start(), info->access_xid_end(),
                            info->access_lsn_end());

        response = *info;
        return RequestHelper::pack_metadata(response);
    };

    // Retrieve through the schema cache
    return _schema_object_cache->get(db_id, table_id, xid, populate);

}

SchemaMetadataPtr
Server::get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid)
{
    proto::GetTargetSchemaRequest request;
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.set_access_xid(access_xid.xid);
    request.set_access_lsn(access_xid.lsn);
    request.set_target_xid(target_xid.xid);
    request.set_target_lsn(target_xid.lsn);

    proto::GetSchemaResponse response;

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got get_target_schema() -- {}, {}", request.access_xid(), request.target_xid());

    boost::shared_lock lock(_read_mutex);

    auto info = _get_schema_info(db_id, request.table_id(), access_xid, target_xid);

    response = *info;
    return RequestHelper::pack_metadata(response);
}

bool
Server::exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    // prepare the request
    proto::ExistsRequest request;
    RequestHelper::set_request_common(request, db_id, xid);
    request.set_table_id(table_id);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got exists() -- db {} tid {} xid {} lsn {}", db_id,
                request.table_id(), request.xid(), request.lsn());

    boost::shared_lock lock(_read_mutex);

    // Check the table existence cache first
    {
        boost::shared_lock cache_lock(_table_existence_cache_mutex);
        auto result = _check_table_existence_cache(db_id, table_id, xid);
        if (result.has_value()) {
            return result.value();
        }
    }

    // Cache miss - need to populate the cache
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
              "Table existence cache miss: db {} tid {} xid {}:{}",
              db_id, table_id, xid.xid, xid.lsn);

    // Upgrade to unique lock on _table_existence_cache_mutex to populate cache
    boost::unique_lock cache_lock(_table_existence_cache_mutex);

    // Double-check that another thread hasn't populated it
    auto result = _check_table_existence_cache(db_id, table_id, xid);
    if (result.has_value()) {
        return result.value();
    }

    // Populate the cache (this will check _table_cache after scanning disk)
    bool found = _populate_table_existence_cache(db_id, table_id);
    if (!found) {
        // Table never existed
        return false;
    }

    // Now check the cache again (must succeed now)
    result = _check_table_existence_cache(db_id, table_id, xid);
    return result.value();
}

std::string
Server::swap_sync_table(const proto::NamespaceRequest &namespace_req,
                        const proto::TableRequest &create_req,
                        const std::vector<proto::IndexRequest> &index_reqs,
                        const proto::UpdateRootsRequest &roots_req)
{
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got swap_sync_table()");
    nlohmann::json ddls = nlohmann::json::array();  // Initialize to empty array

    // 1. acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // 2. check if the namespace exists at the end of the target XID, if it doesn't create it
    XidLsn ns_xid(namespace_req.xid(), namespace_req.lsn());
    auto ns_info = _get_namespace_info(namespace_req.db_id(), namespace_req.name(), ns_xid);
    if (!ns_info) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Create namespace; db {}, name {}, id {}, xid {}:{}",
                            namespace_req.db_id(), namespace_req.name(),
                            namespace_req.namespace_id(), ns_xid.xid, ns_xid.lsn);

        auto&& ns_ddl = _mutate_namespace(namespace_req.db_id(), namespace_req.namespace_id(),
                                          namespace_req.name(), ns_xid, true);
        ns_ddl["action"] = "ns_create";
        ddls.push_back(ns_ddl);
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Create namespace name {}, id {}", namespace_req.name(),
                            namespace_req.namespace_id());
    } else {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Skip create namespace name {}, id {}",
                            namespace_req.name(), namespace_req.namespace_id());
    }

    // 3. retrieve the table information at the end of the target XID
    XidLsn xid(create_req.xid(), constant::MAX_LSN);
    auto info = _get_table_info(create_req.db_id(), create_req.table().id(), xid);

    // 4. if the table exists at the end of the XID, perform a drop
    bool is_resync = false;
    if (info != nullptr) {
        proto::DropTableRequest drop;
        drop.set_db_id(create_req.db_id());
        drop.set_table_id(create_req.table().id());
        drop.set_xid(create_req.xid());
        drop.set_lsn(constant::RESYNC_DROP_LSN);
        drop.set_namespace_name(create_req.table().namespace_name());
        drop.set_name(create_req.table().name());

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Drop table: {}:{} @ {}:{}", drop.db_id(), drop.table_id(),
                            drop.xid(), drop.lsn());

        is_resync = true;
        auto&& drop_ddl = this->_drop_table(drop, is_resync);
        drop_ddl["is_resync"] = is_resync;
        ddls.push_back(drop_ddl);
    }

    // 5. perform a create table
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Create table: {}:{} @ {}:{}", create_req.db_id(),
                        create_req.table().id(), create_req.xid(), create_req.lsn());

    assert(create_req.lsn() == constant::RESYNC_CREATE_LSN);
    auto&& create_ddl_array = this->_create_table(create_req);

    DCHECK(create_ddl_array.is_array());  // Always an array now

    if (create_ddl_array.empty()) {
        // Empty array - parent chain incomplete, DDL deferred
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                  "DDL deferred during resync for table {} - parent chain incomplete",
                  create_req.table().id());

    } else {
        // Non-empty array - mark first element (root parent) with is_resync flag
        create_ddl_array[0]["is_resync"] = is_resync;

        for (const auto& ddl_item : create_ddl_array) {
            ddls.push_back(ddl_item);
        }

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                  "Resync generated {} DDL statement(s) (parent + descendants)",
                  create_ddl_array.size());
    }

    for (const proto::IndexRequest& index : index_reqs) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Create index: {}:{} @ {}:{}", index.db_id(),
                            index.index().id(), index.xid(), index.lsn());

        CHECK_EQ(index.lsn(), constant::RESYNC_CREATE_LSN);

        // note: we don't store the create_index DDL since it doesn't need to be replayed at the FDW
        bool created = false;
        this->_create_index(index, created);
    }

    // 6. update the metadata of the table
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Update roots: {}:{} @ {}:{}", create_req.db_id(),
                        create_req.table().id(), create_req.xid(), create_req.lsn());
    this->_update_roots(roots_req);

    // 7. serialize the ddl json and return
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Response: {}", nlohmann::to_string(ddls));

    // Auto-invalidate the cache for the swapped table
    invalidate_table(create_req.db_id(), create_req.table().id(),
                     XidLsn(create_req.xid(), create_req.lsn()));

    return nlohmann::to_string(ddls);
}

std::shared_ptr<UserType>
Server::get_usertype(uint64_t db_id, uint64_t type_id, const XidLsn &xid)
{
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "got get_usertype() -- db {} type_id {} xid {} lsn {}",
              db_id, type_id, xid.xid, xid.lsn);

    boost::shared_lock lock(_read_mutex);

    auto info = _get_usertype_info(db_id, type_id, xid);
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2, "info is null? {}", info == nullptr);

    std::shared_ptr<UserType> user_type;
    if (info != nullptr) {
        user_type = std::make_shared<UserType>(info->id,
                                               info->namespace_id,
                                               info->type,
                                               info->name,
                                               info->value_json,
                                               info->exists);
    } else {
        user_type = std::make_shared<UserType>(0, 0, 0, "", "", false);
    }
    return user_type;
}

void
Server::invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    _schema_object_cache->invalidate_table(db_id, table_id, xid);
}

void
Server::invalidate_db(uint64_t db_id, const XidLsn &xid)
{
    _schema_object_cache->invalidate_db(db_id, xid);
}

void
Server::remove_db(uint64_t db_id)
{
    boost::unique_lock write_lock(_write_mutex);
    boost::unique_lock read_lock(_read_mutex);
    boost::unique_lock lock(_mutex);
    {
        boost::unique_lock lock(_xid_mutex);
        _read_xid.erase(db_id);
        _write_xid.erase(db_id);
    }
    _write.erase(db_id);
    _read.erase(db_id);
    _namespace_name_cache.erase(db_id);
    _namespace_id_cache.erase(db_id);
    _usertype_id_cache.erase(db_id);
    _table_cache.erase(db_id);
    _roots_cache.erase(db_id);
    _schema_cache.erase(db_id);
    _index_cache.erase(db_id);
    _schema_object_cache->remove_db(db_id);
}

proto::IndexesInfo
Server::_get_unfinished_indexes_info(uint64_t db_id)
{
    proto::IndexesInfo indexes;
    auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
    auto names_schema = names_t->extent_schema();
    auto names_fields = names_schema->get_fields();

    auto search_key = sys_tbl::IndexNames::Primary::key_tuple(0, 0, 0, 0);
    struct IndexBasicInfo {
        uint64_t index_id;
        XidLsn xid_lsn;
        uint64_t table_id;
    };

    // Build a map <index_id, <xid, IndexBasicInfo>>
    // containing map of unfinished indexes
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, IndexBasicInfo>> unfinished_indexes_map;

    for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
        auto& row = *names_i;
        auto index_id = names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(&row);

        if (index_id == 0) {
            // we dont have to pick primary indexes
            continue;
        }

        auto state = names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(&row);
        auto index_xid = names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row);
        if ((static_cast<sys_tbl::IndexNames::State>(state) ==
                    sys_tbl::IndexNames::State::BEING_DELETED) ||
                (static_cast<sys_tbl::IndexNames::State>(state) ==
                 sys_tbl::IndexNames::State::NOT_READY)) {
            IndexBasicInfo info;
            info.index_id = index_id;
            XidLsn index_xid_lsn(names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row),
                    names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row));
            info.xid_lsn = index_xid_lsn;
            info.table_id = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(&row);

            // Pick the latest unfinished state for the index
            unfinished_indexes_map.erase(index_id);
            unfinished_indexes_map.try_emplace(index_id)
                .first->second
                .try_emplace(index_xid, std::move(info));
        } else {
            unfinished_indexes_map.erase(index_id);
        }
    }

    // From the above map, reorganize them in the form of
    // {xid: [indexes]}  =>  proto::IndexesInfo
    // after fetching full index info (proto::IndexInfo)
    proto::IndexesInfo unfinished_indexes;
    proto::GetIndexInfoRequest index_info_request;
    index_info_request.set_db_id(db_id);

    for (const auto& [index_id, index_entry]: unfinished_indexes_map) {
        auto& [index_xid, index_basic_info] = *index_entry.begin();
        proto::IndexInfoList* index_info_list = nullptr;
        auto it = unfinished_indexes.mutable_xid_index_map()->find(index_xid);
        if (it != unfinished_indexes.mutable_xid_index_map()->end()) {
            // If the xid exists, get the existing IndexInfoList
            index_info_list = &it->second;
        } else {
            // If the xid does not exist, create a new IndexInfoList
            index_info_list = &(*unfinished_indexes.mutable_xid_index_map())[index_xid];
        }

        index_info_request.set_index_id(index_basic_info.index_id);
        index_info_request.set_xid(index_basic_info.xid_lsn.xid);
        index_info_request.set_lsn(index_basic_info.xid_lsn.lsn);
        index_info_request.set_table_id(index_basic_info.table_id);
        auto index_info = _get_index_info(index_info_request);
        _populate_index_columns(db_id, index_info, index_basic_info.xid_lsn);
        *index_info_list->add_indexes() = std::move(index_info);
    }

    return unfinished_indexes;
}

proto::IndexInfo
Server::_get_index_info(const proto::GetIndexInfoRequest& request)
{
    XidLsn xid(request.xid(), request.lsn());
    std::optional<uint64_t> tid;
    if (request.has_table_id()) {
        tid = request.table_id();
    }

    // check the cache
    auto cache_entry = _find_cached_index(request.db_id(), request.index_id(), xid, tid);
    if (cache_entry) {
        return cache_entry->first;
    }

    // read from disk
    auto info = _find_index(request.db_id(), request.index_id(), xid, tid);
    if (!info) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Index not found: {}@{} - {}", request.db_id(),
                            request.xid(), request.index_id());
        proto::IndexInfo dummy;
        dummy.set_id(0);
        return dummy;
    }

    return std::get<0>(*info);
}

bool
Server::_set_index_state(const proto::SetIndexStateRequest& request)
{
    XidLsn xid(request.xid(), request.lsn());

    auto info = std::make_shared<proto::GetSchemaResponse>();
    info->set_access_xid_start(0);
    info->set_access_lsn_start(0);
    info->set_access_xid_end(constant::LATEST_XID);
    info->set_access_lsn_end(constant::MAX_LSN);

    // XXX this is overly heavy-weight to retrieve a specific index
    _read_schema_indexes(info, request.db_id(), request.table_id(), xid);

    auto index_i = std::ranges::find_if(
        info->indexes(), [&](const auto& v) { return request.index_id() == v.id(); });

    if (index_i == info->mutable_indexes()->end()) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Index not found for table {} -- {}", request.table_id(),
                            request.index_id());
        return false;
    }

    // this copies the existing index info
    auto index_info = *index_i;
    CHECK(index_info.table_id() == request.table_id() && index_info.id() == request.index_id());
    index_info.set_state(request.state());

    std::map<uint32_t, uint32_t> keys;
    std::map<uint32_t, std::string> op_classes;
    for (const auto& column : index_info.columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
        op_classes[column.idx_position()] = column.opclass();
    }

    return _upsert_index_name(request.db_id(), index_info, xid, keys, op_classes);
}

bool
Server::_upsert_index_name(uint64_t db_id,
                            const proto::IndexInfo& index_info,
                            const XidLsn& xid,
                            const std::map<uint32_t, uint32_t>& keys,
                            const std::map<uint32_t, std::string>& op_classes,
                            Server::IndexType index_type)
{
    auto index_names_t = _get_mutable_system_table(db_id, sys_tbl::IndexNames::ID);

    auto is_unique = index_info.is_unique();
    if (index_type == Server::IndexType::PRIMARY) {
        is_unique = true;
    }

    std::string index_tree_type(constant::INDEX_TYPE_BTREE);
    if (!index_info.index_type().empty()) {
        index_tree_type = index_info.index_type();
    }
    auto tuple = sys_tbl::IndexNames::Data::tuple(
        index_info.namespace_id(), index_info.name(), index_info.table_id(), index_info.id(), xid.xid, xid.lsn,
        static_cast<sys_tbl::IndexNames::State>(index_info.state()), is_unique, index_tree_type, index_names_t->get_next_internal_row_id());


    // update the index state
    index_names_t->upsert(tuple, constant::UNKNOWN_EXTENT);

    // update columns with the state XID
    if (index_type == Server::IndexType::PRIMARY) {
        if(!keys.empty()) {
            _write_index(xid, db_id, index_info.table_id(), constant::INDEX_PRIMARY, keys, op_classes);
        }
    } else if (index_type == Server::IndexType::LOOKASIDE) {
        _write_index(xid, db_id, index_info.table_id(), constant::INDEX_LOOK_ASIDE, keys, op_classes);
    } else {
        _write_index(xid, db_id, index_info.table_id(), index_info.id(), keys, op_classes);
    }

    // add to index cache
    {
        boost::unique_lock lock(_mutex);
        _index_cache[db_id][index_info.table_id()][index_info.id()].emplace_back(
            xid, index_info);
    }

    return true;
}

proto::IndexInfo
Server::_create_index(const proto::IndexRequest& request, bool &created)
{
    XidLsn xid(request.xid(), request.lsn());

    std::map<uint32_t, uint32_t> keys;
    std::map<uint32_t, std::string> op_classes;
    for (const auto& column : request.index().columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
        op_classes[column.idx_position()] = column.opclass();
    }

    // update index names
    // Create a copy to add namespace ID, before caching the index_info
    auto mutable_index_request = request;

    // lookup the namespace info
    auto ns_info = _get_namespace_info(request.db_id(), request.index().namespace_name(), xid);
    CHECK(ns_info);

    // Set namespace ID for the requested index
    mutable_index_request.mutable_index()->set_namespace_id(ns_info->id);

    if (request.index().index_type() == constant::INDEX_TYPE_GIN || _check_index_columns(request.db_id(), mutable_index_request.index(), keys, xid)) {
        created = _upsert_index_name(request.db_id(), mutable_index_request.index(), xid, keys, op_classes);
    }
    return mutable_index_request.index();
}

bool
Server::_check_index_columns(uint64_t db_id, const proto::IndexInfo & index_info, const std::map<uint32_t, uint32_t> & keys, XidLsn xid)
{
    // get the schema prior to this change
    auto info = _get_schema_info(db_id, index_info.table_id(), xid, xid);
    for (auto [idx_position, column_position]: keys) {
        uint32_t column_type = 0;
        for (auto const& c : info->columns()) {
            if (c.position() == column_position) {
                column_type = c.pg_type();
                break;
            }
        }
        CHECK(column_type != 0) << "Failed to find column at position " << column_position << " for table " << index_info.table_id();
        switch(column_type) {
            case FLOAT8OID:
            case INT8OID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case DATEOID:
            case FLOAT4OID:
            case INT4OID:
            case INT2OID:
            case BOOLOID:
            case CHAROID:
            case UUIDOID:
            case VARCHAROID:
            case TEXTOID:
            // added
            case TEXTARRAYOID:
            case NUMERICOID:
                break;
            default:
                // Support for user-defined types
                if (column_type >= constant::FIRST_USER_DEFINED_PG_OID) {
                    return true;
                }
                LOG_ERROR("Unsupported index column type {} for index column: table {}, index {}",
                    column_type, idx_position,
                    index_info.table_id(), index_info.name());
                return false;
        }
    }
    return true;
}

std::optional<std::tuple<proto::IndexInfo, uint64_t, XidLsn>>
Server::_find_index(uint64_t db_id,
                     uint64_t index_id,
                     const XidLsn& access_xid,
                     std::optional<uint64_t> optional_tid)
{
    // All tables share the same primary index id, and the table id is required in this case.
    // For other indexes we use PG OID that must be unique and tid is optional
    assert(index_id != constant::INDEX_PRIMARY || optional_tid);

    uint64_t tid = optional_tid.value_or(0);

    // use the cached one first

    auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
    auto names_schema = names_t->extent_schema();
    auto names_fields = names_schema->get_fields();

    auto search_key = sys_tbl::IndexNames::Primary::key_tuple(tid, index_id, 0, 0);

    XidLsn index_xid;
    uint64_t table_id = 0;
    bool is_unique = false;
    std::string name;
    uint64_t namespace_id = 0;
    uint8_t state = 0;
    bool found = false;
    std::string index_type;

    // find the last XID for this index
    for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
        auto& row = *names_i;
        uint64_t id = names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(&row);

        if (id < index_id) {
            continue;
        }

        // Skip look aside index entry only if we are not looking for it
        if (id == constant::INDEX_LOOK_ASIDE && index_id != constant::INDEX_LOOK_ASIDE) {
            continue;
        }

        if (index_id != id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "No data found for index {} -- {}", db_id, index_id);
            break;
        }

        uint64_t xid = names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row);
        uint64_t lsn = names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row);
        index_xid = {xid, lsn};
        if (access_xid < index_xid) {
            continue;
        }

            auto found_tid = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(&row);
        if (tid && tid != found_tid) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found a dupoicate index id {} -- {}, {}/{}", db_id,
                                index_id, tid, found_tid);
            break;
        }

        table_id = found_tid;
        is_unique = names_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(&row);
        name = names_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(&row);
        namespace_id = names_fields->at(sys_tbl::IndexNames::Data::NAMESPACE_ID)->get_uint64(&row);
        state = names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(&row);
        index_type = names_fields->at(sys_tbl::IndexNames::Data::INDEX_TYPE)->get_text(&row);

        found = true;
    }

    if (!found) {
        return {};
    }

    proto::IndexInfo info;
    info.set_id(index_id);
    info.set_name(name);
    info.set_is_unique(is_unique);
    info.set_table_id(table_id);
    info.set_state(state);
    info.set_index_type(index_type);

    // need to look up the schema name in the namespace_names table
    auto ns_info = _get_namespace_info(db_id, namespace_id, access_xid, false);
    CHECK(ns_info);
    info.set_namespace_name(ns_info->name);
    info.set_namespace_id(ns_info->id);

    return {{info, namespace_id, index_xid}};
}

void
Server::_drop_index(const XidLsn& xid,
                     uint64_t db_id,
                     uint64_t index_id,
                     std::optional<uint64_t> tid,
                     sys_tbl::IndexNames::State index_state,
                     std::optional<std::reference_wrapper<proto::IndexInfo>> dropped_index_info_ref)
{
    assert(index_state == sys_tbl::IndexNames::State::DELETED || index_state == sys_tbl::IndexNames::State::BEING_DELETED);
    auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
    auto names_schema = names_t->extent_schema();
    auto names_fields = names_schema->get_fields();

    // find the last record for the index id
    auto info = _find_index(db_id, index_id, xid, tid);

    if (!info) {
        LOG_WARN("Drop index not found: {}@{} - {}", db_id, xid.xid, index_id);
        return;
    }
    auto& index_info = std::get<0>(*info);

    // Set table_id in the index info, as the drop index request wont have table ID
    if (dropped_index_info_ref) {
        dropped_index_info_ref->get().set_table_id(index_info.table_id());
    }

    auto state = static_cast<sys_tbl::IndexNames::State>(index_info.state());
    if (state == sys_tbl::IndexNames::State::DELETED ||
        state == sys_tbl::IndexNames::State::BEING_DELETED) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Index already deleted: {}@{} - {}", db_id, xid.xid, index_id);
        return;
    }

    // note: this might not be true during recovery
    // assert(xid > std::get<2>(*info));

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Drop index found {}:{} -- {}", db_id, index_info.table_id(),
                        index_id);
    std::map<uint32_t, uint32_t> keys;
    std::map<uint32_t, std::string> op_classes;
    for (const auto& column : index_info.columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
        op_classes[column.idx_position()] = column.opclass();
    }

    index_info.set_state(static_cast<int32_t>(index_state));
    _upsert_index_name(db_id, index_info, xid, keys, op_classes);
}

bool
Server::_has_parent(uint64_t db_id,
                    uint64_t parent_table_id,
                    const XidLsn& xid,
                    uint64_t child_snapshot_xid)
{
    auto table_info = _get_table_info(db_id, parent_table_id, xid);
    if (!table_info || !table_info->exists) {
        return false;
    }

    // For normal table creates (snapshot_xid == 0), parent just needs to exist
    // For table resyncs (snapshot_xid != 0), parent must have matching snapshot XID
    if (child_snapshot_xid == 0) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                  "Parent {} exists for normal table create (child snapshot_xid=0)",
                  parent_table_id);
        return true;
    }

    // Check that parent's snapshot XID matches child's snapshot XID
    // This prevents attaching a newly resync'd child to an older version of the parent
    auto parent_roots = _get_roots_info(db_id, parent_table_id, xid);
    if (!parent_roots) {
        return false;
    }

    uint64_t parent_snapshot_xid = parent_roots->snapshot_xid();
    bool snapshot_xid_matches = (parent_snapshot_xid == child_snapshot_xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
              "Checking parent {} snapshot XID for resync: parent={}, child={}, match={}",
              parent_table_id, parent_snapshot_xid, child_snapshot_xid, snapshot_xid_matches);

    return snapshot_xid_matches;
}

std::vector<uint64_t>
Server::_get_direct_children(uint64_t db_id,
                             uint64_t parent_table_id,
                             const XidLsn& xid)
{
    std::vector<uint64_t> children;
    std::unordered_set<uint64_t> tables_in_cache;

    // First check _table_cache for uncommitted children
    {
        boost::unique_lock cache_lock(_mutex);
        auto db_cache_iter = _table_cache.find(db_id);
        if (db_cache_iter != _table_cache.end()) {
            for (const auto& [table_id, xid_map] : db_cache_iter->second) {
                // Get the latest version visible at our XID
                auto info_iter = xid_map.lower_bound(xid);
                if (info_iter != xid_map.end()) {
                    // Found a visible version in cache - skip disk entries for this table
                    tables_in_cache.insert(table_id);

                    const auto& table_info = info_iter->second;

                    // Check if this is a child of our parent and exists
                    if (table_info->exists &&
                        table_info->parent_table_id.has_value() &&
                        table_info->parent_table_id.value() == parent_table_id) {
                        children.push_back(table_id);
                    }
                }
            }
        }
    }

    // Now scan disk for children not in cache
    auto table = _get_system_table(db_id, sys_tbl::TableNames::ID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    uint64_t current_table_id = 0;
    std::optional<uint64_t> current_parent_id = std::nullopt;
    bool current_exists = false;
    bool found_any = false;

    auto check_and_add = [&]() {
        if (found_any && current_exists &&
            current_parent_id.has_value() &&
            current_parent_id.value() == parent_table_id &&
            !tables_in_cache.contains(current_table_id)) {  // Skip if in cache
            children.push_back(current_table_id);
        }
    };

    // Full scan using begin() and end()
    for (auto it = table->begin(); it != table->end(); ++it) {
        auto& row = *it;

        uint64_t row_table_id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
        uint64_t row_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
        uint64_t row_lsn = fields->at(sys_tbl::TableNames::Data::LSN)->get_uint64(&row);

        // If we've moved to a new table, check if the previous one was a child
        if (found_any && row_table_id != current_table_id) {
            check_and_add();
            found_any = false;
        }

        // Skip rows that are newer than our XID/LSN
        if (row_xid > xid.xid || (row_xid == xid.xid && row_lsn > xid.lsn)) {
            continue;
        }

        // This is the latest visible version for this table_id
        current_table_id = row_table_id;
        current_exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);

        if (!fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->is_null(&row)) {
            current_parent_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);
        } else {
            current_parent_id = std::nullopt;
        }

        found_any = true;
    }

    // Don't forget to check the last table
    check_and_add();

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
              "Found {} direct children for table {} (cache: {}, disk: {})",
              children.size(), parent_table_id, tables_in_cache.size(),
              children.size() - tables_in_cache.size());

    return children;
}

nlohmann::json
Server::_generate_attach_partition_ddl(uint64_t db_id,
                                       const TableCacheRecordPtr& child_info,
                                       uint64_t parent_table_id,
                                       const XidLsn& xid)
{
    auto parent_info = _get_table_info(db_id, parent_table_id, xid);

    DCHECK(child_info && child_info->exists);
    DCHECK(parent_info && parent_info->exists);
    DCHECK(child_info->partition_bound.has_value());

    auto child_ns = _get_namespace_info(db_id, child_info->namespace_id, xid);
    auto parent_ns = _get_namespace_info(db_id, parent_info->namespace_id, xid);

    nlohmann::json ddl;
    ddl["action"] = "attach_partition";
    ddl["partition_schema"] = child_ns->name;
    ddl["partition_name"] = child_info->name;
    ddl["partition_bound"] = child_info->partition_bound.value();
    ddl["schema"] = parent_ns->name;
    ddl["table"] = parent_info->name;
    ddl["xid"] = xid.xid;
    ddl["lsn"] = xid.lsn;

    return ddl;
}

void
Server::_generate_child_attach_ddls(uint64_t db_id,
                                    uint64_t parent_table_id,
                                    const XidLsn& xid,
                                    uint64_t parent_snapshot_xid,
                                    std::vector<nlohmann::json>& ddl_array_out)
{
    // Find all children that have this table as their parent
    auto children = _get_direct_children(db_id, parent_table_id, xid);

    if (children.empty()) {
        return;  // No waiting children
    }

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
              "Found {} waiting children for table {}", children.size(), parent_table_id);

    for (uint64_t child_id : children) {
        auto child_info = _get_table_info(db_id, child_id, xid);
        if (!child_info || !child_info->exists) {
            continue;
        }

        // Only attach children with matching snapshot XID
        // This prevents attaching old child versions to a newly resync'd parent
        if (child_info->partition_bound.has_value()) {
            auto child_roots = _get_roots_info(db_id, child_id, xid);
            if (!child_roots) {
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                          "Skipping child table {} - no roots info", child_id);
                continue;
            }

            uint64_t child_snapshot_xid = child_roots->snapshot_xid();
            if (child_snapshot_xid != parent_snapshot_xid) {
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                          "Skipping child table {} - snapshot XID mismatch (parent={}, child={})",
                          child_id, parent_snapshot_xid, child_snapshot_xid);
                continue;
            }

            auto attach_ddl = _generate_attach_partition_ddl(
                db_id,
                child_info,
                parent_table_id,
                xid
            );
            ddl_array_out.push_back(attach_ddl);

            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                      "Generated ATTACH DDL for child table {} (snapshot_xid={})",
                      child_id, child_snapshot_xid);
        }
    }
}

nlohmann::json
Server::_create_table(const proto::TableRequest& request)
{
    XidLsn xid(request.xid(), request.lsn());

    // retrieve the id of the namespace
    auto ns_info = _get_namespace_info(request.db_id(), request.table().namespace_name(),
                                       XidLsn(request.xid(), request.lsn()));
    CHECK(ns_info);

    // initialize the ddl statement
    nlohmann::json ddl;
    ddl["action"] = "create";
    ddl["schema"] = request.table().namespace_name();
    ddl["table"] = request.table().name();
    ddl["tid"] = request.table().id();
    ddl["xid"] = request.xid();
    ddl["lsn"] = request.lsn();
    ddl["rls_enabled"] = request.table().rls_enabled();
    ddl["rls_forced"] = request.table().rls_forced();
    ddl["columns"] = nlohmann::json::array();

    // partition info - track for system tables and logic, but don't add to CREATE DDL
    std::optional<uint64_t> parent_table_id = std::nullopt;
    bool has_parent = false;  // Track if direct parent exists

    if (request.table().has_parent_table_id() && request.table().parent_table_id() != constant::INVALID_TABLE) {
        parent_table_id = request.table().parent_table_id();
        // Check if direct parent exists with matching snapshot XID
        has_parent = _has_parent(request.db_id(), parent_table_id.value(), xid, request.snapshot_xid());
        // Note: Don't add parent info to CREATE DDL - it goes in ATTACH PARTITION
    }

    // partition key -- this is a parent table; either root or intermediate
    std::optional<std::string> partition_key = std::nullopt;
    if (request.table().has_partition_key() && !request.table().partition_key().empty()) {
        partition_key = request.table().partition_key();
        ddl["partition_key"] = partition_key.value();
    }

    // partition bound - track for system tables but don't add to CREATE DDL
    // It will be part of ATTACH PARTITION instead
    std::optional<std::string> partition_bound = std::nullopt;
    if (request.table().has_partition_bound() && !request.table().partition_bound().empty()) {
        partition_bound = request.table().partition_bound();
        // Note: Don't add partition_bound to CREATE DDL - it goes in ATTACH PARTITION
    }

    // add table name
    auto table_info =
        std::make_shared<TableCacheRecord>(request.table().id(), request.xid(), request.lsn(),
                                           ns_info->id, request.table().name(),
                                           parent_table_id, partition_key, partition_bound,
                                           request.table().rls_enabled(),
                                           request.table().rls_forced(), true);
    _set_table_info(request.db_id(), table_info);

    // add roots and stats entry -- may get overwritten later if data is added to the table
    auto roots_info = std::make_shared<proto::GetRootsResponse>();
    proto::RootInfo* ri = roots_info->add_roots();
    ri->set_index_id(constant::INDEX_PRIMARY);
    ri->set_extent_id(constant::UNKNOWN_EXTENT);
    proto::RootInfo* la_ri = roots_info->add_roots();
    la_ri->set_index_id(constant::INDEX_LOOK_ASIDE);
    la_ri->set_extent_id(constant::UNKNOWN_EXTENT);
    roots_info->set_snapshot_xid(request.snapshot_xid());

    _set_roots_info(request.db_id(), request.table().id(), xid, roots_info);

    // add schemas entries for each column
    std::vector<proto::ColumnHistory> columns;
    for (const auto& column : request.table().columns()) {
        proto::ColumnHistory& history = columns.emplace_back();
        history.set_xid(xid.xid);
        history.set_lsn(xid.lsn);
        history.set_exists(true);
        history.set_update_type(static_cast<uint8_t>(SchemaUpdateType::NEW_COLUMN));
        *history.mutable_column() = column;

        // store the column data into the json
        nlohmann::json column_json;
        column_json["name"] = column.name();
        column_json["type"] = column.pg_type();
        column_json["type_name"] = column.type_name();
        column_json["type_namespace"] = column.type_namespace();
        column_json["nullable"] = column.is_nullable();
        if (column.has_default_value()) {
            column_json["default"] = column.default_value();
        }

        ddl["columns"].push_back(column_json);
    }

    _set_schema_info(request.db_id(), request.table().id(), ns_info->id, request.table().name(),
                     columns);

    _set_primary_index(request.db_id(), ns_info->id, request.table().id(), request.table().name(),
                       request.table().namespace_name(), xid);

    // Update the table existence cache - append new range since XIDs are monotonic
    {
        boost::unique_lock cache_lock(_table_existence_cache_mutex);
        auto& ranges = _table_existence_cache[request.db_id()][request.table().id()];

        // Always append new range (XIDs are guaranteed monotonic)
        ranges.push_back(TableExistenceRange{xid, XidLsn{constant::LATEST_XID, constant::MAX_LSN}});

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
                  "Updated table existence cache on create: db {} tid {} added range [{},{})-[{},{}) - total {} range(s)",
                  request.db_id(), request.table().id(),
                  xid.xid, xid.lsn,
                  constant::LATEST_XID, constant::MAX_LSN,
                  ranges.size());
    }

    // Build DDL array - always include CREATE TABLE
    std::vector<nlohmann::json> ddl_array;

    // Step 1: Always generate CREATE TABLE DDL
    // (partition_bound and parent info not added - they go in ATTACH PARTITION)
    ddl_array.push_back(ddl);

    // Step 2: If this is a child partition with an existing parent, generate ATTACH PARTITION DDL
    if (partition_bound.has_value() && has_parent) {
        auto child_info = _get_table_info(request.db_id(), request.table().id(), xid);
        DCHECK(child_info != nullptr && child_info->exists);

        auto attach_ddl = _generate_attach_partition_ddl(
            request.db_id(),
            child_info,
            parent_table_id.value(),
            xid
        );
        ddl_array.push_back(attach_ddl);

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                  "Generated ATTACH PARTITION for child table {}", request.table().id());
    }

    // Step 3: If this is a partitioned parent, check for waiting children and attach them
    if (partition_key.has_value()) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1,
                  "Table {} is a partitioned parent - checking for waiting children",
                  request.table().id());

        size_t before_count = ddl_array.size();
        _generate_child_attach_ddls(request.db_id(),
                                    request.table().id(),
                                    xid,
                                    request.snapshot_xid(),
                                    ddl_array);

        if (ddl_array.size() > before_count) {
            LOG_INFO("Generated ATTACH DDLs for {} waiting children of parent table {}",
                     ddl_array.size() - before_count, request.table().id());
        }
    }

    // ALWAYS return a JSON array
    return nlohmann::json(ddl_array);
}

nlohmann::json
Server::_generate_partition_updates(const proto::TableRequest& request,
                                     const proto::ColumnHistory& history)
{
    nlohmann::json ddl;
    std::vector<uint64_t> table_ids;
    for (auto &partition : request.table().partition_data()) {
        table_ids.push_back(partition.table_id());
    }

    ddl["action"] = "resync_partitions";
    ddl["parent_table_id"] = request.table().id();
    ddl["table_ids"] = table_ids;

    return ddl;
}

nlohmann::json
Server::_drop_table(const proto::DropTableRequest& request, bool is_resync)
{
    // retrieve the id of the namespace
    // DROP SCHEMA in PG will automatically drop all tables
    // DropTable may be called after DropNamespace, so we
    // don't check if the namespace was dropped and just use
    // the id of the dropped schema in this case
    auto ns_info = _get_namespace_info(request.db_id(), request.namespace_name(),
                                       XidLsn(request.xid(), request.lsn()), false);

    CHECK(ns_info);

    auto old_table_info = _get_table_info(request.db_id(), request.table_id(), XidLsn(request.xid(), request.lsn()));

    CHECK(old_table_info);

    // initialize the ddl json
    nlohmann::json ddl;
    ddl["action"] = "drop";
    ddl["tid"] = request.table_id();
    ddl["xid"] = request.xid();
    ddl["lsn"] = request.lsn();
    ddl["schema"] = request.namespace_name();
    ddl["table"] = request.name();

    if (old_table_info->parent_table_id.has_value()) {
        ddl["parent_table_id"] = old_table_info->parent_table_id.value();
    }
    if (old_table_info->partition_key.has_value() && !old_table_info->partition_key.value().empty()) {
        ddl["partition_key"] = old_table_info->partition_key.value();
    }
    if (old_table_info->partition_bound.has_value() && !old_table_info->partition_bound.value().empty()) {
        ddl["partition_bound"] = old_table_info->partition_bound.value();
    }

    XidLsn xid(request.xid(), request.lsn());

    // drop indexes

    auto index_info = std::make_shared<proto::GetSchemaResponse>();
    index_info->set_access_xid_start(0);
    index_info->set_access_lsn_start(0);
    index_info->set_access_xid_end(constant::LATEST_XID);
    index_info->set_access_lsn_end(constant::MAX_LSN);

    _read_schema_indexes(index_info, request.db_id(), request.table_id(), xid);

    for (auto const& idx : index_info->indexes()) {
        if (is_resync || idx.id() == constant::INDEX_PRIMARY || idx.id() == constant::INDEX_LOOK_ASIDE) {
            // If resync, all the indexes can be marked as dropped as new ones will be created while copying table
            _drop_index(xid, request.db_id(), idx.id(), request.table_id(), sys_tbl::IndexNames::State::DELETED);
        } else {
            // For secondary indexes (actual drop index requests), indexer will take care of marking them DELETED
            _drop_index(xid, request.db_id(), idx.id(), request.table_id(), sys_tbl::IndexNames::State::BEING_DELETED);
        }
    }

    // mark the table as dropped in the table_names
    // don't have the rls flags, so set them to false, shouldn't matter
    auto table_info = std::make_shared<TableCacheRecord>(
        request.table_id(), request.xid(), request.lsn(), ns_info->id, request.name(), false);
    _set_table_info(request.db_id(), table_info);

    // get the schema prior to this change
    auto info = _get_schema_info(request.db_id(), request.table_id(), xid, xid);

    // remove all of the schema columns
    std::vector<proto::ColumnHistory> changes;
    for (const auto& column : info->columns()) {
        proto::ColumnHistory& change = changes.emplace_back();
        change.set_xid(request.xid());
        change.set_lsn(request.lsn());
        change.set_exists(false);
        change.set_update_type(static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN));
        *change.mutable_column() = column;
    }
    _set_schema_info(request.db_id(), request.table_id(), ns_info->id, request.name(), changes);

    // Update the table existence cache - close the last range since XIDs are monotonic
    {
        boost::unique_lock cache_lock(_table_existence_cache_mutex);
        auto db_it = _table_existence_cache.find(request.db_id());
        if (db_it != _table_existence_cache.end()) {
            auto table_it = db_it->second.find(request.table_id());
            if (table_it != db_it->second.end() && !table_it->second.empty()) {
                // Update the last (most recent) range's end XID (XIDs are guaranteed monotonic)
                auto& last_range = table_it->second.back();
                last_range.end_xid_lsn = xid;

                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
                          "Updated table existence cache on drop: db {} tid {} closed range [{},{})-[{},{}) - total {} range(s)",
                          request.db_id(), request.table_id(),
                          last_range.start_xid_lsn.xid, last_range.start_xid_lsn.lsn,
                          last_range.end_xid_lsn.xid, last_range.end_xid_lsn.lsn,
                          table_it->second.size());
            }
        }
    }

    return ddl;
}

nlohmann::json
Server::_mutate_namespace(
    uint64_t db_id, uint64_t ns_id, std::optional<std::string> name, const XidLsn& xid, bool exists)
{
    // construct the DDL to provide to the FDW
    nlohmann::json ddl;
    ddl["id"] = ns_id;
    ddl["xid"] = xid.xid;
    ddl["lsn"] = xid.lsn;
    if (name) {
        ddl["name"] = *name;
    }

    // record the namespace info into the cache
    {
        boost::unique_lock lock(_mutex);
        auto entry = std::make_shared<NamespaceCacheRecord>(ns_id, name.value_or(""), exists);
        _namespace_id_cache[db_id][ns_id][xid] = entry;
        if (name) {
            // XXX do we need to make sure we cache the name even if it's not provided?  e.g.,
            //     in the case of drop?
            _namespace_name_cache[db_id][*name][xid] = entry;
        }
    }

    // add the namespace to the namespace_names table
    auto table = _get_mutable_system_table(db_id, sys_tbl::NamespaceNames::ID);
    auto tuple =
        sys_tbl::NamespaceNames::Data::tuple(ns_id, name.value_or(""), xid.xid, xid.lsn, exists, table->get_next_internal_row_id());
    table->upsert(tuple, constant::UNKNOWN_EXTENT);

    return ddl;
}

void
Server::_update_roots(const proto::UpdateRootsRequest& request)
{
    XidLsn xid(request.xid());

    auto info = std::make_shared<proto::GetRootsResponse>();
    *info->mutable_roots() = request.roots();
    *info->mutable_stats() = request.stats();
    info->set_snapshot_xid(request.snapshot_xid());

    _set_roots_info(request.db_id(), request.table_id(), xid, info);
}

XidLsn
Server::_get_read_xid(uint64_t db_id)
{
    boost::unique_lock lock(_xid_mutex);
    auto read_i = _read_xid.find(db_id);
    if (read_i != _read_xid.end()) {
        return read_i->second;
    }

    auto xid_mgr = xid_mgr::XidMgrServer::get_instance();
    auto xid = xid_mgr->get_committed_xid(db_id, 0);

    _read_xid[db_id] = XidLsn(xid);
    _write_xid[db_id] = xid + 1;

    return XidLsn(xid);
}

uint64_t
Server::_get_write_xid(uint64_t db_id)
{
    boost::unique_lock lock(_xid_mutex);
    auto write_i = _write_xid.find(db_id);
    if (write_i != _write_xid.end()) {
        return write_i->second;
    }

    auto xid_mgr = xid_mgr::XidMgrServer::get_instance();
    auto xid = xid_mgr->get_committed_xid(db_id, 0);

    _read_xid[db_id] = XidLsn(xid);
    _write_xid[db_id] = xid + 1;

    return xid + 1;
}

void
Server::_set_xids(uint64_t db_id, const XidLsn& read_xid, uint64_t write_xid)
{
    boost::unique_lock lock(_xid_mutex);
    _read_xid[db_id] = read_xid;
    _write_xid[db_id] = write_xid;
}

std::vector<uint64_t>
Server::_get_modified_partition_details(uint64_t db_id,
                                         const XidLsn &xid,
                                         uint64_t table_id,
                                         const google::protobuf::RepeatedPtrField<proto::PartitionData> &partition_data,
                                         std::unordered_map<uint64_t, std::pair<std::string, std::string>> *partition_map,
                                         bool is_attached)
{
    auto table = _get_system_table(db_id, sys_tbl::TableNames::ID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    // Mutex for the table cache
    boost::shared_lock lock(_mutex);

    std::unordered_map<uint64_t, uint64_t> system_table_ids;

    // Iterate the table_names table and find the child tables who has the parent_table_id as the current_table_id
    for (auto row : (*table)) {
        auto tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
        auto parent_table_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);
        auto table_xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);

        // make sure that the table is marked as existing at this XID/LSN
        bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
        if (!exists) {
            LOG_WARN("Table {} marked non-existant at xid {}:{}", tid, xid.xid, xid.lsn);
            continue;
        }

        if ( system_table_ids.contains(tid) && system_table_ids[tid] < table_xid) {
            // Remove the existing entry if the XID is less than the current processing XID
            system_table_ids.erase(tid);
        }
        if (parent_table_id == table_id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Adding disk table {}:{} to system_table_ids", tid, fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row));
            system_table_ids.try_emplace(tid, table_xid);
        }
    }

    // Validate the system table ids from cache
    // Only add the tables that have the right table parent table id
    for (const auto& [tid, xid_map] : _table_cache[db_id]) {
        const auto& [latest_key, latest_table_ptr] = *xid_map.begin();

        // Add the table id to the system_table_ids set if the parent table id matches the current table id
        // Remove the table id from the system_table_ids set if the cache doesn't have the table info
        if (latest_table_ptr->parent_table_id == table_id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Adding cached table {}:{} to system_table_ids", tid, latest_table_ptr->name);
            system_table_ids.try_emplace(tid, latest_table_ptr->xid);
        } else {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Removing table {}:{} from system_table_ids", tid, latest_table_ptr->name);
            system_table_ids.erase(tid);
        }
    }

    // Parse the requested table ids
    std::unordered_set<uint64_t> table_ids;
    for ( const auto &part_data : partition_data ) {
        // Validate the partition data parent table id to match the current table_id
        if (part_data.parent_table_id() != table_id) {
            LOG_WARN("Parent table id {} does not match the current table id {}",
                part_data.parent_table_id(), table_id);
            continue;
        }
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Adding partition table {}:{} to table_ids", part_data.table_id(), part_data.table_name());
        table_ids.insert(part_data.table_id());
        if (partition_map != nullptr) {
            partition_map->insert(std::make_pair(
                part_data.table_id(), std::make_pair(
                    part_data.partition_bound(),
                    part_data.partition_key()
                )
            ));
        }
    }

    // Get the different in order to identify the attached partition
    std::vector<uint64_t> result;

    if ( is_attached ) {
        // Get the difference in order to identify the attached partition
        for (const auto& tid : table_ids) {
            if (!system_table_ids.contains(tid)) {
                result.push_back(tid);
            }
        }
    } else {
        // Get the difference in order to identify the detached partition
        for (const auto& tid : system_table_ids) {
            if (!table_ids.contains(tid.first)) {
                result.push_back(tid.first);
            }
        }
    }

    return result;
}

nlohmann::json
Server::_mutate_usertype(uint64_t db_id,
                          uint64_t type_id,
                          const std::string &name,
                          uint64_t ns_id,
                          int8_t type,
                          const std::string &value_json,
                          const XidLsn xid,
                          bool exists)
{
    // construct the DDL to provide to the FDW
    nlohmann::json ddl;
    ddl["id"] = type_id;
    ddl["xid"] = xid.xid;
    ddl["lsn"] = xid.lsn;
    ddl["name"] = name;

    if (exists) {
        // these are not set for drop when exists is false
        ddl["value"] = value_json;
        ddl["type"] = type;
        ddl["namespace_id"] = ns_id;
    }

    // record the user defined type info into the cache
    {
        boost::unique_lock lock(_mutex);
        auto entry = std::make_shared<UserTypeCacheRecord>(type_id, name, ns_id, type, value_json, exists);
        _usertype_id_cache[db_id][type_id][xid] = entry;
    }

    // add the type to the user types table
    auto table = _get_mutable_system_table(db_id, sys_tbl::UserTypes::ID);
    auto tuple =
        sys_tbl::UserTypes::Data::tuple(type_id, ns_id, name, value_json, xid.xid, xid.lsn, type, exists, table->get_next_internal_row_id());
    table->upsert(tuple, constant::UNKNOWN_EXTENT);

    return ddl;
}

Server::UserTypeCacheRecordPtr
Server::_get_usertype_info(uint64_t db_id, uint64_t type_id, const XidLsn& xid)
{
    // check the cache of un-finalized records
    {
        boost::unique_lock lock(_mutex);
        auto user_type_i = _usertype_id_cache[db_id].find(type_id);
        if (user_type_i != _usertype_id_cache[db_id].end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = user_type_i->second.lower_bound(xid);
            if (info_i != user_type_i->second.end()) {
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found user type {} in cache at exists {}", type_id, info_i->second->exists);
                return info_i->second;
            }
        }
    }

    // read from disk
    auto table = _get_system_table(db_id, sys_tbl::UserTypes::ID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::UserTypes::Primary::key_tuple(type_id, xid.xid, xid.lsn);
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2, "Searching for user type {} at {}:{}", type_id, xid.xid, xid.lsn);

    // find the row that matches the type_id at the given XID/LSN
    auto row_i = table->inverse_lower_bound(search_key);
    auto &&row = *row_i;

    // make sure type ID exists at this XID/LSN
    auto id_field = fields->at(sys_tbl::UserTypes::Data::TYPE_ID);
    if (row_i == table->end()) {
        LOG_WARN("No user rows for search key type {} at xid {}:{}", type_id, xid.xid, xid.lsn);
        return nullptr;
    }

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2, "Found user type id: {}", id_field->get_uint64(&row));
    if (row_i == table->end() || id_field->get_uint64(&row) != type_id) {
        LOG_WARN("No user type info at xid {}:{}", xid.xid, xid.lsn);
        return nullptr;
    }

    // make sure that the usertype is marked as existing at this XID/LSN
    bool exists = fields->at(sys_tbl::UserTypes::Data::EXISTS)->get_bool(&row);
    if (!exists) {
        LOG_WARN("User type marked non-existant at xid {}:{}", xid.xid, xid.lsn);
        return nullptr;
    }

    // create and populate the user type info
    return std::make_shared<UserTypeCacheRecord>(
        type_id,
        fields->at(sys_tbl::UserTypes::Data::NAME)->get_text(&row),
        fields->at(sys_tbl::UserTypes::Data::NAMESPACE_ID)->get_uint64(&row),
        fields->at(sys_tbl::UserTypes::Data::TYPE)->get_uint8(&row),
        fields->at(sys_tbl::UserTypes::Data::VALUE)->get_text(&row),
        fields->at(sys_tbl::UserTypes::Data::EXISTS)->get_bool(&row));
}

Server::TableCacheRecordPtr
Server::_get_table_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
{
    // check the cache
    boost::unique_lock lock(_mutex);
    auto table_i = _table_cache[db_id].find(table_id);
    if (table_i != _table_cache[db_id].end()) {
        // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
        auto info_i = table_i->second.lower_bound(xid);
        if (info_i != table_i->second.end()) {
            return info_i->second;
        }
    }
    lock.unlock();

    // not present, read from disk
    auto table_names_t = _get_system_table(db_id, sys_tbl::TableNames::ID);
    auto schema = table_names_t->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::TableNames::Primary::key_tuple(table_id, xid.xid, xid.lsn);

    // find the row that matches the name of the table_id at the given XID/LSN
    auto row_i = table_names_t->inverse_lower_bound(search_key);
    auto &&row = *row_i;

    // make sure table ID exists at this XID/LSN
    if (row_i == table_names_t->end() ||
        fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row) != table_id) {
        LOG_WARN("No table info for {} at xid {}:{}", table_id, xid.xid, xid.lsn);
        return nullptr;
    }

    // make sure that the table is marked as existing at this XID/LSN
    bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);
    if (!exists) {
        LOG_WARN("Table {} marked non-existant at xid {}:{}", table_id, xid.xid, xid.lsn);
        return nullptr;
    }

    // read the row from the extent and retrieve the FQN
    auto info = std::make_shared<TableCacheRecord>();
    info->id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
    info->xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
    info->lsn = fields->at(sys_tbl::TableNames::Data::LSN)->get_uint64(&row);
    info->namespace_id = fields->at(sys_tbl::TableNames::Data::NAMESPACE_ID)->get_uint64(&row);
    info->name = fields->at(sys_tbl::TableNames::Data::NAME)->get_text(&row);
    info->rls_enabled = fields->at(sys_tbl::TableNames::Data::RLS_ENABLED)->get_bool(&row);
    info->rls_forced = fields->at(sys_tbl::TableNames::Data::RLS_FORCED)->get_bool(&row);
    info->exists = exists;

    std::optional<uint64_t> parent_table_id = std::nullopt;
    if (!fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->is_null(&row)) {
        parent_table_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);
    }
    std::optional<std::string> partition_key = std::nullopt;
    if (!fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->is_null(&row)) {
        partition_key = fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->get_text(&row);
    }
    std::optional<std::string> partition_bound = std::nullopt;
    if (!fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->is_null(&row)) {
        partition_bound = fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->get_text(&row);
    }
    info->parent_table_id = parent_table_id;
    info->partition_key = partition_key;
    info->partition_bound = partition_bound;

    // note: we currently only keep un-finalized mutations in the cache, so don't cache here
    return info;
}

bool
Server::_populate_table_existence_cache(uint64_t db_id, uint64_t table_id)
{
    // Preconditions: Caller holds boost::unique_lock on _table_existence_cache_mutex AND boost::shared_lock on _read_mutex

    TableExistenceRanges ranges;

    // PHASE 1: Scan finalized entries from TableNames system table
    {
        auto table_names_t = _get_system_table(db_id, sys_tbl::TableNames::ID);
        auto schema = table_names_t->extent_schema();
        auto fields = schema->get_fields();

        // Create search key for this table_id at XID=0, LSN=0 to find the first entry
        auto search_key = sys_tbl::TableNames::Primary::key_tuple(table_id, 0, 0);

        XidLsn range_start{0, 0};
        bool in_range = false;
        bool found_any = false;

        // Scan all entries for this table_id and build ranges
        for (auto row_i = table_names_t->lower_bound(search_key); row_i != table_names_t->end(); ++row_i) {
            auto& row = *row_i;

            // Check if we're still looking at the same table_id
            uint64_t current_table_id = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
            if (current_table_id != table_id) {
                // We've moved past this table's entries
                break;
            }

            // Get the XID/LSN and exists flag for this entry
            uint64_t xid = fields->at(sys_tbl::TableNames::Data::XID)->get_uint64(&row);
            uint64_t lsn = fields->at(sys_tbl::TableNames::Data::LSN)->get_uint64(&row);
            bool exists = fields->at(sys_tbl::TableNames::Data::EXISTS)->get_bool(&row);

            found_any = true;

            if (exists && !in_range) {
                // Start of new range
                range_start = XidLsn{xid, lsn};
                in_range = true;
            } else if (!exists && in_range) {
                // End of current range
                ranges.push_back(TableExistenceRange{range_start, XidLsn{xid, lsn}});
                in_range = false;
            }
        }

        // Handle open range (table still exists)
        if (in_range) {
            ranges.push_back(TableExistenceRange{range_start, XidLsn{constant::LATEST_XID, constant::MAX_LSN}});
        }

        if (!found_any) {
            // No finalized entries - check _table_cache for unfinalized entries
            // (fall through to PHASE 2)
        }
    }

    // PHASE 2: Check _table_cache for unfinalized entries
    // These will always be at XID/LSN after the finalized entries
    {
        boost::unique_lock table_cache_lock(_mutex);
        auto table_i = _table_cache[db_id].find(table_id);
        if (table_i != _table_cache[db_id].end()) {
            // Iterate through _table_cache entries in chronological order
            // Note: _table_cache stores entries in REVERSE order for lower_bound lookup
            // So we iterate backwards to get chronological order
            for (auto it = table_i->second.rbegin(); it != table_i->second.rend(); ++it) {
                const auto& xid_lsn = it->first;
                bool exists = it->second->exists;

                if (exists && (ranges.empty() || ranges.back().end_xid_lsn != XidLsn{constant::LATEST_XID, constant::MAX_LSN})) {
                    // Start new range
                    ranges.push_back(TableExistenceRange{xid_lsn, XidLsn{constant::LATEST_XID, constant::MAX_LSN}});
                } else if (!exists && !ranges.empty() && ranges.back().end_xid_lsn == XidLsn{constant::LATEST_XID, constant::MAX_LSN}) {
                    // Close the last range
                    ranges.back().end_xid_lsn = xid_lsn;
                }
            }
        }
    }

    // If we didn't find any entries at all, return false
    if (ranges.empty()) {
        return false;
    }

    // Store ranges in cache
    _table_existence_cache[db_id][table_id] = std::move(ranges);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
              "Populated table existence cache: db {} table {} with {} range(s)",
              db_id, table_id, _table_existence_cache[db_id][table_id].size());

    return true;
}

std::optional<bool>
Server::_check_table_existence_cache(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
{
    // Preconditions: Caller holds boost::shared_lock on _table_existence_cache_mutex

    auto db_it = _table_existence_cache.find(db_id);
    if (db_it == _table_existence_cache.end()) {
        return std::nullopt;
    }

    auto table_it = db_it->second.find(table_id);
    if (table_it == db_it->second.end()) {
        return std::nullopt;
    }

    const auto& ranges = table_it->second;
    if (ranges.empty()) {
        return std::nullopt;
    }

    // Check if xid falls within any of the ranges
    // Ranges are sorted by start_xid_lsn, so we iterate through them
    for (const auto& range : ranges) {
        // Table exists if xid is in [start_xid_lsn, end_xid_lsn)
        // Note: end_xid_lsn is exclusive
        if (xid >= range.start_xid_lsn && xid < range.end_xid_lsn) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
                      "Table existence cache hit: db {} tid {} xid {}:{} exists=true (range [{},{})-[{},{}))",
                      db_id, table_id, xid.xid, xid.lsn,
                      range.start_xid_lsn.xid, range.start_xid_lsn.lsn,
                      range.end_xid_lsn.xid, range.end_xid_lsn.lsn);
            return true;
        }
    }

    // Not in any range
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2,
              "Table existence cache hit: db {} tid {} xid {}:{} exists=false ({} ranges checked)",
              db_id, table_id, xid.xid, xid.lsn, ranges.size());
    return false;
}

Server::NamespaceCacheRecordPtr
Server::_get_namespace_info(uint64_t db_id, uint64_t namespace_id, const XidLsn& xid, bool check_exists)
{
    // check the cache of un-finalized records
    {
        boost::unique_lock lock(_mutex);
        auto namespace_i = _namespace_id_cache[db_id].find(namespace_id);
        if (namespace_i != _namespace_id_cache[db_id].end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = namespace_i->second.lower_bound(xid);
            if (info_i != namespace_i->second.end()) {
                return info_i->second;
            }
        } else {
            auto it = _last_namespace_update_by_id.find(db_id);
            if (it != _last_namespace_update_by_id.end()) {
                auto const& [last_xid, ns] = it->second;
                if (last_xid <= xid.xid) {
                    auto ns_it = ns.find(namespace_id);
                    if (ns_it != ns.end()) {
                        return ns_it->second;
                    }
                }
            }
        }
    }

    // read from disk
    auto table = _get_system_table(db_id, sys_tbl::NamespaceNames::ID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::NamespaceNames::Primary::key_tuple(namespace_id, xid.xid, xid.lsn);

    // find the row that matches the namespace_id at the given XID/LSN
    auto row_i = table->inverse_lower_bound(search_key);
    auto &&row = *row_i;

    // make sure table ID exists at this XID/LSN
    auto id_field = fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID);
    if (row_i == table->end() || id_field->get_uint64(&row) != namespace_id) {
        LOG_WARN("No namespace info at xid {}:{}", xid.xid, xid.lsn);
        return nullptr;
    }

    // make sure that the table is marked as existing at this XID/LSN
    if (check_exists) {
        bool exists = fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row);
        if (!exists) {
            LOG_WARN("Namespace marked non-existant at xid {}:{}", xid.xid, xid.lsn);
            return nullptr;
        }
    }

    // create and populate the namespace info
    auto info =  std::make_shared<NamespaceCacheRecord>(
        namespace_id, fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row),
        fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row));

    // save the last read info
    {
        boost::unique_lock lock(_mutex);
        auto it = _last_namespace_update_by_id.find(db_id);
        if (it != _last_namespace_update_by_id.end()) {
            auto& [last_xid, ns] = it->second;
            if (xid.xid >= last_xid) {
                ns[namespace_id] = info;
            }
        }
    }
    return info;
}

Server::NamespaceCacheRecordPtr
Server::_get_namespace_info(uint64_t db_id, const std::string& name, const XidLsn& xid, bool check_exists)
{
    // check the cache of un-finalized records
    {
        boost::unique_lock lock(_mutex);
        auto namespace_i = _namespace_name_cache[db_id].find(name);
        if (namespace_i != _namespace_name_cache[db_id].end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = namespace_i->second.lower_bound(xid);
            if (info_i != namespace_i->second.end()) {
                return info_i->second;
            }
        } else {
            auto it = _last_namespace_update_by_name.find(db_id);
            if (it != _last_namespace_update_by_name.end()) {
                auto const& [last_xid, ns] = it->second;
                if (last_xid <= xid.xid) {
                    auto ns_it = ns.find(name);
                    if (ns_it != ns.end()) {
                        return ns_it->second;
                    }
                }
            }
        }
    }

    // read from disk
    auto table = _get_system_table(db_id, sys_tbl::NamespaceNames::ID);

    // check if the table is empty
    // note: this is a hack to get around the fact that doing a secondary index search on a
    //       vacant table is broken right now, otherwise we could follow the main path.  See
    //       ticket SPR-520.
    if (table->empty()) {
        LOG_WARN("Namespace names table empty at xid {}:{}", xid.xid, xid.lsn);
        return nullptr;
    }

    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::NamespaceNames::Secondary::key_tuple(name, xid.xid, xid.lsn);

    // find the row that matches the name at the given XID/LSN
    auto row_i = table->inverse_lower_bound(search_key, 1);
    auto &&row = *row_i;

    // verify that the name is present and exists
    if (row_i == table->end(1)) {
        LOG_WARN("Couldn't find entry for namespace {} @ {}:{}", name, xid.xid, xid.lsn);
        return nullptr;
    }

    if (name != fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row)) {
        LOG_WARN("Couldn't find entry for namespace {} @ {}:{}", name, xid.xid, xid.lsn);
        return nullptr;
    }
    if (check_exists && !fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row)) {
        LOG_WARN("Namespace marked as not-exists {} @ {}:{}", name, xid.xid, xid.lsn);
        return nullptr;
    }

    // return the namespace info
    auto info = std::make_shared<NamespaceCacheRecord>(
        fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&row), name,
        fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row));

    // save the last read info
    {
        boost::unique_lock lock(_mutex);
        auto it = _last_namespace_update_by_name.find(db_id);
        if (it != _last_namespace_update_by_name.end()) {
            auto& [last_xid, ns] = it->second;
            if (xid.xid >= last_xid) {
                ns[name] = info;
            }
        }
    }
    return info;
}

void
Server::_set_table_info(uint64_t db_id, TableCacheRecordPtr table_info)
{
    XidLsn xid(table_info->xid, table_info->lsn);

    // update the cache
    boost::unique_lock lock(_mutex);
    _table_cache[db_id][table_info->id][xid] = table_info;
    lock.unlock();

    // record the change to the system table
    auto table_names_t = _get_mutable_system_table(db_id, sys_tbl::TableNames::ID);
    auto tuple =
        sys_tbl::TableNames::Data::tuple(table_info->namespace_id, table_info->name, table_info->id,
                                         table_info->xid, table_info->lsn, table_info->exists,
                                         table_info->parent_table_id, table_info->partition_key,
                                         table_info->partition_bound, table_info->rls_enabled,
                                         table_info->rls_forced, table_names_t->get_next_internal_row_id());
    table_names_t->upsert(tuple, constant::UNKNOWN_EXTENT);
}

void
Server::_clear_table_info(uint64_t db_id)
{
    // clear the table cache since it only contains un-finalized entries
    boost::unique_lock lock(_mutex);
    _table_cache.erase(db_id);
}

void
Server::_clear_namespace_info(uint64_t db_id, uint64_t xid)
{
    boost::unique_lock lock(_mutex);
    if (_namespace_id_cache[db_id].empty() && _namespace_name_cache[db_id].empty()) {
        return;
    }
    // mark that the namespace needs to be re-read at this XID
    _last_namespace_update_by_id[db_id] = {xid, {}};
    _last_namespace_update_by_name[db_id] = {xid, {}};

    _namespace_name_cache.erase(db_id);
    _namespace_id_cache.erase(db_id);
}

Server::RootsCacheRecordPtr
Server::_get_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
{
    // possibly cached stats
    uint64_t row_count = 0;
    uint64_t end_offset = 0;
    uint64_t last_internal_row_id = 0;
    bool stats_found = false;

    // get cached roots
    std::optional<XidLsnToRootsInfoMap> cached_roots;
    {
        boost::shared_lock lock(_mutex);

        const auto it = _roots_cache.find(db_id);
        if (it != _roots_cache.end()) {
            const auto roots_i = it->second.find(table_id);
            if (roots_i != it->second.end()) {
                cached_roots = roots_i->second;
            }
        }
    }

    uint64_t snapshot_xid = 0;

    // this is just a  helper function to find cached roots by index
    auto find_cached_root = [&](uint64_t index_id) -> std::optional<uint64_t> {
        for (const auto& xid_roots: *cached_roots) {
            // the XidLsnToRootsInfoMap is ordered by XID is revese order (latest first)
            // so we just need to find the first match
            if (xid_roots.first > xid) {
                continue;
            }
            // we found a cached entry, update the stats
            row_count = xid_roots.second->stats().row_count();
            last_internal_row_id = xid_roots.second->stats().last_internal_row_id();
            stats_found = true;
            if (index_id == constant::INDEX_PRIMARY) {
                end_offset = xid_roots.second->stats().end_offset();
            }
            auto it = std::ranges::find_if(xid_roots.second->roots(), [index_id](const auto& v) {
                        return index_id == v.index_id();
                    }
            );
            if (it != xid_roots.second->roots().end()) {
                snapshot_xid = xid_roots.second->snapshot_xid();
                return it->extent_id();
            }
        }
        return {};
    };

    auto info = std::make_shared<proto::GetSchemaResponse>();
    info->set_access_xid_start(0);
    info->set_access_lsn_start(0);
    info->set_access_xid_end(constant::LATEST_XID);
    info->set_access_lsn_end(constant::MAX_LSN);

    // get the index states
    _read_schema_indexes(info, db_id, table_id, xid);

    // go over each index and get the roots
    auto roots_t = _get_system_table(db_id, sys_tbl::TableRoots::ID);
    auto schema = roots_t->extent_schema();
    auto table_id_f = schema->get_field("table_id");

    auto index_id_f = schema->get_field("index_id");
    auto end_offset_f = schema->get_field("end_offset");

    const std::string& sxid =
        sys_tbl::TableRoots::Data::SCHEMA[sys_tbl::TableRoots::Data::SNAPSHOT_XID].name;
    auto sxid_f = schema->get_field(sxid);

    auto eid_f = schema->get_field("extent_id");
    auto xid_f = schema->get_field("xid");

    auto roots_info = std::make_shared<proto::GetRootsResponse>();

    for (const auto& idx: info->indexes()) {
        if (static_cast<sys_tbl::IndexNames::State>(idx.state()) != sys_tbl::IndexNames::State::READY &&
                static_cast<sys_tbl::IndexNames::State>(idx.state()) != sys_tbl::IndexNames::State::BEING_DELETED) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Index deleted or not-ready, so skipping the root {} -- {}",
                      table_id, idx.id());
            continue;
        }

        // check the cache first
        if (cached_roots){
            auto extent_id = find_cached_root(idx.id());
            if (extent_id.has_value()) {
                proto::RootInfo ri;
                ri.set_index_id(idx.id());
                ri.set_extent_id(*extent_id);
                *roots_info->add_roots() = ri;

                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found cached root: {} {}", idx.id(), extent_id);
                continue; // use cached data and go to the next index
            }
        }

        // not found in cached roots, go to the table
        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, idx.id(), xid.xid);
        auto row_i = roots_t->inverse_lower_bound(search_key);

        if (row_i == roots_t->end()) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Couldn't find search key: {} {} {}", table_id, idx.id(), xid.xid);
            continue;
        }

        auto &&row = *row_i;
        if (table_id_f->get_uint64(&row) != table_id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Wrong table ID: {} != {}", table_id_f->get_uint64(&row), table_id);
            continue;
        }
        if (index_id_f->get_uint64(&row) != idx.id()) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Wrong index ID: {} != {}", index_id_f->get_uint64(&row), idx.id());
            continue;
        }

        auto record_xid = xid_f->get_uint64(&row);
        if (record_xid > xid.xid) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Wrong XID: {} > {}", record_xid, xid.xid);
            continue;
        }

        if (idx.id() == constant::INDEX_PRIMARY) {
            end_offset = end_offset_f->get_uint64(&row);
        }

        proto::RootInfo ri;
        ri.set_index_id(idx.id());
        ri.set_extent_id(eid_f->get_uint64(&row));
        *roots_info->add_roots() = ri;

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG2, "Found root in table: {} {}", idx.id(), eid_f->get_uint64(&row));

        // use snapshot_xid of the last row
        snapshot_xid = sxid_f->get_uint64(&row);
    }

    if (roots_info->roots().empty()) {
        LOG_WARN("Couldn't find table_roots entry for {}@{}:{}", table_id, xid.xid,
                    xid.lsn);
    }

    if (!stats_found) {
        boost::shared_lock lock(_mutex);
        auto it_db = _last_table_stats_update.find(db_id);
        if (it_db != _last_table_stats_update.end()) {
            auto const& [last_xid, tables] = it_db->second;
            if (last_xid <= xid.xid) {
                auto tab_it = tables.find(table_id);
                if (tab_it != tables.end()) {
                    row_count = tab_it->second.row_count;
                    last_internal_row_id = tab_it->second.last_internal_row_id;
                    stats_found = true;
                }
            }
        }
    }

    // access the stats table if we didn't find cached stats
    if (!stats_found) {
        auto stats_t = _get_system_table(db_id, sys_tbl::TableStats::ID);
        auto stats_schema = stats_t->extent_schema();
        auto stats_key_fields = stats_schema->get_sort_fields();

        auto search_key = sys_tbl::TableStats::Primary::key_tuple(table_id, xid.xid);
        auto srow_i = stats_t->inverse_lower_bound(search_key);
        auto &&row = *srow_i;

        // need to confirm that the table ID matches, but the XID may not match
        table_id_f = stats_schema->get_field("table_id");
        if (srow_i != stats_t->end() && table_id_f->get_uint64(&row) == table_id) {
            // retrieve the stats from the row
            auto row_count_f = stats_schema->get_field("row_count");
            auto last_internal_row_id_f = stats_schema->get_field("last_internal_row_id");
            row_count = row_count_f->get_uint64(&row);
            last_internal_row_id = last_internal_row_id_f->get_uint64(&row);
        } else {
            // no stats for this table?  seems like a potential error
            LOG_WARN("Couldn't find table_stats entry for {}@{}:{}", table_id, xid.xid, xid.lsn);
        }
    }

    roots_info->mutable_stats()->set_row_count(row_count);
    roots_info->mutable_stats()->set_end_offset(end_offset);
    roots_info->mutable_stats()->set_last_internal_row_id(last_internal_row_id);
    roots_info->set_snapshot_xid(snapshot_xid);

    return roots_info;
}

void
Server::_set_roots_info(uint64_t db_id,
                         uint64_t table_id,
                         const XidLsn& xid,
                         RootsCacheRecordPtr roots_info)
{
    // cache the roots info
    {
        boost::unique_lock lock(_mutex);
        _roots_cache[db_id][table_id][xid] = roots_info;
    }

    // update the table_roots
    auto table_roots_t = _get_mutable_system_table(db_id, sys_tbl::TableRoots::ID);
    for (auto const& r : roots_info->roots()) {
        auto tuple = sys_tbl::TableRoots::Data::tuple(table_id, r.index_id(), xid.xid,
                                                      r.extent_id(), roots_info->snapshot_xid(),
                                                      roots_info->stats().end_offset(),
                                                      table_roots_t->get_next_internal_row_id());
        table_roots_t->upsert(tuple, constant::UNKNOWN_EXTENT);

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Updated root {}@{}:{} {} - {}", table_id, xid.xid, xid.lsn,
                            r.index_id(), r.extent_id());
    }

    {
        boost::unique_lock lock(_mutex);
        auto& [last_xid, tables] = _last_table_stats_update[db_id];
        last_xid = xid.xid; // record the last XID we updated the stats at
        auto& table_stats = tables[table_id];
        table_stats.row_count = roots_info->stats().row_count();
        table_stats.last_internal_row_id = roots_info->stats().last_internal_row_id();
    }

    auto table_stats_t = _get_mutable_system_table(db_id, sys_tbl::TableStats::ID);
    auto tuple =
        sys_tbl::TableStats::Data::tuple(table_id, xid.xid, roots_info->stats().row_count(),
                roots_info->stats().last_internal_row_id(), table_stats_t->get_next_internal_row_id());
    table_stats_t->upsert(tuple, constant::UNKNOWN_EXTENT);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Updated stats {}@{}:{} - {}", table_id, xid.xid, xid.lsn,
                        roots_info->stats().row_count());
}

void
Server::_clear_roots_info(uint64_t db_id)
{
    // note: we clear everything because the cache only contains un-finalized data
    boost::unique_lock lock(_mutex);
    _roots_cache.erase(db_id);
}

Server::SchemaInfoPtr
Server::_get_schema_info(uint64_t db_id,
                          uint64_t table_id,
                          const XidLsn& access_xid,
                          const XidLsn& target_xid)
{
    auto info = std::make_shared<proto::GetSchemaResponse>();
    info->set_access_xid_start(0);
    info->set_access_lsn_start(0);
    info->set_access_xid_end(constant::LATEST_XID);
    info->set_access_lsn_end(constant::MAX_LSN);

    // first read the columns from the schemas table
    XidLsn&& read_xid = _get_read_xid(db_id);
    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Read schema info {}@{}:{} for @{}:{}", table_id,
                        access_xid.xid, access_xid.lsn, target_xid.xid, target_xid.lsn);

    // note: we always try to read data from disk up to the access_xid in case some of the data
    //       past the read_xid has already made it to disk
    _read_schema_columns(info, db_id, table_id, access_xid);

    // if the requested access XID is ahead of the read XID, apply changes from the cache
    _apply_schema_cache_history(info, db_id, table_id, access_xid);

    // read the index data and attach it to the info object
    _read_schema_indexes(info, db_id, table_id, access_xid);

    // note: at this point we have the set of columns at the access_xid
    if (access_xid == target_xid) {
        info->set_target_xid_start(info->access_xid_start());
        info->set_target_lsn_start(info->access_lsn_start());
        info->set_target_xid_end(info->access_xid_end());
        info->set_target_lsn_end(info->access_lsn_end());
        return info;
    }

    // now collect any history between access_xid and target_xid
    // note: we read any history from the on-disk table since there might always be some history
    //       on disk if the on-disk data is ahead of the read_xid
    _read_schema_history(info, db_id, table_id, access_xid, target_xid);

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Tried to read history from disk: {}", info->history().size());

    // if the target is ahead of the guaranteed on-disk data then don't need to check the in-memory
    // data
    XidLsn xid = std::max(access_xid, read_xid);
    if (target_xid > xid) {
        // read any history from the cache
        _get_schema_cache_history(info, db_id, table_id, xid, target_xid);

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Tried to read history from memory: {}",
                            info->history().size());
    }

    return info;
}

void
Server::_read_schema_indexes(SchemaInfoPtr schema_info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid)
{
    auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
    auto names_schema = names_t->extent_schema();
    auto names_fields = names_schema->get_fields();

    auto search_key = sys_tbl::IndexNames::Primary::key_tuple(table_id, 0, 0, 0);

    for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
        auto& row = *names_i;
        uint64_t tid = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(&row);

        if (tid != table_id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG3, "No more indexes for table {} -- {}", table_id, tid);
            break;
        }

        XidLsn index_xid(names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row),
                         names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row));

        if (access_xid < index_xid) {
            const XidLsn end_xid(schema_info->access_xid_end(), schema_info->access_lsn_end());
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "No more data for table indexes {}@{}:{}", tid,
                                index_xid.xid, index_xid.lsn);
            if (index_xid < end_xid) {
                schema_info->set_access_xid_end(index_xid.xid);
                schema_info->set_access_lsn_end(index_xid.lsn);
            }
            continue;
        }

        // note: this means the index is valid from the found xid/lsn
        const XidLsn start_xid(schema_info->access_xid_start(), schema_info->access_lsn_start());
        if (start_xid < index_xid) {
            schema_info->set_access_xid_start(index_xid.xid);
            schema_info->set_access_lsn_start(index_xid.lsn);
        }

        proto::IndexInfo info;
        info.set_id(names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(&row));
        info.set_state(names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(&row));

        if (static_cast<sys_tbl::IndexNames::State>(info.state()) ==
            sys_tbl::IndexNames::State::DELETED) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found deleted index {}@{}:{} - {}", tid, index_xid.xid,
                                index_xid.lsn, info.id());
            // make sure to delete it from the result vector
            // note: DELETED will always come after or at the same XID of other states
            auto it = std::ranges::find_if(schema_info->indexes(),
                                           [&](auto const& v) { return info.id() == v.id(); });
            if (it != schema_info->indexes().end()) {
                schema_info->mutable_indexes()->erase(it);
            }
            continue;
        }

        if (static_cast<sys_tbl::IndexNames::State>(info.state()) ==
            sys_tbl::IndexNames::State::NOT_READY) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found not-ready index {}@{}:{} - {}", tid,
                                index_xid.xid, index_xid.lsn, info.id());
        }

        uint64_t namespace_id =
            names_fields->at(sys_tbl::IndexNames::Data::NAMESPACE_ID)->get_uint64(&row);
        auto ns_info = _get_namespace_info(db_id, namespace_id, access_xid, false);
        CHECK(ns_info);
        info.set_namespace_name(ns_info->name);
        info.set_namespace_id(ns_info->id);

        info.set_name(names_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(&row));
        info.set_table_id(tid);
        info.set_is_unique(names_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(&row));

        // populate Index columns for the given XidLsn
        _populate_index_columns(db_id, info, index_xid);

        // erase any of the previous info, we'll keep the last one only
        auto it = std::ranges::find_if(schema_info->indexes(),
                                       [&](auto const& v) { return info.id() == v.id(); });
        if (it != schema_info->indexes().end()) {
            schema_info->mutable_indexes()->erase(it);
        }
        *schema_info->add_indexes() = std::move(info);
    }

    // apply cached changes
    _apply_index_cache_history(schema_info, db_id, table_id, access_xid);
}

void
Server::_populate_index_columns(uint64_t db_id, proto::IndexInfo& info, XidLsn index_xid) {
    auto indexes_t = _get_system_table(db_id, sys_tbl::Indexes::ID);
    auto indexes_schema = indexes_t->extent_schema();
    auto indexes_fields = indexes_schema->get_fields();

    auto index_key = sys_tbl::Indexes::Primary::key_tuple(info.table_id(), info.id(), index_xid.xid,
            index_xid.lsn, 0);
    for (auto index_i = indexes_t->lower_bound(index_key); index_i != indexes_t->end();
            ++index_i) {
        auto& row = *index_i;
        uint64_t tid = indexes_fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(&row);
        uint64_t index_id =
            indexes_fields->at(sys_tbl::Indexes::Data::INDEX_ID)->get_uint64(&row);

        if (tid != info.table_id() || index_id != info.id()) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG3, "No more indexes for table {} -- {}, {} -- {}",
                    info.table_id(), tid, index_id, info.id());
            break;
        }
        // index_xid and xid's of index columns must match
        uint64_t xid = indexes_fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&row);
        uint64_t lsn = indexes_fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&row);
        if (index_xid != XidLsn(xid, lsn)) {
            break;
        }

        proto::IndexColumn* col = info.add_columns();
        col->set_position(
                indexes_fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(&row));
        col->set_idx_position(
                indexes_fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(&row));
    }
}

void
Server::_read_schema_columns(SchemaInfoPtr info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid)
{
    // clear any existing column data
    info->mutable_columns()->Clear();

    // get an accessor for the schema table
    auto schemas_t = _get_system_table(db_id, sys_tbl::Schemas::ID);

    // construct the column accessors for the schemas table
    auto schema = schemas_t->extent_schema();
    auto fields = schema->get_fields();

    // read everything with the given table_id
    auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);

    // find the valid column metadata for the provided access_xid
    auto table_i = schemas_t->lower_bound(search_key);
    for (; table_i != schemas_t->end(); ++table_i) {
        auto& row = *table_i;

        // get the table_id from the entry
        uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&row);
        if (tid != table_id) {
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "No more data for table {} -- {}", table_id, tid);
            // if we have read all of the entries for this table ID, stop processing
            // note: this means that the last schema column we've constructed so far is current
            break;
        }

        // don't apply changes that are beyond the requested XID/LSN
        uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(&row);
        uint64_t lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(&row);
        const XidLsn row_xid(xid, lsn);
        if (access_xid < row_xid) {
            const XidLsn end_xid(info->access_xid_end(), info->access_lsn_end());
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "No more data for table column {}@{}:{}", tid, xid,
                                lsn);
            // note: this means the schema column is valid up to the found xid/lsn
            if (row_xid < end_xid) {
                info->set_access_xid_end(xid);
                info->set_access_lsn_end(lsn);
            }
            continue;
        }

        // note: this means the schema column is valid from the found xid/lsn
        const XidLsn start_xid(info->access_xid_start(), info->access_lsn_start());
        if (start_xid < row_xid) {
            info->set_access_xid_start(xid);
            info->set_access_lsn_start(lsn);
        }

        // remove the column if it doesn't exist
        auto position = fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(&row);
        bool exists = fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&row);
        if (!exists) {
            // Remove any existing column at this position
            for (int i = 0; i < info->columns_size(); i++) {
                if (info->columns(i).position() == position) {
                    info->mutable_columns()->DeleteSubrange(i, 1);
                    break;
                }
            }
        } else {
            // Find position where column should be (either existing or new)
            int idx = 0;
            for (; idx < info->columns_size(); idx++) {
                if (info->columns(idx).position() >= position) {
                    break;
                }
            }

            proto::TableColumn* column;
            if (idx < info->columns_size() && info->columns(idx).position() == position) {
                // Update existing column at this position
                column = info->mutable_columns(idx);
            } else {
                // Insert new column at this position
                info->mutable_columns()->Add();
                for (int i = info->columns_size() - 1; i > idx; i--) {
                    info->mutable_columns(i)->Swap(info->mutable_columns(i - 1));
                }
                column = info->mutable_columns(idx);
            }

            column->set_name(fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&row));
            column->set_type(fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(&row));
            column->set_pg_type(fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(&row));
            column->set_position(position);
            column->set_is_nullable(fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(&row));
            column->set_is_generated(false);  // XXX
            if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(&row)) {
                column->set_default_value(
                    fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(&row));
            }
            // note: pk_position set via scan of Indexes system table later
        }
    }

    // if no schema (e.g., due to DROP TABLE) then return empty schema info
    if (info->columns().empty()) {
        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found no columns for table {}@{}:{}", table_id,
                            access_xid.xid, access_xid.lsn);
        return;
    }

    // retrieve the primary index data for the table at this XID/LSN
    auto indexes_t = _get_system_table(db_id, sys_tbl::Indexes::ID);

    schema = indexes_t->extent_schema();
    fields = schema->get_fields();

    // find the first entry that matches for this XID/LSN
    search_key = sys_tbl::Indexes::Primary::key_tuple(table_id, constant::INDEX_PRIMARY,
                                                      access_xid.xid, access_xid.lsn, 0);

    auto index_i = indexes_t->inverse_lower_bound(search_key);
    if (index_i == indexes_t->end()) {
        LOG_WARN("Didn't find a primary index for the table: {}@{}:{}", table_id, access_xid.xid,
                    access_xid.lsn);
        return;
    }

    // determine the XID we found and only read those entries
    auto row = *index_i;
    XidLsn index_xid(fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&row),
                     fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&row));

    bool done = false;
    while (!done) {
        row = *index_i;

        // ensure we are reading data for the requested table
        uint64_t tid = fields->at(sys_tbl::Indexes::Data::TABLE_ID)->get_uint64(&row);
        if (tid != table_id) {
            // if we have read all of the entries for this table ID, stop processing
            break;
        }

        uint64_t xid = fields->at(sys_tbl::Indexes::Data::XID)->get_uint64(&row);
        uint64_t lsn = fields->at(sys_tbl::Indexes::Data::LSN)->get_uint64(&row);

        // ensure we are still reading the correct XID/LSN
        if (index_xid != XidLsn(xid, lsn)) {
            break;
        }

        // update the primary key details in the schema columns
        uint32_t column_id = fields->at(sys_tbl::Indexes::Data::COLUMN_ID)->get_uint32(&row);
        uint32_t index_pos = fields->at(sys_tbl::Indexes::Data::POSITION)->get_uint32(&row);
        bool found = false;
        for (auto& column : *info->mutable_columns()) {
            if (column.position() == column_id) {
                column.set_pk_position(index_pos);
                found = true;
                break;
            }
        }
        CHECK(found) << "Failed to find matching column for primary key";

        LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Found index row {} for table {}@{}:{}", column_id,
                            table_id, access_xid.xid, access_xid.lsn);

        done = (index_pos == 0);
        if (!done) {
            --index_i;
        }
    }
}

void
Server::_apply_schema_cache_history(SchemaInfoPtr info,
                                     uint64_t db_id,
                                     uint64_t table_id,
                                     const XidLsn& xid)
{
    boost::unique_lock ulock(_mutex);

    // check the cache to see if it has entries for this table, if not, nothing to apply
    auto schema_i = _schema_cache[db_id].find(table_id);
    if (schema_i == _schema_cache[db_id].end()) {
        return;
    }

    // can downgrade to a shared lock once we've guaranteed to create the _schema_cache entry
    boost::shared_lock slock(std::move(ulock));

    // go through the history and apply any changes up through the provided XID/LSN
    for (const auto& column : schema_i->second) {
        for (const auto& history : column.second) {
            const XidLsn history_xid(history.xid(), history.lsn());
            if (xid < history_xid) {
                const XidLsn end_xid(info->access_xid_end(), info->access_lsn_end());

                // note: the schema's validity must end at least at this point
                LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Checking end {}:{} < {}:{}", history.xid(),
                                    history.lsn(), end_xid.xid, end_xid.lsn);
                if (history_xid < end_xid) {
                    info->set_access_xid_end(history.xid());
                    info->set_access_lsn_end(history.lsn());
                }
                break;  // stop applying changes
            }

            // note: the schema's validity must start at least at this point
            const XidLsn start_xid(info->access_xid_start(), info->access_lsn_start());
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Checking start {}:{} < {}:{}", start_xid.xid,
                                start_xid.lsn, history.xid(), history.lsn());

            if (start_xid < history_xid) {
                info->set_access_xid_start(history.xid());
                info->set_access_lsn_start(history.lsn());
            }

            // Apply the recorded change, ensuring the columns remain sorted by column.position()
            auto* columns = info->mutable_columns();
            int target_position = history.column().position();
            LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Applying schema change: {}",
                                history.ShortDebugString());

            // Find index of column with position >= target_position
            int index = 0;
            for (; index < columns->size(); ++index) {
                if ((*columns)[index].position() >= target_position) {
                    break;
                }
            }

            if (history.exists()) {
                if (index < columns->size() && (*columns)[index].position() == target_position) {
                    // Update existing column
                    (*columns)[index] = history.column();
                } else {
                    // Insert new column at correct position
                    *columns->Add() = history.column();
                    for (int i = columns->size() - 1; i > index; --i) {
                        columns->SwapElements(i, i - 1);
                    }
                }
            } else if (index < columns->size() && (*columns)[index].position() == target_position) {
                // Remove column if it exists
                columns->DeleteSubrange(index, 1);
            }
        }
    }
}

void
Server::_apply_index_cache_history(SchemaInfoPtr schema_info,
                                    uint64_t db_id,
                                    uint64_t table_id,
                                    const XidLsn& xid)
{
    boost::shared_lock ulock(_mutex);

    auto db_it = _index_cache.find(db_id);
    if (db_it == _index_cache.end()) {
        return;
    }

    auto tab_it = db_it->second.find(table_id);
    if (tab_it == db_it->second.end()) {
        return;
    }

    for (auto const& [_, cache] : tab_it->second) {
        // cache is vector<IndexCacheItem>
        if (cache.empty()) {
            continue;
        }

        // get the first item that is greater than xid
        auto iit =
            std::ranges::upper_bound(cache, IndexCacheItem{xid, {}},
                                     [](auto const& a, auto const& b) { return a.xid < b.xid; });
        // check if we need to update the end of the XID range
        if (iit != cache.end()) {
            const XidLsn end_xid(schema_info->access_xid_end(),
                                 schema_info->access_lsn_end());
            if (iit->xid < end_xid) {
                schema_info->set_access_xid_end(iit->xid.xid);
                schema_info->set_access_lsn_end(iit->xid.lsn);
            }
        }

        if (iit == cache.begin()) {
            continue;
        }

        --iit;

        // check if we need to update the start of the XID range
        const XidLsn start_xid(schema_info->access_xid_start(),
                               schema_info->access_lsn_start());
        if (start_xid < iit->xid) {
            schema_info->set_access_xid_start(iit->xid.xid);
            schema_info->set_access_lsn_start(iit->xid.lsn);
        }

        // replace the existing info with the cached one
        auto it = std::ranges::find_if(*schema_info->mutable_indexes(),
                                       [&](auto const& v) { return iit->info.id() == v.id(); });
        if (it != schema_info->mutable_indexes()->end()) {
            schema_info->mutable_indexes()->erase(it);
        }

        if (static_cast<sys_tbl::IndexNames::State>(iit->info.state()) ==
            sys_tbl::IndexNames::State::DELETED) {
            continue;
        }

        *schema_info->add_indexes() = iit->info;
    }
}

std::optional<std::pair<proto::IndexInfo, XidLsn>>
Server::_find_cached_index(uint64_t db_id,
                            uint64_t index_id,
                            const XidLsn& xid,
                            std::optional<uint64_t> tid)
{
    boost::unique_lock ulock(_mutex);

    auto db_it = _index_cache.find(db_id);
    if (db_it == _index_cache.end()) {
        return {};
    }

    const std::vector<IndexCacheItem>* cache = nullptr;

    if (tid.has_value()) {
        auto it = db_it->second.find(*tid);
        if (it == db_it->second.end()) {
            return {};
        }
        auto iit = it->second.find(index_id);
        if (iit == it->second.end()) {
            return {};
        }
        cache = &(iit->second);
    } else {
        // Except for primary indexes, secondary index ID's should be
        // globally unique so tid is optional in this case but
        // it is required for primary indexes.
        CHECK(index_id != constant::INDEX_PRIMARY);

        auto const& db = db_it->second;

        // find the first matching index_id
        for (auto const& [_, tab_cache] : db) {
            auto iit = tab_cache.find(index_id);
            if (iit != tab_cache.end()) {
                cache = &(iit->second);
                break;
            }
        }
        if (!cache) {
            return {};
        }
    }

    // get the first item that is greater than xid
    auto it = std::ranges::upper_bound(*cache, IndexCacheItem{xid, {}},
                                       [](auto const& a, auto const& b) { return a.xid < b.xid; });

    if (it == cache->begin()) {
        return {};
    }
    --it;

    return {{it->info, it->xid}};
}

void
Server::_read_schema_history(SchemaInfoPtr info,
                              uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn& access_xid,
                              const XidLsn& target_xid)
{
    info->set_target_xid_start(0);
    info->set_target_lsn_start(0);
    info->set_target_xid_end(constant::LATEST_XID);
    info->set_target_lsn_end(constant::MAX_LSN);

    // get an accessor for the schema table
    auto schemas_t = _get_system_table(db_id, sys_tbl::Schemas::ID);

    // construct the column accessors for the schemas table
    auto schema = schemas_t->extent_schema();
    auto fields = schema->get_fields();

    // read everything with the given table_id
    auto search_key = sys_tbl::Schemas::Primary::key_tuple(table_id, 0, 0, 0);

    // find the valid column metadata for the provided access_xid
    auto table_i = schemas_t->lower_bound(search_key);
    for (; table_i != schemas_t->end(); ++table_i) {
        auto& row = *table_i;

        // get the table_id from the entry
        uint64_t tid = fields->at(sys_tbl::Schemas::Data::TABLE_ID)->get_uint64(&row);
        if (tid != table_id) {
            // if we have read all of the entries for this table ID, stop processing
            break;
        }

        uint64_t xid = fields->at(sys_tbl::Schemas::Data::XID)->get_uint64(&row);
        uint64_t lsn = fields->at(sys_tbl::Schemas::Data::LSN)->get_uint64(&row);
        XidLsn row_xid(xid, lsn);

        // don't capture changes that are before the access_xid
        if (row_xid < access_xid) {
            continue;
        }

        // don't capture changes that are beyond the target_xid
        if (target_xid < row_xid) {
            info->set_target_xid_end(row_xid.xid);
            info->set_target_lsn_end(row_xid.lsn);
            continue;
        }

        info->set_target_xid_start(row_xid.xid);
        info->set_target_lsn_start(row_xid.lsn);

        // store the entry into the history
        proto::ColumnHistory* entry = info->add_history();
        entry->set_xid(xid);
        entry->set_lsn(lsn);
        entry->set_exists(fields->at(sys_tbl::Schemas::Data::EXISTS)->get_bool(&row));
        entry->set_update_type(fields->at(sys_tbl::Schemas::Data::UPDATE_TYPE)->get_uint8(&row));
        proto::TableColumn* column = entry->mutable_column();
        column->set_name(fields->at(sys_tbl::Schemas::Data::NAME)->get_text(&row));
        column->set_type(fields->at(sys_tbl::Schemas::Data::TYPE)->get_uint8(&row));
        column->set_pg_type(fields->at(sys_tbl::Schemas::Data::PG_TYPE)->get_int32(&row));
        column->set_position(fields->at(sys_tbl::Schemas::Data::POSITION)->get_uint32(&row));
        column->set_is_nullable(fields->at(sys_tbl::Schemas::Data::NULLABLE)->get_bool(&row));
        column->set_is_generated(false);  // XXX
        if (!fields->at(sys_tbl::Schemas::Data::DEFAULT)->is_null(&row)) {
            column->set_default_value(fields->at(sys_tbl::Schemas::Data::DEFAULT)->get_text(&row));
        }
    }
}

void
Server::_get_schema_cache_history(SchemaInfoPtr info,
                                   uint64_t db_id,
                                   uint64_t table_id,
                                   const XidLsn& access_xid,
                                   const XidLsn& target_xid)

{
    boost::unique_lock ulock(_mutex);

    // check the cache to see if it has entries for this table, if not, nothing to apply
    auto schema_i = _schema_cache[db_id].find(table_id);
    if (schema_i == _schema_cache[db_id].end()) {
        return;
    }

    // can downgrade to a shared lock once we've guaranteed to create the _schema_cache entry
    boost::shared_lock slock(std::move(ulock));

    // go through the history and capture any changes up through the provided XID/LSN
    for (auto& column : schema_i->second) {
        for (auto& entry : column.second) {
            XidLsn xid(entry.xid(), entry.lsn());

            if (xid < access_xid) {
                continue;  // already applied these changes
            }

            if (target_xid < xid) {
                info->set_target_xid_end(xid.xid);
                info->set_target_lsn_end(xid.lsn);
                break;  // stop capturing changes
            }

            info->set_target_xid_start(xid.xid);
            info->set_target_lsn_start(xid.lsn);
            *info->add_history() = entry;
        }
    }
}

void
Server::_set_schema_info(uint64_t db_id,
                          uint64_t table_id,
                          uint64_t namespace_id,
                          const std::string& table_name,
                          const std::vector<proto::ColumnHistory>& columns)
{
    auto schemas_t = _get_mutable_system_table(db_id, sys_tbl::Schemas::ID);

    // add the column change history to the cache
    for (auto& history : columns) {
        assert(history.has_column());
        assert(!history.has_index_column());

        // XXX do we need to enforce XID ordering somehow here?  are we guaranteed to apply these in
        // xid order?
        boost::unique_lock lock(_mutex);
        _schema_cache[db_id][table_id][history.column().position()].push_back(history);
        lock.unlock();

        // write the column data to the schemas table
        std::optional<std::string> value;
        if (history.column().has_default_value()) {
            value = history.column().default_value();
        }
        auto tuple = sys_tbl::Schemas::Data::tuple(
            table_id, history.column().position(), history.xid(), history.lsn(), history.exists(),
            history.column().name(), history.column().type(),
            history.column().pg_type(),  // pg type oid
            history.column().is_nullable(), value, history.update_type(), schemas_t->get_next_internal_row_id());
        schemas_t->upsert(tuple, constant::UNKNOWN_EXTENT);
    }
}

void
Server::_set_primary_index(uint64_t db_id,
                            uint64_t namespace_id,
                            uint64_t table_id,
                            const std::string& namespace_name,
                            const std::string& table_name,
                            const XidLsn& xid)
{
    // pk_position, position
    std::map<uint32_t, uint32_t> primary_keys;

    auto info = _get_schema_info(db_id, table_id, xid, xid);

    proto::IndexInfo index;

    index.set_id(constant::INDEX_PRIMARY);
    index.set_name(table_name + ".primary_key");
    index.set_is_unique(true);
    index.set_namespace_name(namespace_name);
    index.set_namespace_id(namespace_id);
    index.set_table_id(table_id);
    index.set_state(static_cast<uint8_t>(sys_tbl::IndexNames::State::READY));

    std::map<uint32_t, std::string> op_classes;
    for (auto const& c : info->columns()) {
        if (!c.has_pk_position()) {
            continue;
        }
        proto::IndexColumn col;
        col.set_position(c.position());
        col.set_idx_position(c.pk_position());
        *index.add_columns() = col;
        primary_keys[c.pk_position()] = c.position();
        op_classes[c.pk_position()] = "";
    }

    _upsert_index_name(db_id, index, xid, primary_keys, op_classes, IndexType::PRIMARY);

    // Set also lookaside index for the table
    _set_lookaside_index(db_id, namespace_id, table_id, namespace_name, table_name, xid);
}

void
Server::_set_lookaside_index(uint64_t db_id,
                             uint64_t namespace_id,
                             uint64_t table_id,
                             const std::string& namespace_name,
                             const std::string& table_name,
                             const XidLsn& xid)
{
    // pk_position, position
    std::map<uint32_t, uint32_t> look_aside_keys;
    std::map<uint32_t, std::string> op_classes;

    proto::IndexInfo index;

    index.set_id(constant::INDEX_LOOK_ASIDE);
    index.set_name(table_name + ".lookaside_key");
    index.set_is_unique(true);
    index.set_namespace_name(namespace_name);
    index.set_namespace_id(namespace_id);
    index.set_table_id(table_id);
    index.set_state(static_cast<uint8_t>(sys_tbl::IndexNames::State::READY));

    proto::IndexColumn col;
    col.set_position(0);
    col.set_idx_position(0);
    *index.add_columns() = col;
    look_aside_keys[0] = 0;
    op_classes[0] = "";

    _upsert_index_name(db_id, index, xid, look_aside_keys, op_classes, IndexType::LOOKASIDE);
}

void
Server::_clear_schema_info(uint64_t db_id)
{
    boost::unique_lock lock(_mutex);
    _schema_cache.erase(db_id);
    _index_cache.erase(db_id);
}

TablePtr
Server::_get_system_table(uint64_t db_id, uint64_t table_id)
{
    boost::unique_lock lock(_mutex);

    // check if we already have a copy of the table
    auto& cache = _read[db_id];
    auto table_i = cache.find(table_id);
    if (table_i != cache.end()) {
        return table_i->second;
    }

    // otherwise create an interface to the table and cache it
    auto&& read_xid = _get_read_xid(db_id);
    TablePtr table = SystemTableMgr::get_instance()->get_table(db_id, table_id, read_xid.xid);

    // cache the table interface
    cache[table_id] = table;
    return table;
}

MutableTablePtr
Server::_get_mutable_system_table(uint64_t db_id, uint64_t table_id)
{
    boost::unique_lock lock(_mutex);

    // check if we already have the table open
    auto& cache = _write[db_id];
    auto table_i = cache.find(table_id);
    if (table_i != cache.end()) {
        return table_i->second;
    }

    // otherwise create an interface to the table and cache it
    auto&& read_xid = _get_read_xid(db_id);
    auto&& write_xid = _get_write_xid(db_id);
    MutableTablePtr table =
        SystemTableMgr::get_instance()->get_mutable_system_table(db_id, table_id, read_xid.xid, write_xid);

    // save the mutable table into the cache
    cache[table_id] = table;
    return table;
}

void
Server::_write_index(const XidLsn& xid,
                      uint64_t db_id,
                      uint64_t tab_id,
                      uint64_t index_id,
                      const std::map<uint32_t, uint32_t>& keys,
                      const std::map<uint32_t, std::string>& op_classes)
{
    if (keys.empty()) {
        LOG_INFO("The index has no keys: {}:{} - {}", db_id, tab_id, index_id);
        return;
    }

    auto indexes_t = _get_mutable_system_table(db_id, sys_tbl::Indexes::ID);
    auto fields = sys_tbl::Indexes::Data::fields(tab_id, index_id, xid.xid, xid.lsn,
                                                 0,   // empty position; filled below
                                                 0,   // empty column ID; filled below
                                                 "",  // empty opclass; filled below
                                                 0);  // empty internal row ID; filled below

    for (auto& [pos, col_id] : keys) {
        fields->at(sys_tbl::Indexes::Data::POSITION) =
            std::make_shared<ConstTypeField<uint32_t>>(pos);
        fields->at(sys_tbl::Indexes::Data::COLUMN_ID) =
            std::make_shared<ConstTypeField<uint32_t>>(col_id);
        fields->at(sys_tbl::Indexes::Data::OPCLASS) =
            std::make_shared<ConstTypeField<std::string>>(op_classes.at(pos));
        fields->at(sys_tbl::Indexes::Data::INTERNAL_ROW_ID) =
            std::make_shared<ConstTypeField<uint64_t>>(indexes_t->get_next_internal_row_id());

        indexes_t->upsert(std::make_shared<FieldTuple>(fields, nullptr),
                          constant::UNKNOWN_EXTENT);
    }
}

proto::ColumnHistory
Server::_generate_update(const google::protobuf::RepeatedPtrField<proto::TableColumn>& old_schema,
                          const google::protobuf::RepeatedPtrField<proto::TableColumn>& new_schema,
                          const XidLsn& xid,
                          nlohmann::json& ddl)
{
    proto::ColumnHistory update;
    update.set_xid(xid.xid);
    update.set_lsn(xid.lsn);

    // Build maps keyed by column.position() for both old and new schemas
    std::map<uint32_t, const proto::TableColumn*> oldMap;
    for (const auto& column : old_schema) {
        oldMap[column.position()] = &column;
    }

    std::map<uint32_t, const proto::TableColumn*> newMap;
    for (const auto& column : new_schema) {
        newMap[column.position()] = &column;
    }

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "Comparing schemas for update: old size = {}, new size = {}",
                            oldMap.size(), newMap.size());

    // Check for removals: any column in oldMap that's missing in newMap
    for (const auto& [pos, old_col] : oldMap) {
        if (newMap.find(pos) == newMap.end()) {


#if ENABLE_SCHEMA_MUTATES
            // Column has been removed
            *update.mutable_column() = *old_col;
            update.set_update_type(static_cast<int8_t>(SchemaUpdateType::REMOVE_COLUMN));
            update.set_exists(false);
            ddl["action"] = "col_drop";
            ddl["column"] = old_col->name();
#else
            ddl["action"] = "resync";
            update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
#endif
            return update;
        }
    }

    // Check for additions: any column in newMap that's missing in oldMap
    for (const auto& [pos, new_col] : newMap) {
        if (oldMap.find(pos) == oldMap.end()) {
            // A new column has been added

#if ENABLE_SCHEMA_MUTATES
            if (new_col->has_default_value()) {
                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
            } else {
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::NEW_COLUMN));
                update.set_exists(true);
                *update.mutable_column() = *new_col;
                ddl["action"] = "col_add";
                ddl["column"]["name"] = new_col->name();
                ddl["column"]["type"] = new_col->pg_type();
                ddl["column"]["nullable"] = new_col->is_nullable();
                if (new_col->has_default_value()) {
                    ddl["column"]["default"] = new_col->default_value();
                }
                CHECK(false); // XXX new_col->type_name must be added
            }
#else
            ddl["action"] = "resync";
            update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
#endif

            return update;
        }
    }

    // Check for modifications: for columns that exist in both maps, compare their attributes
    for (const auto& [pos, old_col] : oldMap) {
        auto it = newMap.find(pos);
        if (it != newMap.end()) {
            const auto* new_col = it->second;
            // Check for a name change
            if (old_col->name() != new_col->name()) {
                *update.mutable_column() = *new_col;
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::NAME_CHANGE));
                update.set_exists(true);
                ddl["action"] = "col_rename";
                ddl["old_name"] = old_col->name();
                ddl["new_name"] = new_col->name();
                return update;
            }

            // Check for a change in nullability (from not-null to nullable).
            // A column going from nullable to not-nullable results in NULL values being
            // populated with a default, which aren't sent via the log.
            if (!old_col->is_nullable() && new_col->is_nullable()) {
#if ENABLE_SCHEMA_MUTATES
                *update.mutable_column() = *new_col;
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::NULLABLE_CHANGE));
                update.set_exists(true);
                ddl["action"] = "col_nullable";
                ddl["column"]["name"] = new_col->name();
                ddl["column"]["nullable"] = new_col->is_nullable();
#else
                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
#endif
                return update;
            }

            // Changing from nullable to not-nullable requires a resync
            if (old_col->is_nullable() && !new_col->is_nullable()) {

                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
                return update;
            }

            // Check for a change in the data type
            if (old_col->pg_type() != new_col->pg_type()) {
                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
                return update;
            }

            // Check for a primary key position change
            if ((old_col->has_pk_position() && !new_col->has_pk_position()) ||
                (!old_col->has_pk_position() && new_col->has_pk_position()) ||
                (old_col->pk_position() != new_col->pk_position())) {
                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
                return update;
            }
        }
    }

    // No change detected
    ddl["action"] = "no_change";
    update.set_update_type(static_cast<int8_t>(SchemaUpdateType::NO_CHANGE));

    LOG_DEBUG(LOG_SCHEMA, LOG_LEVEL_DEBUG1, "No schema change detected for table @{}:{}",
              xid.xid, xid.lsn);

    return update;
}

}  // namespace springtail::sys_tbl_mgr
