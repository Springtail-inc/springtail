#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include <common/constants.hh>
#include <common/properties.hh>
#include <grpc/grpc_server.hh>
#include <sys_tbl_mgr/exception.hh>
#include <sys_tbl_mgr/server.hh>
#include <sys_tbl_mgr/service.hh>
#include <sys_tbl_mgr/table_mgr.hh>
#include <xid_mgr/xid_mgr_client.hh>

namespace springtail::sys_tbl_mgr {

grpc::Status
Service::Ping(grpc::ServerContext* context,
              const google::protobuf::Empty* request,
              google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "Ping");
    return grpc::Status::OK;
}

grpc::Status
Service::CreateIndex(grpc::ServerContext* context,
                     const proto::IndexRequest* request,
                     proto::IndexProcessRequest* response)
{
    ServerSpan span(context, "SysTblMgrService", "CreateIndex");

    LOG_INFO("got CreateIndex(): db {}, table {}, index {}, xid {}:{}", request->db_id(),
                request->index().table_id(), request->index().id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    try {
        // perform the CREATE INDEX
        const auto &index_info = _create_index(*request);
        response->set_action("create_index");
        *response->mutable_index() = index_info;

        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } catch (const std::exception& e) {
        LOG_ERROR("CreateIndex() failed: {}", e.what());
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, e.what());
        return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
    }
}

proto::IndexesInfo
Service::_get_unfinished_indexes_info(uint64_t db_id)
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
            if (unfinished_indexes_map.find(index_id) != unfinished_indexes_map.end()) {
                unfinished_indexes_map.erase(index_id);
            }
            unfinished_indexes_map.try_emplace(index_id)
                .first->second
                .try_emplace(index_xid, std::move(info));
        } else {
            if (unfinished_indexes_map.find(index_id) != unfinished_indexes_map.end()) {
                unfinished_indexes_map.erase(index_id);
            }
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
Service::_get_index_info(const proto::GetIndexInfoRequest& request)
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
        LOG_DEBUG(LOG_SCHEMA, "Index not found: {}@{} - {}", request.db_id(),
                            request.xid(), request.index_id());
        proto::IndexInfo dummy;
        dummy.set_id(0);
        return dummy;
    }

    return std::get<0>(*info);
}

bool
Service::_set_index_state(const proto::SetIndexStateRequest& request)
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
        LOG_DEBUG(LOG_SCHEMA, "Index not found for table {} -- {}", request.table_id(),
                            request.index_id());
        return false;
    }

    // this copies the existing index info
    auto index_info = *index_i;
    CHECK(index_info.table_id() == request.table_id() && index_info.id() == request.index_id());
    index_info.set_state(request.state());

    std::map<uint32_t, uint32_t> keys;
    for (const auto& column : index_info.columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
    }

    return _upsert_index_name(request.db_id(), index_info, xid, keys);
}

bool
Service::_upsert_index_name(uint64_t db_id,
                            const proto::IndexInfo& index_info,
                            const XidLsn& xid,
                            const std::map<uint32_t, uint32_t>& keys,
                            bool is_primary_index)
{
    auto index_names_t = _get_mutable_system_table(db_id, sys_tbl::IndexNames::ID);

    auto is_unique = index_info.is_unique();
    if (is_primary_index) {
        is_unique = true;
    }

    auto tuple = sys_tbl::IndexNames::Data::tuple(
        index_info.namespace_id(), index_info.name(), index_info.table_id(), index_info.id(), xid.xid, xid.lsn,
        static_cast<sys_tbl::IndexNames::State>(index_info.state()), is_unique);

    // update the index state
    index_names_t->upsert(tuple, constant::UNKNOWN_EXTENT);

    // update columns with the state XID
    if (is_primary_index) {
        if(!keys.empty()) {
            _write_index(xid, db_id, index_info.table_id(), constant::INDEX_PRIMARY, keys);
        }
    } else {
        _write_index(xid, db_id, index_info.table_id(), index_info.id(), keys);
    }

    // add to index cache
    {
        boost::unique_lock lock(_mutex);
        _index_cache[db_id][index_info.table_id()][index_info.id()].emplace_back(
            xid, index_info);
    }

    return true;
}

grpc::Status
Service::SetIndexState(grpc::ServerContext* context,
                       const proto::SetIndexStateRequest* request,
                       google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "SetIndexState");

    LOG_INFO("got SetIndexState()");

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    if (_set_index_state(*request)) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
        return grpc::Status::OK;
    } else {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError,
                               "Failed to set index state");
        return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to set index state");
    }
}

grpc::Status
Service::GetIndexInfo(grpc::ServerContext* context,
                      const proto::GetIndexInfoRequest* request,
                      proto::IndexInfo* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetIndexInfo");

    LOG_INFO("got GetIndexInfo()");

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_read_mutex);

    *response = _get_index_info(*request);
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetUnfinishedIndexesInfo(grpc::ServerContext* context,
        const proto::GetUnfinishedIndexesInfoRequest* request,
        proto::IndexesInfo* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetUnfinishedIndexesInfo");

    LOG_INFO("got GetUnfinishedIndexesInfo");

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_read_mutex);

    *response = _get_unfinished_indexes_info(request->db_id());
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

proto::IndexInfo
Service::_create_index(const proto::IndexRequest& request)
{
    XidLsn xid(request.xid(), request.lsn());

    std::map<uint32_t, uint32_t> keys;
    for (const auto& column : request.index().columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
    }

    // update index names
    // Create a copy to add namespace ID, before caching the index_info
    auto mutable_index_request = request;

    // lookup the namespace info
    auto ns_info = _get_namespace_info(request.db_id(), request.index().namespace_name(), xid);
    CHECK(ns_info);

    // Set namespace ID for the requested index
    mutable_index_request.mutable_index()->set_namespace_id(ns_info->id);

    _upsert_index_name(request.db_id(), mutable_index_request.index(), xid, keys);

    return mutable_index_request.index();
}

grpc::Status
Service::DropIndex(grpc::ServerContext* context,
                   const proto::DropIndexRequest* request,
                   proto::IndexProcessRequest* response)
{
    LOG_INFO("got DropIndex(): db {}, index {}, xid {}:{}", request->db_id(),
                request->index_id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    XidLsn xid(request->xid(), request->lsn());

    // perform the DROP INDEX
    _drop_index(xid, request->db_id(), request->index_id(), std::nullopt, sys_tbl::IndexNames::State::BEING_DELETED);

    // Create response for the dropped index
    proto::IndexInfo dropped_index;
    dropped_index.set_id(request->index_id());
    dropped_index.set_name(request->name());
    dropped_index.set_namespace_name(request->namespace_name());
    response->set_action("drop_index");
    *response->mutable_index() = dropped_index;

    return grpc::Status::OK;
}

std::optional<std::tuple<proto::IndexInfo, uint64_t, XidLsn>>
Service::_find_index(uint64_t db_id,
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
    bool is_unique;
    std::string name;
    uint64_t namespace_id;
    uint8_t state;
    bool found = false;

    // find the last XID for this index
    for (auto names_i = names_t->lower_bound(search_key); names_i != names_t->end(); ++names_i) {
        auto& row = *names_i;
        uint64_t id = names_fields->at(sys_tbl::IndexNames::Data::INDEX_ID)->get_uint64(&row);

        if (id < index_id) {
            continue;
        }
        if (index_id != id) {
            LOG_DEBUG(LOG_SCHEMA, "No data found for index {} -- {}", db_id, index_id);
            break;
        }

        {
            uint64_t xid = names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row);
            uint64_t lsn = names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row);
            index_xid = {xid, lsn};
            if (access_xid < index_xid) {
                continue;
            }
        }
        auto found_tid = names_fields->at(sys_tbl::IndexNames::Data::TABLE_ID)->get_uint64(&row);
        if (tid && tid != found_tid) {
            LOG_DEBUG(LOG_SCHEMA, "Found a dupoicate index id {} -- {}, {}/{}", db_id,
                                index_id, tid, found_tid);
            break;
        }

        table_id = found_tid;
        is_unique = names_fields->at(sys_tbl::IndexNames::Data::IS_UNIQUE)->get_bool(&row);
        name = names_fields->at(sys_tbl::IndexNames::Data::NAME)->get_text(&row);
        namespace_id = names_fields->at(sys_tbl::IndexNames::Data::NAMESPACE_ID)->get_uint64(&row);
        state = names_fields->at(sys_tbl::IndexNames::Data::STATE)->get_uint8(&row);

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

    // need to look up the schema name in the namespace_names table
    auto ns_info = _get_namespace_info(db_id, namespace_id, access_xid, false);
    CHECK(ns_info);
    info.set_namespace_name(ns_info->name);
    info.set_namespace_id(ns_info->id);

    return {{info, namespace_id, index_xid}};
}

void
Service::_drop_index(const XidLsn& xid,
                     uint64_t db_id,
                     uint64_t index_id,
                     std::optional<uint64_t> tid,
                     sys_tbl::IndexNames::State index_state)
{
    assert(index_state == sys_tbl::IndexNames::State::DELETED || index_state == sys_tbl::IndexNames::State::BEING_DELETED);
    auto names_t = _get_system_table(db_id, sys_tbl::IndexNames::ID);
    auto names_schema = names_t->extent_schema();
    auto names_fields = names_schema->get_fields();

    // find the last record for the index id
    auto info = _find_index(db_id, index_id, xid, tid);

    if (!info) {
        LOG_DEBUG(LOG_SCHEMA, "Drop index not found: {}@{} - {}", db_id, xid.xid,
                            index_id);
        return;
    }
    auto& index_info = std::get<0>(*info);

    auto state = static_cast<sys_tbl::IndexNames::State>(index_info.state());
    if (state == sys_tbl::IndexNames::State::DELETED ||
        state == sys_tbl::IndexNames::State::BEING_DELETED) {
        LOG_DEBUG(LOG_SCHEMA, "Index already deleted: {}@{} - {}", db_id, xid.xid, index_id);
        return;
    }

    // note: this might not be true during recovery
    // assert(xid > std::get<2>(*info));

    LOG_DEBUG(LOG_SCHEMA, "Drop index found {}:{} -- {}", db_id, index_info.table_id(),
                        index_id);
    std::map<uint32_t, uint32_t> keys;
    for (const auto& column : index_info.columns()) {
        assert(keys.find(column.idx_position()) == keys.end());
        keys[column.idx_position()] = column.position();
    }

    index_info.set_state(static_cast<int32_t>(index_state));
    _upsert_index_name(db_id, index_info, xid, keys);
}

grpc::Status
Service::CreateTable(grpc::ServerContext* context,
                     const proto::TableRequest* request,
                     proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "CreateTable");

    LOG_INFO("got CreateTable() -- db {} table {} xid {} lsn {}", request->db_id(),
                request->table().id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // perform the CREATE TABLE
    auto&& ddl = _create_table(*request);

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

nlohmann::json
Service::_create_table(const proto::TableRequest& request)
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
    ddl["columns"] = nlohmann::json::array();

    // partition info
    std::optional<uint64_t> parent_table_id = constant::INVALID_TABLE;
    if (request.table().has_parent_table_id()) {
        parent_table_id = request.table().parent_table_id();
        ddl["parent_table_id"] = parent_table_id.value();
        auto parent_table_info = _get_table_info(request.db_id(), parent_table_id.value(), xid);
        if (parent_table_id.value() != constant::INVALID_TABLE) {
            if (parent_table_info == nullptr) {
                LOG_ERROR("Parent table not found: {}@{} - {}", request.db_id(), xid.xid, parent_table_id.value());
            } else {
                ddl["parent_table_name"] = parent_table_info->name;
                auto namespace_name = _get_namespace_info(request.db_id(), parent_table_info->namespace_id, xid);
                if (namespace_name == nullptr) {
                    LOG_ERROR("Parent namespace not found: {}@{} - {}", request.db_id(), xid.xid, parent_table_id.value());
                }
                ddl["parent_namespace_name"] = namespace_name->name;
            }
        }
    }

    // partition key -- this is a parent table; either root or intermediate
    std::optional<std::string> partition_key = std::nullopt;
    if (request.table().has_partition_key()) {
        partition_key = request.table().partition_key();
        ddl["partition_key"] = partition_key.value();
    }

    // this is a partitioned table, it is a leaf if partition_key is empty
    std::optional<std::string> partition_bound = std::nullopt;
    if (request.table().has_partition_bound()) {
        partition_bound = request.table().partition_bound();
        ddl["partition_bound"] = partition_bound.value();
    }

    // add table name
    auto table_info =
        std::make_shared<TableCacheRecord>(request.table().id(), request.xid(), request.lsn(),
                                           ns_info->id, request.table().name(), true,
                                           parent_table_id, partition_key, partition_bound);
    _set_table_info(request.db_id(), table_info);

    // add roots and stats entry -- may get overwritten later if data is added to the table
    auto roots_info = std::make_shared<proto::GetRootsResponse>();
    proto::RootInfo* ri = roots_info->add_roots();
    ri->set_index_id(constant::INDEX_PRIMARY);
    ri->set_extent_id(constant::UNKNOWN_EXTENT);
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

    return ddl;
}

nlohmann::json
Service::_generate_partition_updates(const proto::TableRequest& request,
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

grpc::Status
Service::AlterTable(grpc::ServerContext* context,
                    const proto::TableRequest* request,
                    proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "AlterTable");

    LOG_INFO("got AlterTable()");

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // retrieve the id of the namespace
    // this function is called by drop table, so we don't check if
    // the namespace exists
    auto ns_info = _get_namespace_info(request->db_id(), request->table().namespace_name(),
                                       XidLsn(request->xid(), request->lsn()), false);
    CHECK(ns_info);

    nlohmann::json ddl;
    ddl["tid"] = request->table().id();
    ddl["xid"] = request->xid();
    ddl["lsn"] = request->lsn();
    ddl["schema"] = request->table().namespace_name();
    ddl["table"] = request->table().name();

    // retrieve the name of the table at the point of alteration
    XidLsn xid(request->xid(), request->lsn());
    auto table_info = _get_table_info(request->db_id(), request->table().id(), xid);

    // note: table should always exist when calling alter_table()
    assert(table_info != nullptr);

    // check if the table is a partitioned table
    std::optional<uint64_t> parent_table_id = std::nullopt;
    std::optional<std::string> partition_key = std::nullopt;
    std::optional<std::string> partition_bound = std::nullopt;
    if (request->table().has_parent_table_id()) {
        parent_table_id = request->table().parent_table_id();
        if (parent_table_id.value() == constant::INVALID_TABLE) {
            parent_table_id = std::nullopt;
        }
    }
    if (request->table().has_partition_key()) {
        partition_key = request->table().partition_key();
    }
    if (request->table().has_partition_bound()) {
        partition_bound = request->table().partition_bound();
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

    if (table_info->namespace_id != ns_info->id) {
        // if the schema/namespace changed then update the table_names table
        // insert the new name for this oid
        auto new_info = std::make_shared<TableCacheRecord>(request->table().id(), request->xid(),
                                                           request->lsn(), ns_info->id,
                                                           request->table().name(), true,
                                                           parent_table_id, partition_key,
                                                           partition_bound);
        _set_table_info(request->db_id(), new_info);

        // set the DDL statement
        ddl["action"] = "set_namespace";

        auto old_ns_info = _get_namespace_info(request->db_id(), table_info->namespace_id,
                                               XidLsn(request->xid(), request->lsn()));
        CHECK(old_ns_info);
        ddl["old_schema"] = old_ns_info->name;

    } else if (table_info->name != request->table().name()) {
        // if the name is changed, update the name in the table_names table
        // insert the new name for this oid
        auto new_info = std::make_shared<TableCacheRecord>(request->table().id(), request->xid(),
                                                           request->lsn(), ns_info->id,
                                                           request->table().name(), true,
                                                           parent_table_id, partition_key,
                                                           partition_bound);
        _set_table_info(request->db_id(), new_info);

        // set the DDL statement
        ddl["action"] = "rename";
        ddl["old_table"] = table_info->name;

        if (table_info->namespace_id != ns_info->id) {
            auto old_ns_info = _get_namespace_info(request->db_id(), table_info->namespace_id,
                                                   XidLsn(request->xid(), request->lsn()));
            CHECK(old_ns_info);
            ddl["old_schema"] = old_ns_info->name;
        } else {
            ddl["old_schema"] = request->table().namespace_name();
        }

        _set_primary_index(request->db_id(), ns_info->id, request->table().id(), table_info->name,
                           ns_info->name, xid);
    } else {
        XidLsn xid(request->xid(), request->lsn());

        // get the schema prior to this change
        auto info = _get_schema_info(request->db_id(), request->table().id(), xid, xid);

        // generate a tuple for the change
        // note: _generate_update() sets the necessary elements of the ddl
        auto history = _generate_update(info->columns(), request->table().columns(),
                                        xid, ddl);

        // If the partition key is not empty, generate the history events for the the child partition tables
        if (partition_key.value_or("") != "") {
            ddl = _generate_partition_updates(*request, history);
        }

        // we won't apply any changes to the system tables in these cases
        if (history.update_type() != static_cast<int8_t>(SchemaUpdateType::NO_CHANGE) &&
            history.update_type() != static_cast<int8_t>(SchemaUpdateType::RESYNC)) {
            // write the column change to the schemas table and update the cache
            _set_schema_info(request->db_id(), request->table().id(), ns_info->id,
                            request->table().name(), {history});
        }

        _set_primary_index(request->db_id(), ns_info->id, request->table().id(),
                    request->table().name(), request->table().namespace_name(), xid);
    }

    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::DropTable(grpc::ServerContext* context,
                   const proto::DropTableRequest* request,
                   proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "DropTable");

    LOG_INFO("got DropTable() {}@{}:{}", request->table_id(), request->xid(), request->lsn());

    // hold a shared lock to prevent a concurrent finalize()
    boost::shared_lock lock(_write_mutex);

    // perform the DROP TABLE
    auto&& ddl = _drop_table(*request);

    // serialize the ddl JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

nlohmann::json
Service::_drop_table(const proto::DropTableRequest& request)
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
        // For secondary indexes, indexer will take care of marking them DELETED
        if (idx.id() == constant::INDEX_PRIMARY) {
            _drop_index(xid, request.db_id(), idx.id(), request.table_id(), sys_tbl::IndexNames::State::DELETED);
        } else {
            _drop_index(xid, request.db_id(), idx.id(), request.table_id(), sys_tbl::IndexNames::State::BEING_DELETED);
        }
    }

    // mark the table as dropped in the table_names
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

    return ddl;
}

grpc::Status
Service::CreateNamespace(grpc::ServerContext* context,
                         const proto::NamespaceRequest* request,
                         proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "CreateNamespace");

    LOG_INFO("got CreateNamespace() -- db {} namespace_id {} name {} xid {} lsn {}",
                request->db_id(), request->namespace_id(), request->name(), request->xid(),
                request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // update the namespace_names table
    XidLsn xid(request->xid(), request->lsn());
    auto ddl =
        _mutate_namespace(request->db_id(), request->namespace_id(), request->name(), xid, true);
    ddl["action"] = "ns_create";

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::AlterNamespace(grpc::ServerContext* context,
                        const proto::NamespaceRequest* request,
                        proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "AlterNamespace");

    LOG_INFO("got AlterNamespace() -- db {} namespace_id {} name {} xid {} lsn {}",
                request->db_id(), request->namespace_id(), request->name(), request->xid(),
                request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);
    XidLsn xid(request->xid(), request->lsn());

    // retrieve the old namespace name
    auto ns_info = _get_namespace_info(request->db_id(), request->namespace_id(), xid);
    CHECK(ns_info);

    // update the namespace_names table
    auto ddl =
        _mutate_namespace(request->db_id(), request->namespace_id(), request->name(), xid, true);
    ddl["action"] = "ns_alter";
    ddl["old_name"] = ns_info->name;

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::DropNamespace(grpc::ServerContext* context,
                       const proto::NamespaceRequest* request,
                       proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "DropNamespace");

    LOG_INFO("got DropNamespace() -- db {} namespace_id {} xid {} lsn {}", request->db_id(),
                request->namespace_id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // update the namespace_names table
    XidLsn xid(request->xid(), request->lsn());
    auto ddl =
        _mutate_namespace(request->db_id(), request->namespace_id(), request->name(), xid, false);
    ddl["action"] = "ns_drop";
    ddl["name"] = request->name();

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

nlohmann::json
Service::_mutate_namespace(
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
        auto entry = std::make_shared<NamespaceCacheRecord>(ns_id, (name) ? *name : "", exists);
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
        sys_tbl::NamespaceNames::Data::tuple(ns_id, (name) ? *name : "", xid.xid, xid.lsn, exists);
    table->upsert(tuple, constant::UNKNOWN_EXTENT);

    return ddl;
}

grpc::Status
Service::UpdateRoots(grpc::ServerContext* context,
                     const proto::UpdateRootsRequest* request,
                     google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "UpdateRoots");

    LOG_INFO("got UpdateRoots()");

    // hold a shared lock to prevent a concurrent finalize()
    boost::shared_lock lock(_write_mutex);

    // update the metadata and return
    _update_roots(*request);
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

void
Service::_update_roots(const proto::UpdateRootsRequest& request)
{
    XidLsn xid(request.xid());

    auto info = std::make_shared<proto::GetRootsResponse>();
    *info->mutable_roots() = request.roots();
    *info->mutable_stats() = request.stats();
    info->set_snapshot_xid(request.snapshot_xid());

    _set_roots_info(request.db_id(), request.table_id(), xid, info);
}

XidLsn
Service::_get_read_xid(uint64_t db_id)
{
    boost::unique_lock lock(_xid_mutex);
    auto read_i = _read_xid.find(db_id);
    if (read_i != _read_xid.end()) {
        return read_i->second;
    }

    auto xid_mgr = XidMgrClient::get_instance();
    auto xid = xid_mgr->get_committed_xid(db_id, 0);

    _read_xid[db_id] = XidLsn(xid);
    _write_xid[db_id] = xid + 1;

    return XidLsn(xid);
}

uint64_t
Service::_get_write_xid(uint64_t db_id)
{
    boost::unique_lock lock(_xid_mutex);
    auto write_i = _write_xid.find(db_id);
    if (write_i != _write_xid.end()) {
        return write_i->second;
    }

    auto xid_mgr = XidMgrClient::get_instance();
    auto xid = xid_mgr->get_committed_xid(db_id, 0);

    _read_xid[db_id] = XidLsn(xid);
    _write_xid[db_id] = xid + 1;

    return xid + 1;
}

void
Service::_set_xids(uint64_t db_id, const XidLsn& read_xid, uint64_t write_xid)
{
    boost::unique_lock lock(_xid_mutex);
    _read_xid[db_id] = read_xid;
    _write_xid[db_id] = write_xid;
}

grpc::Status
Service::Finalize(grpc::ServerContext* context,
                  const proto::FinalizeRequest* request,
                  google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "Finalize");

    LOG_INFO("got Finalize()");

    // block all mutations
    boost::unique_lock wlock(_write_mutex);

    // note: it is safe to pre-write data from later XIDs into the system tables during a finalize
    //       since if there is a failure they will simply be overwritten during recovery
    auto write_xid = _get_write_xid(request->db_id());
    LOG_DEBUG(LOG_SCHEMA, "Finalize system tables: {}@{} >= {}", request->db_id(),
                        request->xid(), write_xid);
    CHECK_GE(request->xid(), write_xid);

    // finalize the mutated tables at the write_xid
    // XXX we currently don't store the metadata, but re-read it from the roots file each time
    std::map<uint64_t, TableMetadata> md_map;
    for (const auto& entry : _write[request->db_id()]) {
        LOG_INFO("Finalize table {}@{}", entry.first, request->xid());
        md_map[entry.first] = entry.second->finalize();
    }
    if (md_map.empty()) {
        LOG_INFO("Nothing to finalize: {}@{} >= {}", request->db_id(), request->xid(),
                    write_xid);
        // NOTE TO REVIEWER: is OK right here?
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk, "Nothing to finalize");
        return grpc::Status::OK;
    }

    // block all read access while we swap access roots
    boost::unique_lock rlock(_read_mutex);

    // move the read_xid to the request xid, and move the write_xid to just beyond the
    // provided request xid
    _set_xids(request->db_id(), XidLsn(request->xid()), request->xid() + 1);

    // note: we could update the table pointers?
    //       or maybe cache the roots to avoid reading the roots file?
    _read[request->db_id()].clear();
    _write[request->db_id()].clear();
    _clear_table_info(request->db_id());
    _clear_roots_info(request->db_id());
    _clear_schema_info(request->db_id());
    _clear_namespace_info(request->db_id());

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetRoots(grpc::ServerContext* context,
                  const proto::GetRootsRequest* request,
                  proto::GetRootsResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetRoots");

    LOG_INFO("got GetRoots()");

    boost::shared_lock lock(_read_mutex);

    XidLsn xid(request->xid(), constant::MAX_LSN);

    // make sure that the table exists at this XID
    auto table_info = _get_table_info(request->db_id(), request->table_id(), xid);
    if (table_info == nullptr) {
        // We just return an empty response if the table doesn't exist
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk, "Table does not exist");
        return grpc::Status::OK;
    }

    // get the roots
    auto info = _get_roots_info(request->db_id(), request->table_id(), xid);

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetSchema(grpc::ServerContext* context,
                   const proto::GetSchemaRequest* request,
                   proto::GetSchemaResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetSchema");

    LOG_INFO("got GetSchema(): db {} tid {} xid {} lsn {}", request->db_id(),
             request->table_id(), request->xid(), request->lsn());

    boost::shared_lock lock(_read_mutex);

    XidLsn xid(request->xid(), request->lsn());
    auto info = _get_schema_info(request->db_id(), request->table_id(), xid, xid);

    LOG_DEBUG(LOG_SCHEMA, "Returning start_xid {}:{}, end_xid {}:{}",
                        info->access_xid_start(), info->access_lsn_start(), info->access_xid_end(),
                        info->access_lsn_end());

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetTargetSchema(grpc::ServerContext* context,
                         const proto::GetTargetSchemaRequest* request,
                         proto::GetSchemaResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetTargetSchema");

    LOG_INFO("got GetTargetSchema() -- {}, {}", request->access_xid(), request->target_xid());

    boost::shared_lock lock(_read_mutex);

    XidLsn access_xid(request->access_xid(), request->access_lsn());
    XidLsn target_xid(request->target_xid(), request->target_lsn());

    auto info = _get_schema_info(request->db_id(), request->table_id(), access_xid, target_xid);

    *response = *info;
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::Exists(grpc::ServerContext* context,
                const proto::ExistsRequest* request,
                google::protobuf::Empty* response)
{
    ServerSpan span(context, "SysTblMgrService", "Exists");

    LOG_INFO("got Exists()");

    boost::shared_lock lock(_read_mutex);

    XidLsn xid(request->xid(), request->lsn());
    auto info = _get_table_info(request->db_id(), request->table_id(), xid);
    if (info == nullptr) {
        span.span()->SetStatus(opentelemetry::trace::StatusCode::kError, "Table not found");
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Table not found");
    }
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::SwapSyncTable(grpc::ServerContext* context,
                       const proto::SwapSyncTableRequest* request,
                       proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "SwapSyncTable");

    LOG_INFO("got SwapSyncTable()");
    const auto& namespace_req = request->namespace_req();
    const auto& create_req = request->create_req();
    const auto& index_reqs = request->index_reqs();
    const auto& roots_req = request->roots_req();

    nlohmann::json ddls;

    // 1. acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // 2. check if the namespace exists at the end of the target XID, if it doesn't create it
    XidLsn ns_xid(namespace_req.xid(), namespace_req.lsn());
    auto ns_info = _get_namespace_info(namespace_req.db_id(), namespace_req.name(), ns_xid);
    if (!ns_info) {
        LOG_DEBUG(LOG_SCHEMA, "Create namespace; db {}, name {}, id {}, xid {}:{}",
                            namespace_req.db_id(), namespace_req.name(),
                            namespace_req.namespace_id(), ns_xid.xid, ns_xid.lsn);

        auto&& ns_ddl = _mutate_namespace(namespace_req.db_id(), namespace_req.namespace_id(),
                                          namespace_req.name(), ns_xid, true);
        ns_ddl["action"] = "ns_create";
        ddls.push_back(ns_ddl);
        LOG_DEBUG(LOG_SCHEMA, "Create namespace name {}, id {}", namespace_req.name(),
                            namespace_req.namespace_id());
    } else {
        LOG_DEBUG(LOG_SCHEMA, "Skip create namespace name {}, id {}",
                            namespace_req.name(), namespace_req.namespace_id());
    }

    // 3. retrieve the table information at the end of the target XID
    XidLsn xid(create_req.xid(), constant::MAX_LSN);
    auto info = _get_table_info(create_req.db_id(), create_req.table().id(), xid);

    // 4. if the table exists at the end of the XID, perform a drop
    if (info != nullptr) {
        proto::DropTableRequest drop;
        drop.set_db_id(create_req.db_id());
        drop.set_table_id(create_req.table().id());
        drop.set_xid(create_req.xid());
        drop.set_lsn(constant::RESYNC_DROP_LSN);
        drop.set_namespace_name(create_req.table().namespace_name());
        drop.set_name(create_req.table().name());

        LOG_DEBUG(LOG_SCHEMA, "Drop table: {}:{} @ {}:{}", drop.db_id(), drop.table_id(),
                            drop.xid(), drop.lsn());

        auto&& drop_ddl = this->_drop_table(drop);
        ddls.push_back(drop_ddl);
    }

    // 5. perform a create table
    LOG_DEBUG(LOG_SCHEMA, "Create table: {}:{} @ {}:{}", create_req.db_id(),
                        create_req.table().id(), create_req.xid(), create_req.lsn());

    assert(create_req.lsn() == constant::RESYNC_CREATE_LSN);
    auto&& create_ddl = this->_create_table(create_req);
    ddls.push_back(create_ddl);

    for (const proto::IndexRequest& index : index_reqs) {
        LOG_DEBUG(LOG_SCHEMA, "Create index: {}:{} @ {}:{}", index.db_id(),
                            index.index().id(), index.xid(), index.lsn());

        CHECK_EQ(index.lsn(), constant::RESYNC_CREATE_LSN);

        // note: we don't store the create_index DDL since it doesn't need to be replayed at the FDW
        this->_create_index(index);
    }

    // 6. update the metadata of the table
    LOG_DEBUG(LOG_SCHEMA, "Update roots: {}:{} @ {}:{}", create_req.db_id(),
                        create_req.table().id(), create_req.xid(), create_req.lsn());
    this->_update_roots(roots_req);

    // 7. serialize the ddl json and return
    LOG_DEBUG(LOG_SCHEMA, "Response: {}", nlohmann::to_string(ddls));
    response->set_statement(nlohmann::to_string(ddls));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}


grpc::Status
Service::CreateUserType(grpc::ServerContext* context,
                        const proto::UserTypeRequest* request,
                        proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "CreateUserType");

    LOG_INFO("got CreateUserType() -- db {} namespace_id {} type_id {} name {} xid {} lsn {}",
                request->db_id(), request->namespace_id(), request->type_id(), request->name(), request->xid(),
                request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    // update the user_types table
    XidLsn xid(request->xid(), request->lsn());
    auto ddl = _mutate_usertype(request->db_id(), request->type_id(), request->name(),
        request->namespace_id(), request->type(), request->value_json(), xid, true);

    ddl["action"] = "ut_create";
    ddl["schema"] = request->namespace_name();

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::AlterUserType(grpc::ServerContext* context,
                       const proto::UserTypeRequest* request,
                       proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "AlterUserType");

    LOG_INFO("got AlterUserType() -- db {} namespace_id {} type_id {} name {} xid {} lsn {}",
                request->db_id(), request->namespace_id(), request->type_id(), request->name(), request->xid(),
                request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);
    XidLsn xid(request->xid(), request->lsn());

    // retrieve the old user type name
    auto user_type_info = _get_usertype_info(request->db_id(), request->type_id(), xid);
    CHECK(user_type_info != nullptr);

    // update the user defined types table
    auto ddl = _mutate_usertype(request->db_id(), request->type_id(), request->name(),
        request->namespace_id(), request->type(), request->value_json(), xid, true);

    ddl["action"] = "ut_alter";
    ddl["schema"] = request->namespace_name();
    ddl["old_name"] = user_type_info->name;
    ddl["old_value"] = user_type_info->value_json;

    // need to get old namespace id and check if it has changed
    if (user_type_info->namespace_id != request->namespace_id()) {
        auto old_ns_info = _get_namespace_info(request->db_id(), user_type_info->namespace_id, xid);
        ddl["old_schema"] = old_ns_info->name;
    } else {
        ddl["old_schema"] = request->namespace_name();
    }

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::DropUserType(grpc::ServerContext* context,
                      const proto::UserTypeRequest* request,
                      proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "DropUserType");

    LOG_INFO("got DropUserType() -- db {} namespace_id {} type_id {} xid {} lsn {}", request->db_id(),
                request->namespace_id(), request->type_id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    XidLsn xid(request->xid(), request->lsn());
    nlohmann::json ddl;
    auto user_type_info = _get_usertype_info(request->db_id(), request->type_id(), xid);
    if (user_type_info == nullptr) {
        // drop could for a type we don't support, so ignore it here
        LOG_WARN("User type {} not found", request->type_id());
        ddl["action"] = "no_change";
    } else {
        // update the user defined types table
        ddl = _mutate_usertype(request->db_id(), request->type_id(), request->name(),
            request->namespace_id(), request->type(), request->value_json(), xid, false);

        ddl["action"] = "ut_drop";
        ddl["schema"] = request->namespace_name();
    }

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::GetUserType(grpc::ServerContext* context,
                     const proto::GetUserTypeRequest* request,
                     proto::GetUserTypeResponse* response)
{
    ServerSpan span(context, "SysTblMgrService", "GetUserType");

    LOG_DEBUG(LOG_SCHEMA, "got GetUserType() -- db {} type_id {} xid {} lsn {}", request->db_id(),
              request->type_id(), request->xid(), request->lsn());

    boost::shared_lock lock(_read_mutex);

    XidLsn xid(request->xid(), constant::MAX_LSN);
    auto info = _get_usertype_info(request->db_id(), request->type_id(), xid);
    LOG_DEBUG(LOG_SCHEMA, "info is null? {}", info == nullptr);

    if (info != nullptr) {
        response->set_type_id(info->id);
        response->set_name(info->name);
        response->set_namespace_id(info->namespace_id);
        response->set_type(info->type);
        response->set_value_json(info->value_json);
        response->set_exists(info->exists);
    } else {
        response->set_exists(false);
    }

    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

std::vector<uint64_t>
_get_modified_partition_details(uint64_t db_id,
                                const XidLsn &xid,
                                uint64_t table_id,
                                const google::protobuf::RepeatedPtrField<proto::PartitionData> &partition_data,
                                std::unordered_map<uint64_t, std::pair<std::string, std::string>> *partition_map,
                                bool is_attached)
{
    auto table = TableMgr::get_instance()->get_table(db_id, sys_tbl::TableNames::ID,
        xid.xid);
    // get field array
    auto fields = table->extent_schema()->get_fields();

    std::unordered_set<uint64_t> system_table_ids;

    // Iterate the table_names table and find the child tables who has the parent_table_id as the current_table_id
    for (auto row : (*table)) {
        auto tid = fields->at(sys_tbl::TableNames::Data::TABLE_ID)->get_uint64(&row);
        auto parent_table_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);

        if (parent_table_id == table_id) {
            system_table_ids.insert(tid);
        }
    }

    // Parse the requested table ids
    std::unordered_set<uint64_t> table_ids;
    for ( const auto &part_data : partition_data ) {
        if (part_data.parent_table_id() != table_id) {
            LOG_WARN("Parent table id {} does not match the current table id {}",
                part_data.parent_table_id(), table_id);
            continue;
        }
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
            if (!table_ids.contains(tid)) {
                result.push_back(tid);
            }
        }
    }

    return result;
}

grpc::Status
Service::AttachPartition(grpc::ServerContext* context,
                         const proto::AttachPartitionRequest* request,
                         proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "AttachPartition");

    LOG_INFO("got AttachPartition() -- db {} table {} xid {} lsn {}", request->db_id(),
              request->table_id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    XidLsn xid(request->xid(), request->lsn());

    std::unordered_map<uint64_t, std::pair<std::string, std::string>> partition_map;
    auto attached_partitions = _get_modified_partition_details(request->db_id(), xid, request->table_id(), request->partition_data(), &partition_map, true);

    // Update the system table for the attached partition
    std::string partition_name = "";
    std::string partition_bound = "";
    for (const auto& attached_table_id : attached_partitions) {
        auto table_info = _get_table_info(request->db_id(), attached_table_id, xid);

        // note: table should always exist when calling alter_table()
        assert(table_info != nullptr);

        std::optional<std::string> partition_key = std::nullopt;
        if ( table_info->partition_key != "" ) {
            partition_key = table_info->partition_key;
        }

        // update the system table
        partition_bound = partition_map[attached_table_id].first;
        partition_key = partition_map[attached_table_id].second;
        auto updated_table_info =
            std::make_shared<TableCacheRecord>(attached_table_id, request->xid(), request->lsn(),
                                               table_info->namespace_id, table_info->name, true,
                                               request->table_id(), partition_key, partition_bound);
        _set_table_info(request->db_id(), updated_table_info);

        partition_name = table_info->name;
    }

    nlohmann::json ddl;

    ddl["action"] = "attach_partition";
    ddl["partition_name"] = partition_name;
    ddl["partition_bound"] = partition_bound;
    ddl["schema"] = request->namespace_name();
    ddl["table"] = request->table_name();

    LOG_DEBUG(LOG_SCHEMA, "Attach partition DDL: {}", ddl.dump());

    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

grpc::Status
Service::DetachPartition(grpc::ServerContext* context,
                         const proto::DetachPartitionRequest* request,
                         proto::DDLStatement* response)
{
    ServerSpan span(context, "SysTblMgrService", "DetachPartition");

    LOG_INFO("got DetachPartition() -- db {} table {} xid {} lsn {}", request->db_id(),
              request->table_id(), request->xid(), request->lsn());

    // acquire a shared lock to ensure no one is doing a finalize
    boost::shared_lock lock(_write_mutex);

    XidLsn xid(request->xid(), request->lsn());

    auto detached_partitions = _get_modified_partition_details(request->db_id(), xid, request->table_id(), request->partition_data(), nullptr, false);

    // Update the system table for the detached table
    std::string partition_name = "";
    for (const auto& detached_table_id : detached_partitions) {
        auto table_info = _get_table_info(request->db_id(), detached_table_id, xid);

        // note: table should always exist when calling alter_table()
        assert(table_info != nullptr);

        std::optional<std::string> partition_key = std::nullopt;
        if ( table_info->partition_key != "" ) {
            partition_key = table_info->partition_key;
        }

        // update the system table
        auto updated_table_info =
            std::make_shared<TableCacheRecord>(detached_table_id, request->xid(), request->lsn(),
                                               table_info->namespace_id, table_info->name, true,
                                               std::nullopt, partition_key, std::nullopt);
        _set_table_info(request->db_id(), updated_table_info);

        partition_name = table_info->name;
    }

    nlohmann::json ddl;

    ddl["action"] = "detach_partition";
    ddl["partition_name"] = partition_name;
    ddl["schema"] = request->namespace_name();
    ddl["table"] = request->table_name();

    LOG_DEBUG(LOG_SCHEMA, "Detach partition DDL: {}", ddl.dump());
    // serialize the JSON and return
    response->set_statement(nlohmann::to_string(ddl));
    span.span()->SetStatus(opentelemetry::trace::StatusCode::kOk);
    return grpc::Status::OK;
}

nlohmann::json
Service::_mutate_usertype(uint64_t db_id,
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
        sys_tbl::UserTypes::Data::tuple(type_id, ns_id, name, value_json, xid.xid, xid.lsn, type, exists);
    table->upsert(tuple, constant::UNKNOWN_EXTENT);

    return ddl;
}

Service::UserTypeCacheRecordPtr
Service::_get_usertype_info(uint64_t db_id, uint64_t type_id, const XidLsn& xid)
{
    // check the cache of un-finalized records
    {
        boost::unique_lock lock(_mutex);
        auto user_type_i = _usertype_id_cache[db_id].find(type_id);
        if (user_type_i != _usertype_id_cache[db_id].end()) {
            // note: we keep XID/LSN in reverse order to allow use of lower_bound() for lookup
            auto info_i = user_type_i->second.lower_bound(xid);
            if (info_i != user_type_i->second.end()) {
                LOG_DEBUG(LOG_SCHEMA, "Found user type {} in cache at exists {}", type_id, info_i->second->exists);
                return info_i->second;
            }
        }
    }

    // read from disk
    auto table = _get_system_table(db_id, sys_tbl::UserTypes::ID);
    auto schema = table->extent_schema();
    auto fields = schema->get_fields();

    auto search_key = sys_tbl::UserTypes::Primary::key_tuple(type_id, xid.xid, xid.lsn);
    LOG_DEBUG(LOG_SCHEMA, "Searching for user type {} at {}:{}", type_id, xid.xid, xid.lsn);

    // find the row that matches the type_id at the given XID/LSN
    auto row_i = table->inverse_lower_bound(search_key);
    auto &&row = *row_i;

    // make sure type ID exists at this XID/LSN
    auto id_field = fields->at(sys_tbl::UserTypes::Data::TYPE_ID);
    if (row_i == table->end()) {
        LOG_WARN("No user rows for search key type {} at xid {}:{}", type_id, xid.xid, xid.lsn);
        return nullptr;
    }

    LOG_DEBUG(LOG_SCHEMA, "Found user type id: {}", id_field->get_uint64(&row));
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


grpc::Status
Service::Revert(grpc::ServerContext* context,
                const proto::RevertRequest* request,
                google::protobuf::Empty* response)
{
    // ensure that we don't have a partially committed XID currently in-memory
    CHECK(_write[request->db_id()].empty());

    LOG_DEBUG(LOG_SCHEMA, "got Revert() -- db {} xid {}", request->db_id(),
                request->xid());

    // get the base directory for table data
    std::filesystem::path table_base;
    nlohmann::json json = Properties::get(Properties::STORAGE_CONFIG);
    Json::get_to<std::filesystem::path>(json, "table_dir", table_base);
    table_base = Properties::make_absolute_path(table_base);

    // go through each system table and adjust it's roots symlink to point to the correct file for
    // the committed XID
    for (auto table_id : sys_tbl::TABLE_IDS) {
        // get the table directory path
        auto table_dir = table_helpers::get_table_dir(table_base, request->db_id(), table_id, 1);

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
                uint64_t xid = std::stoull(filename.substr(6)); // skip "roots."
                roots_files.try_emplace(xid, entry.path());
            } catch (...) {
                // Skip files with invalid numbers
                continue;
            }
        }

        LOG_DEBUG(LOG_SCHEMA, "Found {} root files for system table {}",
                  roots_files.size(), table_id);

        // find the largest valid XID
        if (!roots_files.empty()) {
            // find the first XID beyond the committed XID
            auto del_i = roots_files.upper_bound(request->xid());
            auto root_i = std::make_reverse_iterator(del_i);

            if (root_i == roots_files.rend()) {
                LOG_DEBUG(LOG_SCHEMA, "Clear system table {}", root_i->second, table_id);
                // there's no valid roots, clear *all* of the system table data
                for (const auto& entry : std::filesystem::directory_iterator(table_dir)) {
                    std::filesystem::remove_all(entry.path());
                }
            } else {
                LOG_DEBUG(LOG_SCHEMA, "Picked root file {} for system table {}", root_i->second,
                          table_id);

                // update the symlink
                auto symlink_path = table_dir / "roots";

                std::filesystem::create_symlink(root_i->second,
                                                table_dir / constant::ROOTS_TMP_FILE);
                std::filesystem::rename(table_dir / constant::ROOTS_TMP_FILE,
                                        table_dir / constant::ROOTS_FILE);

                // remove any roots files with larger XIDs
                for (; del_i != roots_files.end(); ++del_i) {
                    LOG_DEBUG(LOG_SCHEMA, "Delete root file {} for system table {}",
                              del_i->second, table_id);
                    std::filesystem::remove(del_i->second);
                }
            }
        }

        // remove rows from the table that are beyond the committed XID
        auto mtable = _get_mutable_system_table(request->db_id(), table_id);
        auto table = _get_system_table(request->db_id(), table_id);
        FieldPtr xid_f = table->extent_schema()->get_field("xid");
        auto primary_fields = table->extent_schema()->get_fields(table->primary_key());
        for (auto row : *table) {
            if (xid_f->get_uint64(&row) > request->xid()) {
                mtable->remove(std::make_shared<FieldTuple>(primary_fields, &row),
                               constant::UNKNOWN_EXTENT);
            }
        }
    }

    return grpc::Status::OK;
}

Service::TableCacheRecordPtr
Service::_get_table_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
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
    info->exists = exists;

    uint64_t parent_table_id = constant::INVALID_TABLE;
    if (!fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->is_null(&row)) {
        parent_table_id = fields->at(sys_tbl::TableNames::Data::PARENT_TABLE_ID)->get_uint64(&row);
    }
    std::string partition_key;
    if (!fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->is_null(&row)) {
        partition_key = fields->at(sys_tbl::TableNames::Data::PARTITION_KEY)->get_text(&row);
    }
    std::string partition_bound;
    if (!fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->is_null(&row)) {
        partition_bound = fields->at(sys_tbl::TableNames::Data::PARTITION_BOUND)->get_text(&row);
    }
    info->parent_table_id = parent_table_id;
    info->partition_key = partition_key;
    info->partition_bound = partition_bound;

    // note: we currently only keep un-finalized mutations in the cache, so don't cache here
    return info;
}

Service::NamespaceCacheRecordPtr
Service::_get_namespace_info(uint64_t db_id, uint64_t namespace_id, const XidLsn& xid, bool check_exists)
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
    return std::make_shared<NamespaceCacheRecord>(
        namespace_id, fields->at(sys_tbl::NamespaceNames::Data::NAME)->get_text(&row),
        fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row));
}

Service::NamespaceCacheRecordPtr
Service::_get_namespace_info(uint64_t db_id, const std::string& name, const XidLsn& xid, bool check_exists)
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

    // return the namespace ID
    return std::make_shared<NamespaceCacheRecord>(
        fields->at(sys_tbl::NamespaceNames::Data::NAMESPACE_ID)->get_uint64(&row), name,
        fields->at(sys_tbl::NamespaceNames::Data::EXISTS)->get_bool(&row));
}

void
Service::_set_table_info(uint64_t db_id, TableCacheRecordPtr table_info)
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
                                         table_info->partition_bound);
    table_names_t->upsert(tuple, constant::UNKNOWN_EXTENT);
}

void
Service::_clear_table_info(uint64_t db_id)
{
    // clear the table cache since it only contains un-finalized entries
    boost::unique_lock lock(_mutex);
    _table_cache.erase(db_id);
}

void
Service::_clear_namespace_info(uint64_t db_id)
{
    boost::unique_lock lock(_mutex);
    _namespace_name_cache.erase(db_id);
    _namespace_id_cache.erase(db_id);
}

Service::RootsCacheRecordPtr
Service::_get_roots_info(uint64_t db_id, uint64_t table_id, const XidLsn& xid)
{
    // possibly cached stats
    uint64_t row_count = 0;
    uint64_t end_offset = 0;
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
            end_offset = xid_roots.second->stats().end_offset();
            stats_found = true;
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
    auto table_id_f = roots_t->extent_schema()->get_field("table_id");

    auto index_id_f = roots_t->extent_schema()->get_field("index_id");

    const std::string& sxid =
        sys_tbl::TableRoots::Data::SCHEMA[sys_tbl::TableRoots::Data::SNAPSHOT_XID].name;
    auto sxid_f = roots_t->extent_schema()->get_field(sxid);

    auto eid_f = roots_t->extent_schema()->get_field("extent_id");
    auto xid_f = roots_t->extent_schema()->get_field("xid");

    auto roots_info = std::make_shared<proto::GetRootsResponse>();

    for (const auto& idx: info->indexes()) {
        if (static_cast<sys_tbl::IndexNames::State>(idx.state()) != sys_tbl::IndexNames::State::READY &&
                static_cast<sys_tbl::IndexNames::State>(idx.state()) != sys_tbl::IndexNames::State::BEING_DELETED) {
            LOG_DEBUG(LOG_SCHEMA, "Index deleted or not-ready, so skipping the root {} -- {}",
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
                continue; // use cached data and go to the next index
            }
        }

        // not found in cached roots, go to the table
        auto search_key = sys_tbl::TableRoots::Primary::key_tuple(table_id, idx.id(), xid.xid);
        auto row_i = roots_t->inverse_lower_bound(search_key);

        if (row_i == roots_t->end()) {
            continue;
        }

        auto &&row = *row_i;
        if (table_id_f->get_uint64(&row) != table_id) {
            continue;
        }
        if (index_id_f->get_uint64(&row) != idx.id()) {
            continue;
        }

        auto record_xid = xid_f->get_uint64(&row);
        if (record_xid > xid.xid) {
            continue;
        }

        proto::RootInfo ri;
        ri.set_index_id(idx.id());
        ri.set_extent_id(eid_f->get_uint64(&row));
        *roots_info->add_roots() = ri;

        // use snapshot_xid of the last row
        snapshot_xid = sxid_f->get_uint64(&row);
    }

    if (roots_info->roots().empty()) {
        LOG_WARN("Couldn't find table_roots entry for {}@{}:{}", table_id, xid.xid,
                    xid.lsn);
    }

    // access the stats table
    if (!stats_found) {
        auto stats_t = _get_system_table(db_id, sys_tbl::TableStats::ID);
        auto stats_key_fields = stats_t->extent_schema()->get_sort_fields();

        auto search_key = sys_tbl::TableStats::Primary::key_tuple(table_id, xid.xid);
        auto srow_i = stats_t->inverse_lower_bound(search_key);
        auto &&row = *srow_i;

        // need to confirm that the table ID matches, but the XID may not match
        table_id_f = stats_t->extent_schema()->get_field("table_id");
        if (srow_i != stats_t->end() && table_id_f->get_uint64(&row) == table_id) {
            // retrieve the stats from the row
            auto row_count_f = stats_t->extent_schema()->get_field("row_count");
            auto end_offset_f = stats_t->extent_schema()->get_field("end_offset");
            row_count = row_count_f->get_uint64(&row);
            end_offset = end_offset_f->get_uint64(&row);
        } else {
            // no stats for this table?  seems like a potential error
            LOG_WARN("Couldn't find table_stats entry for {}@{}:{}", table_id, xid.xid, xid.lsn);
        }

    }

    roots_info->mutable_stats()->set_row_count(row_count);
    roots_info->mutable_stats()->set_end_offset(end_offset);
    roots_info->set_snapshot_xid(snapshot_xid);

    return roots_info;
}

void
Service::_set_roots_info(uint64_t db_id,
                         uint64_t table_id,
                         const XidLsn& xid,
                         RootsCacheRecordPtr roots_info)
{
    // cache the roots info
    boost::unique_lock lock(_mutex);
    _roots_cache[db_id][table_id][xid] = roots_info;
    lock.unlock();

    // update the table_roots
    auto table_roots_t = _get_mutable_system_table(db_id, sys_tbl::TableRoots::ID);
    for (auto const& r : roots_info->roots()) {
        auto tuple = sys_tbl::TableRoots::Data::tuple(table_id, r.index_id(), xid.xid,
                                                      r.extent_id(), roots_info->snapshot_xid());
        table_roots_t->upsert(tuple, constant::UNKNOWN_EXTENT);

        LOG_DEBUG(LOG_SCHEMA, "Updated root {}@{}:{} {} - {}", table_id, xid.xid, xid.lsn,
                            r.index_id(), r.extent_id());
    }

    // TODO: make table stats updates optional or move to a separate API
    // update the table_stats
    auto table_stats_t = _get_mutable_system_table(db_id, sys_tbl::TableStats::ID);
    auto tuple =
        sys_tbl::TableStats::Data::tuple(table_id, xid.xid, roots_info->stats().row_count(), roots_info->stats().end_offset());
    table_stats_t->upsert(tuple, constant::UNKNOWN_EXTENT);

    LOG_DEBUG(LOG_SCHEMA, "Updated stats {}@{}:{} - {}", table_id, xid.xid, xid.lsn,
                        roots_info->stats().row_count());
}

void
Service::_clear_roots_info(uint64_t db_id)
{
    // note: we clear everything because the cache only contains un-finalized data
    boost::unique_lock lock(_mutex);
    _roots_cache.erase(db_id);
}

Service::SchemaInfoPtr
Service::_get_schema_info(uint64_t db_id,
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
    LOG_DEBUG(LOG_SCHEMA, "Read schema info {}@{}:{} for @{}:{}", table_id,
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

    LOG_DEBUG(LOG_SCHEMA, "Tried to read history from disk: {}", info->history().size());

    // if the target is ahead of the guaranteed on-disk data then don't need to check the in-memory
    // data
    XidLsn xid = std::max(access_xid, read_xid);
    if (target_xid > xid) {
        // read any history from the cache
        _get_schema_cache_history(info, db_id, table_id, xid, target_xid);

        LOG_DEBUG(LOG_SCHEMA, "Tried to read history from memory: {}",
                            info->history().size());
    }

    return info;
}

void
Service::_read_schema_indexes(SchemaInfoPtr schema_info,
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
            LOG_DEBUG(LOG_SCHEMA, "No more indexes for table {} -- {}", table_id, tid);
            break;
        }

        XidLsn index_xid(names_fields->at(sys_tbl::IndexNames::Data::XID)->get_uint64(&row),
                         names_fields->at(sys_tbl::IndexNames::Data::LSN)->get_uint64(&row));

        if (access_xid < index_xid) {
            const XidLsn end_xid(schema_info->access_xid_end(), schema_info->access_lsn_end());
            LOG_DEBUG(LOG_SCHEMA, "No more data for table indexes {}@{}:{}", tid,
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
            LOG_DEBUG(LOG_SCHEMA, "Found deleted index {}@{}:{} - {}", tid, index_xid.xid,
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
            LOG_DEBUG(LOG_SCHEMA, "Found not-ready index {}@{}:{} - {}", tid,
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
Service::_populate_index_columns(uint64_t db_id, proto::IndexInfo& info, XidLsn index_xid) {
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
            LOG_DEBUG(LOG_SCHEMA, "No more indexes for table {} -- {}, {} -- {}",
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
Service::_read_schema_columns(SchemaInfoPtr info,
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
            LOG_DEBUG(LOG_SCHEMA, "No more data for table {} -- {}", table_id, tid);
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
            LOG_DEBUG(LOG_SCHEMA, "No more data for table column {}@{}:{}", tid, xid,
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
        LOG_DEBUG(LOG_SCHEMA, "Found no columns for table {}@{}:{}", table_id,
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

        LOG_DEBUG(LOG_SCHEMA, "Found index row {} for table {}@{}:{}", column_id,
                            table_id, access_xid.xid, access_xid.lsn);

        done = (index_pos == 0);
        if (!done) {
            --index_i;
        }
    }
}

void
Service::_apply_schema_cache_history(SchemaInfoPtr info,
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
                LOG_DEBUG(LOG_SCHEMA, "Checking end {}:{} < {}:{}", history.xid(),
                                    history.lsn(), end_xid.xid, end_xid.lsn);
                if (history_xid < end_xid) {
                    info->set_access_xid_end(history.xid());
                    info->set_access_lsn_end(history.lsn());
                }
                break;  // stop applying changes
            }

            // note: the schema's validity must start at least at this point
            const XidLsn start_xid(info->access_xid_start(), info->access_lsn_start());
            LOG_DEBUG(LOG_SCHEMA, "Checking start {}:{} < {}:{}", start_xid.xid,
                                start_xid.lsn, history.xid(), history.lsn());

            if (start_xid < history_xid) {
                info->set_access_xid_start(history.xid());
                info->set_access_lsn_start(history.lsn());
            }

            // Apply the recorded change, ensuring the columns remain sorted by column.position()
            auto* columns = info->mutable_columns();
            int target_position = history.column().position();
            LOG_DEBUG(LOG_SCHEMA, "Applying schema change: {}",
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
Service::_apply_index_cache_history(SchemaInfoPtr schema_info,
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
Service::_find_cached_index(uint64_t db_id,
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
Service::_read_schema_history(SchemaInfoPtr info,
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
Service::_get_schema_cache_history(SchemaInfoPtr info,
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
Service::_set_schema_info(uint64_t db_id,
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
            history.column().is_nullable(), value, history.update_type());
        schemas_t->upsert(tuple, constant::UNKNOWN_EXTENT);
    }
}

void
Service::_set_primary_index(uint64_t db_id,
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

    for (auto const& c : info->columns()) {
        if (!c.has_pk_position()) {
            continue;
        }
        proto::IndexColumn col;
        col.set_position(c.position());
        col.set_idx_position(c.pk_position());
        *index.add_columns() = col;
        primary_keys[c.pk_position()] = c.position();
    }

    _upsert_index_name(db_id, index, xid, primary_keys, true);
}

void
Service::_clear_schema_info(uint64_t db_id)
{
    boost::unique_lock lock(_mutex);
    _schema_cache.erase(db_id);
    _index_cache.erase(db_id);
}

TablePtr
Service::_get_system_table(uint64_t db_id, uint64_t table_id)
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
    TablePtr table = TableMgr::get_instance()->get_table(db_id, table_id, read_xid.xid);

    // cache the table interface
    cache[table_id] = table;
    return table;
}

MutableTablePtr
Service::_get_mutable_system_table(uint64_t db_id, uint64_t table_id)
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
        TableMgr::get_instance()->get_mutable_table(db_id, table_id, read_xid.xid, write_xid);

    // save the mutable table into the cache
    cache[table_id] = table;
    return table;
}

void
Service::_write_index(const XidLsn& xid,
                      uint64_t db_id,
                      uint64_t tab_id,
                      uint64_t index_id,
                      const std::map<uint32_t, uint32_t>& keys)
{
    if (keys.empty()) {
        LOG_INFO("The index has no keys: {}:{} - {}", db_id, tab_id, index_id);
        return;
    }

    auto indexes_t = _get_mutable_system_table(db_id, sys_tbl::Indexes::ID);
    auto fields = sys_tbl::Indexes::Data::fields(tab_id, index_id, xid.xid, xid.lsn,
                                                 0,   // empty position; filled below
                                                 0);  // empty column ID; filled below

    for (auto&& entry : keys) {
        fields->at(sys_tbl::Indexes::Data::POSITION) =
            std::make_shared<ConstTypeField<uint32_t>>(entry.first);
        fields->at(sys_tbl::Indexes::Data::COLUMN_ID) =
            std::make_shared<ConstTypeField<uint32_t>>(entry.second);

        indexes_t->upsert(std::make_shared<FieldTuple>(fields, nullptr),
                          constant::UNKNOWN_EXTENT);
    }
}

proto::ColumnHistory
Service::_generate_update(const google::protobuf::RepeatedPtrField<proto::TableColumn>& old_schema,
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
            if (old_col->pk_position() != new_col->pk_position()) {
                ddl["action"] = "resync";
                update.set_update_type(static_cast<int8_t>(SchemaUpdateType::RESYNC));
                return update;
            }
        }
    }

    // No change detected
    ddl["action"] = "no_change";
    update.set_update_type(static_cast<int8_t>(SchemaUpdateType::NO_CHANGE));
    return update;
}
}  // namespace springtail::sys_tbl_mgr
