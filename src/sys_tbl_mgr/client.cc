#include <grpcpp/grpcpp.h>
#include <proto/sys_tbl_mgr.grpc.pb.h>

#include <cassert>
#include <memory>
#include <string>

#include <common/common.hh>
#include <common/exception.hh>
#include <grpc/grpc_client.hh>
#include <common/json.hh>
#include <common/logging.hh>
#include <common/object_cache.hh>
#include <common/properties.hh>
#include <nlohmann/json.hpp>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/exception.hh>

namespace springtail::sys_tbl_mgr {

Client::Client()
{
    nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
    std::string server = Properties::get_sys_tbl_mgr_hostname();
    uint64_t cache_size;

    if (!Json::get_to<uint64_t>(json, "cache_size", cache_size)) {
        throw Error("Sys tbl mgr cache size settings not found");
    }

    nlohmann::json rpc_json;
    if (!Json::get_to<nlohmann::json>(json, "rpc_config", rpc_json)) {
        throw Error("Sys tbl mgr RPC settings not found");
    }

    auto channel = grpc_client::create_channel("SysTblMgr", server, rpc_json);
    _stub = proto::SysTblMgr::NewStub(channel);
    _schema_cache = std::make_shared<SchemaCache>(cache_size);
}

void
Client::ping()
{
    google::protobuf::Empty request;
    google::protobuf::Empty response;

    grpc_client::retry_rpc("SysTblMgr", "Ping",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->Ping(context, request, &response);
                           });
}

template <typename Req>
void
_set_request_common(Req &r, uint64_t db_id, const XidLsn &xid)
{
    r.set_db_id(db_id);
    r.set_xid(xid.xid);
    r.set_lsn(xid.lsn);
}

proto::TableRequest
_gen_table_request(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
{
    proto::TableRequest request;
    _set_request_common(request, db_id, xid);

    auto *table = request.mutable_table();
    table->set_id(msg.oid);
    table->set_namespace_name(msg.namespace_name);
    table->set_name(msg.table);

    for (const auto &col : msg.columns) {
        auto *column = table->add_columns();
        column->set_name(col.name);
        column->set_type(col.type);
        column->set_pg_type(col.pg_type);
        column->set_position(col.position);
        column->set_is_nullable(col.is_nullable);
        column->set_is_generated(col.is_generated);
        column->set_type_name(col.type_name);
        if (col.is_pkey) {
            column->set_pk_position(col.pk_position);
        }
        if (col.default_value) {
            column->set_default_value(*col.default_value);
        }
    }
    return request;
}

proto::IndexRequest
_gen_index_request(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg)
{
    proto::IndexRequest request;
    _set_request_common(request, db_id, xid);
    auto *index = request.mutable_index();
    index->set_id(msg.oid);
    index->set_namespace_name(msg.namespace_name);
    index->set_name(msg.index);
    index->set_is_unique(msg.is_unique);
    index->set_table_name(msg.table_name);
    index->set_table_id(msg.table_oid);
    for (const auto &col : msg.columns) {
        auto *column = index->add_columns();
        column->set_position(col.position);
        column->set_name(col.name);
        column->set_idx_position(col.idx_position);
    }
    return request;
}

std::string
Client::create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
{
    auto request = _gen_table_request(db_id, xid, msg);
    proto::DDLStatement response;

    grpc_client::retry_rpc("SysTblMgr", "CreateTable",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateTable(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError();
    }
    return response.statement();
}

std::string
Client::alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg)
{
    auto request = _gen_table_request(db_id, xid, msg);
    proto::DDLStatement response;

    grpc_client::retry_rpc("SysTblMgr", "AlterTable",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->AlterTable(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError();
    }

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.oid, xid);

    return response.statement();
}

std::string
Client::drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg)
{
    proto::DropTableRequest request;
    _set_request_common(request, db_id, xid);
    request.set_table_id(msg.oid);
    request.set_namespace_name(msg.namespace_name);
    request.set_name(msg.table);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "DropTable",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->DropTable(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError();
    }

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.oid, xid);
    return response.statement();
}

std::string
Client::create_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    _set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "CreateNamespace",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateNamespace(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::alter_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    _set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "AlterNamespace",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->AlterNamespace(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::drop_namespace(uint64_t db_id, const XidLsn &xid, const PgMsgNamespace &msg)
{
    proto::NamespaceRequest request;
    _set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "DropNamespace",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->DropNamespace(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::create_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    _set_request_common(request, db_id, xid);
    request.set_type_id(msg.oid);
    request.set_namespace_id(msg.namespace_id);
    request.set_name(msg.name);
    request.set_namespace_name(msg.namespace_name);
    request.set_value_json(msg.value_json);
    request.set_type(msg.type);

    proto::DDLStatement response;

    grpc_client::retry_rpc("SysTblMgr", "CreateUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::alter_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    _set_request_common(request, db_id, xid);
    request.set_type_id(msg.oid);
    request.set_namespace_id(msg.namespace_id);
    request.set_name(msg.name);
    request.set_namespace_name(msg.namespace_name);
    request.set_value_json(msg.value_json);
    request.set_type(msg.type);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "AlterUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->AlterUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::drop_usertype(uint64_t db_id, const XidLsn &xid, const PgMsgUserType &msg)
{
    proto::UserTypeRequest request;
    _set_request_common(request, db_id, xid);
    request.set_namespace_id(msg.oid);
    request.set_name(msg.name);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "DropUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->DropUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::create_index(uint64_t db_id,
                     const XidLsn &xid,
                     const PgMsgIndex &msg,
                     sys_tbl::IndexNames::State state)
{
    auto request = _gen_index_request(db_id, xid, msg);
    auto *index = request.mutable_index();
    index->set_state(static_cast<int32_t>(state));

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "CreateIndex",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateIndex(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, msg.table_oid, xid);

    return response.statement();
}

void
Client::set_index_state(uint64_t db_id,
                        const XidLsn &xid,
                        uint64_t table_id,
                        uint64_t index_id,
                        sys_tbl::IndexNames::State state)
{
    proto::SetIndexStateRequest request;
    _set_request_common(request, db_id, xid);
    request.set_table_id(table_id);
    request.set_index_id(index_id);
    request.set_state(static_cast<int32_t>(state));

    google::protobuf::Empty response;
    grpc_client::retry_rpc("SysTblMgr", "SetIndexState",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->SetIndexState(context, request, &response);
                           });

    // Automatically invalidate the schema cache from the provided XID
    invalidate_table(db_id, table_id, xid);
}

proto::IndexInfo
Client::get_index_info(uint64_t db_id,
                       uint64_t index_id,
                       const XidLsn &xid,
                       std::optional<uint64_t> tid)
{
    proto::GetIndexInfoRequest request;
    _set_request_common(request, db_id, xid);
    request.set_index_id(index_id);
    if (tid) {
        request.set_table_id(*tid);
    }

    proto::IndexInfo response;
    grpc_client::retry_rpc("SysTblMgr", "GetIndexInfo",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->GetIndexInfo(context, request, &response);
                           });

    return response;
}

std::string
Client::drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg)
{
    proto::DropIndexRequest request;
    _set_request_common(request, db_id, xid);
    request.set_index_id(msg.oid);
    request.set_namespace_name(msg.namespace_name);

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "DropIndex",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->DropIndex(context, request, &response);
                           });

    // Automatically invalidate the schema cache from the provided XID
    _schema_cache->invalidate_by_index(db_id, msg.oid, xid);

    return response.statement();
}

void
Client::update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid, const TableMetadata &metadata)
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
    request.set_snapshot_xid(metadata.snapshot_xid);

    google::protobuf::Empty response;
    grpc_client::retry_rpc("SysTblMgr", "UpdateRoots",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->UpdateRoots(context, request, &response);
                           });
}

void
Client::finalize(uint64_t db_id, uint64_t xid)
{
    proto::FinalizeRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);

    google::protobuf::Empty response;
    grpc_client::retry_rpc("SysTblMgr", "Finalize",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->Finalize(context, request, &response);
                           });
}

void
Client::revert(uint64_t db_id, uint64_t xid)
{
    proto::RevertRequest request;
    request.set_db_id(db_id);
    request.set_xid(xid);

    google::protobuf::Empty response;
    grpc_client::retry_rpc("SysTblMgr", "Revert",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->Revert(context, request, &response);
                           });
}

void Client::use_roots_cache(std::shared_ptr<ShmCache> c)
{
    _roots_cache.store(std::move(c));
}

TableMetadataPtr
Client::get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid)
{
    proto::GetRootsResponse response;
    bool found = false;

    auto cache = _roots_cache.load();
    if (cache) {
        auto msg = cache->find(db_id, table_id, xid);
        if (msg) {
            found = response.ParseFromString(msg.value());
            CHECK(found);
        }
    }

    if (!found) {
        proto::GetRootsRequest request;
        request.set_db_id(db_id);
        request.set_table_id(table_id);
        request.set_xid(xid);

        grpc_client::retry_rpc("SysTblMgr", "GetRoots",
                               [this, &request, &response](grpc::ClientContext *context) {
                                   return _stub->GetRoots(context, request, &response);
                               });

        if (cache) {
            std::string msg = response.SerializeAsString();
            cache->insert(db_id, table_id, xid, msg);
        }
    }

    auto metadata = std::make_shared<TableMetadata>();
    for (const auto &root : response.roots()) {
        metadata->roots.push_back({root.index_id(), root.extent_id()});
    }
    metadata->stats.row_count = response.stats().row_count();
    metadata->stats.end_offset = response.stats().end_offset();
    metadata->snapshot_xid = response.snapshot_xid();

    return metadata;
}

SchemaMetadataPtr
Client::_pack_metadata(const proto::GetSchemaResponse &result)
{
    auto metadata = std::make_shared<SchemaMetadata>();
    for (const auto &column : result.columns()) {
        SchemaColumn value(column.name(), column.position(), static_cast<SchemaType>(column.type()),
                           column.pg_type(), column.is_nullable());
        if (column.has_pk_position()) {
            value.pkey_position = column.pk_position();
        }
        if (column.has_default_value()) {
            value.default_value = column.default_value();
        }

        metadata->columns.push_back(value);
    }
    // sort columns by position
    std::ranges::sort(metadata->columns,
                      [](auto const &a, auto const &b) { return a.position < b.position; });

    for (const auto &history : result.history()) {
        const auto &column = history.column();
        SchemaColumn value(history.xid(), history.lsn(), column.name(), column.position(),
                           static_cast<SchemaType>(column.type()), column.pg_type(),
                           history.exists(), column.is_nullable());
        value.update_type = static_cast<SchemaUpdateType>(history.update_type());
        if (column.has_pk_position()) {
            value.pkey_position = column.pk_position();
        }
        if (column.has_default_value()) {
            value.default_value = column.default_value();
        }

        metadata->history.push_back(value);
    }

    for (const auto &idx : result.indexes()) {
        Index info;
        info.id = idx.id();
        info.name = idx.name();
        info.schema = idx.namespace_name();
        info.state = idx.state();
        info.table_id = idx.table_id();
        info.is_unique = idx.is_unique();
        for (const auto &col : idx.columns()) {
            info.columns.emplace_back(col.idx_position(), col.position());
        }
        // sort by index position
        std::ranges::sort(info.columns, [](auto const &a, auto const &b) {
            return a.idx_position < b.idx_position;
        });
        metadata->indexes.push_back(std::move(info));
    }

    XidLsn access_start(result.access_xid_start(), result.access_lsn_start());
    XidLsn access_end(result.access_xid_end(), result.access_lsn_end());
    XidLsn target_start(result.target_xid_start(), result.target_lsn_start());
    XidLsn target_end(result.target_xid_end(), result.target_lsn_end());

    metadata->access_range = XidRange(access_start, access_end);
    metadata->target_range = XidRange(target_start, target_end);

    return metadata;
}

std::shared_ptr<const SchemaMetadata>
Client::get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    auto populate = [this](uint64_t db, uint64_t tid, const XidLsn &xid) {
        proto::GetSchemaRequest request;
        request.set_db_id(db);
        request.set_table_id(tid);
        request.set_xid(xid.xid);
        request.set_lsn(xid.lsn);

        proto::GetSchemaResponse response;
        grpc_client::retry_rpc("SysTblMgr", "GetSchema",
                               [this, &request, &response](grpc::ClientContext *context) {
                                   return _stub->GetSchema(context, request, &response);
                               });

        return _pack_metadata(response);
    };

    // Retrieve through the schema cache
    return _schema_cache->get(db_id, table_id, xid, populate);
}

SchemaMetadataPtr
Client::get_target_schema(uint64_t db_id,
                          uint64_t table_id,
                          const XidLsn &access_xid,
                          const XidLsn &target_xid)
{
    proto::GetTargetSchemaRequest request;
    request.set_db_id(db_id);
    request.set_table_id(table_id);
    request.set_access_xid(access_xid.xid);
    request.set_access_lsn(access_xid.lsn);
    request.set_target_xid(target_xid.xid);
    request.set_target_lsn(target_xid.lsn);

    proto::GetSchemaResponse response;
    grpc_client::retry_rpc("SysTblMgr", "GetTargetSchema",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->GetTargetSchema(context, request, &response);
                           });

    return _pack_metadata(response);
}

bool
Client::exists(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    // prepare the request
    proto::ExistsRequest request;
    _set_request_common(request, db_id, xid);
    request.set_table_id(table_id);
    google::protobuf::Empty response;

    auto status = grpc_client::retry_rpc_status(
        "SysTblMgr", "Exists", [this, &request, &response](grpc::ClientContext *context) {
            return _stub->Exists(context, request, &response);
        });

    if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
        return false;
    } else if (!status.ok()) {
        throw SysTblMgrError(status.error_message());
    }

    return true;
}

std::string
Client::create_namespace(const proto::NamespaceRequest &request)
{
    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "CreateNamespace",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateNamespace(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::swap_sync_table(const proto::NamespaceRequest &namespace_req,
                        const proto::TableRequest &create_req,
                        const std::vector<proto::IndexRequest> &index_reqs,
                        const proto::UpdateRootsRequest &roots_req)
{
    proto::SwapSyncTableRequest request;
    *request.mutable_namespace_req() = namespace_req;
    *request.mutable_create_req() = create_req;
    for (const auto &idx_req : index_reqs) {
        *request.add_index_reqs() = idx_req;
    }
    *request.mutable_roots_req() = roots_req;

    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "SwapSyncTable",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->SwapSyncTable(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError();
    }

    // Auto-invalidate the cache for the swapped table
    invalidate_table(create_req.db_id(), create_req.table().id(),
                     XidLsn(create_req.xid(), create_req.lsn()));

    return response.statement();
}

void
Client::invalidate_table(uint64_t db_id, uint64_t table_id, const XidLsn &xid)
{
    _schema_cache->invalidate_table(db_id, table_id, xid);
}

void
Client::invalidate_db(uint64_t db_id, const XidLsn &xid)
{
    _schema_cache->invalidate_db(db_id, xid);
}

std::string
Client::create_usertype(const proto::UserTypeRequest &request)
{
    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "CreateUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->CreateUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::drop_usertype(const proto::UserTypeRequest &request)
{
    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "DropUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->DropUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

std::string
Client::alter_usertype(const proto::UserTypeRequest &request)
{
    proto::DDLStatement response;
    grpc_client::retry_rpc("SysTblMgr", "AlterUserType",
                           [this, &request, &response](grpc::ClientContext *context) {
                               return _stub->AlterUserType(context, request, &response);
                           });

    if (response.statement().empty()) {
        throw SysTblMgrError("No DDL statement");
    }
    return response.statement();
}

}  // namespace springtail::sys_tbl_mgr
