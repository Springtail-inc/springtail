#include <string>
#include <memory>
#include <cassert>

#include <nlohmann/json.hpp>

#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TCompactProtocol.h>

#include <common/properties.hh>
#include <common/logging.hh>
#include <common/json.hh>
#include <common/object_cache.hh>
#include <common/common.hh>
#include <common/exception.hh>

#include <thrift/sys_tbl_mgr/Service.h>

#include <sys_tbl_mgr/exception.hh>
#include <sys_tbl_mgr/client.hh>

#include <vector>

namespace springtail::sys_tbl_mgr {

    Client::Client()
    {
        nlohmann::json json = Properties::get(Properties::SYS_TBL_MGR_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        // fetch properties for the sys tbl mgr
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("Sys tbl mgr client settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Sys tbl mgr server settings not found");
        }

        // init channel pool
        int max_connections = Json::get_or<int>(client_json, "connections", 8);
        int port = Json::get_or<int>(server_json, "port", 55051);

        std::string server = Properties::get_sys_tbl_mgr_hostname();

        init(server, port, max_connections);
    }

    // exposed client service interface below

    void
    Client::ping()
    {
        Status result;
        _invoke_with_retries([&result](ThriftClient &c) {
            c.client->ping(result);
        });
        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    template<typename Req>
    void _set_request_common(Req& r, uint64_t db_id,
                       const XidLsn &xid) {
        r.db_id = db_id;
        r.xid = xid.xid;
        r.lsn = xid.lsn;
    }

    TableRequest
    _gen_table_request(uint64_t db_id,
                       const XidLsn &xid,
                       const PgMsgTable &msg)
    {
        TableRequest request;
        _set_request_common(request, db_id, xid);
        request.table.id = msg.oid;
        request.table.schema = msg.schema;
        request.table.name = msg.table;
        for (const auto &col : msg.columns) {
            TableColumn column;
            column.__set_name(col.column_name);
            column.__set_type(col.type);
            column.__set_pg_type(col.pg_type);
            column.__set_position(col.position);
            column.__set_is_nullable(col.is_nullable);
            column.__set_is_generated(col.is_generated);
            if (col.is_pkey) {
                column.__set_pk_position(col.pk_position);
            }
            if (col.default_value) {
                column.__set_default_value(*col.default_value);
            }

            request.table.columns.push_back(column);
        }
        return request;
    }

    IndexRequest
    _gen_index_request(uint64_t db_id,
                       const XidLsn &xid,
                       const PgMsgIndex &msg)
    {
        IndexRequest request;
        _set_request_common(request, db_id, xid);
        request.index.id = msg.oid;
        request.index.schema = msg.schema;
        request.index.name = msg.index;
        request.index.is_unique = msg.is_unique;
        request.index.table_name = msg.table_name;
        request.index.table_id = msg.table_oid;
        for (const auto &col : msg.columns) {
            IndexColumn column;
            column.position = col.position;
            column.name = col.name;
            column.idx_position = col.idx_position;
            request.index.columns.push_back(column);
        }
        return request;
    }

    std::string
    Client::create_table(uint64_t db_id,
                         const XidLsn &xid,
                         const PgMsgTable &msg)
    {
        DDLStatement result;

        auto &&request = _gen_table_request(db_id, xid, msg);
        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->create_table(result, request);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    std::string
    Client::alter_table(uint64_t db_id,
                        const XidLsn &xid,
                        const PgMsgTable &msg)
    {
        DDLStatement result;

        auto &&request = _gen_table_request(db_id, xid, msg);
        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->alter_table(result, request);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    std::string
    Client::drop_table(uint64_t db_id,
                       const XidLsn &xid,
                       const PgMsgDropTable &msg)
    {
        DDLStatement result;

        DropTableRequest request;
        request.db_id = db_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;
        request.table_id = msg.oid;
        request.schema = msg.schema;
        request.name = msg.table;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->drop_table(result, request);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }


    std::string
    Client::create_index(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg, sys_tbl::IndexNames::State state)
    {
        DDLStatement result;

        auto &&request = _gen_index_request(db_id, xid, msg);
        request.index.state=static_cast<int8_t>(state);

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->create_index(result, request);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    void
    Client::set_index_state(uint64_t db_id, const XidLsn &xid, uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state)
    {
        Status result;

        SetIndexStateRequest request;
        _set_request_common(request, db_id, xid);

        request.table_id = table_id;
        request.index_id = index_id;
        request.state = static_cast<uint8_t>(state);

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->set_index_state(result, request);
        });

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }


    IndexInfo
    Client::get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid)
    {
        IndexInfo result;

        GetIndexInfoRequest request;
        _set_request_common(request, db_id, xid);
        request.index_id = index_id;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->get_index_info(result, request);
        });

        return result;
    }


    std::string
    Client::drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg)
    {
        DDLStatement result;

        DropIndexRequest request;
        _set_request_common(request, db_id, xid);

        request.index_id = msg.oid;
        request.schema = msg.schema;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->drop_index(result, request);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    void
    Client::update_roots(uint64_t db_id,
                         uint64_t table_id,
                         uint64_t xid,
                         const TableMetadata &metadata)
    {
        Status result;

        UpdateRootsRequest request;
        request.db_id = db_id;
        request.xid = xid;
        request.table_id = table_id;
        for (auto const& [index_id, extent_id]: metadata.roots) {
            sys_tbl_mgr::RootInfo ri;
            ri.index_id = index_id;
            ri.extent_id = extent_id;
            request.roots.push_back(ri);
        }

        request.stats.row_count = metadata.stats.row_count;
        request.snapshot_xid = metadata.snapshot_xid;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->update_roots(result, request);
        });

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }

    void
    Client::finalize(uint64_t db_id,
                     uint64_t xid)
    {
        Status result;

        FinalizeRequest request;
        request.db_id = db_id;
        request.xid = xid;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->finalize(result, request);
        });

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }

    TableMetadata
    Client::get_roots(uint64_t db_id,
                      uint64_t table_id,
                      uint64_t xid)
    {
        GetRootsResponse result;

        GetRootsRequest request;
        request.db_id = db_id;
        request.xid = xid;
        request.table_id = table_id;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->get_roots(result, request);
        });

        TableMetadata metadata;
        for (auto const &r: result.roots) {
            metadata.roots.emplace_back(r.index_id, r.extent_id);
        }
        metadata.stats.row_count = result.stats.row_count;
        metadata.snapshot_xid = result.snapshot_xid;

        return metadata;
    }

    SchemaMetadata
    Client::get_schema(uint64_t db_id,
                       uint64_t table_id,
                       const XidLsn &xid)
    {
        GetSchemaResponse result;

        GetSchemaRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->get_schema(result, request);
        });

        SchemaMetadata metadata;
        for (auto column : result.columns) {
            SchemaColumn value(column.name,
                               column.position,
                               static_cast<SchemaType>(column.type),
                               column.pg_type,
                               column.is_nullable);
            if (column.__isset.pk_position) {
                value.pkey_position = column.pk_position;
            }
            if (column.__isset.default_value) {
                value.default_value = column.default_value;
            }

            metadata.columns.push_back(value);
        }

        for (auto history : result.history) {
            SchemaColumn value(history.xid,
                               history.lsn,
                               history.column.name,
                               history.column.position,
                               static_cast<SchemaType>(history.column.type),
                               history.column.pg_type,
                               history.exists,
                               history.column.is_nullable);
            value.update_type = static_cast<SchemaUpdateType>(history.update_type);
            if (history.column.__isset.pk_position) {
                value.pkey_position = history.column.pk_position;
            }
            if (history.column.__isset.default_value) {
                value.default_value = history.column.default_value;
            }

            metadata.history.push_back(value);
        }

        for (auto const& idx: result.indexes) {
            Index info;
            info.id = idx.id;
            info.name = idx.name;
            info.schema = idx.schema;
            info.state = idx.state;
            info.table_id = idx.table_id;
            info.is_unique = idx.is_unique;
            for (auto const& col: idx.columns) {
                info.columns.emplace_back(col.idx_position, col.position);
            }
            //sort by index position
            std::ranges::sort(info.columns, [](auto const& a, auto const& b) {return a.idx_position < b.idx_position;});
            metadata.indexes.push_back(std::move(info));
        }

        return metadata;
    }

    SchemaMetadata
    Client::get_target_schema(uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn &access_xid,
                              const XidLsn &target_xid)
    {
        GetSchemaResponse result;

        GetTargetSchemaRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.access_xid = access_xid.xid;
        request.access_lsn = access_xid.lsn;
        request.target_xid = target_xid.xid;
        request.target_lsn = target_xid.lsn;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->get_target_schema(result, request);
        });

        SchemaMetadata metadata;
        for (auto column : result.columns) {
            SchemaColumn value(column.name,
                               column.position,
                               static_cast<SchemaType>(column.type),
                               column.pg_type,
                               column.is_nullable);
            if (column.__isset.pk_position) {
                value.pkey_position = column.pk_position;
            }
            if (column.__isset.default_value) {
                value.default_value = column.default_value;
            }

            metadata.columns.push_back(value);
        }

        for (auto history : result.history) {
            SchemaColumn value(history.xid,
                               history.lsn,
                               history.column.name,
                               history.column.position,
                               static_cast<SchemaType>(history.column.type),
                               history.column.pg_type,
                               history.exists,
                               history.column.is_nullable);
            value.update_type = static_cast<SchemaUpdateType>(history.update_type);
            if (history.column.__isset.pk_position) {
                value.pkey_position = history.column.pk_position;
            }
            if (history.column.__isset.default_value) {
                value.default_value = history.column.default_value;
            }

            metadata.history.push_back(value);
        }

        return metadata;
    }

    bool
    Client::exists(uint64_t db_id,
                   uint64_t table_id,
                   const XidLsn &xid)
    {
        ExistsRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;

        bool ret = false;
        _invoke_with_retries([&ret, &request](ThriftClient &c) {
            ret = c.client->exists(request);
        });

        return ret;
    }

    std::string
    Client::swap_sync_table(const TableRequest &create,
                            const UpdateRootsRequest &roots)
    {
        DDLStatement result;

        _invoke_with_retries([&result, &create, &roots](ThriftClient &c) {
            c.client->swap_sync_table(result, create, roots);
        });

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

} // namespace
