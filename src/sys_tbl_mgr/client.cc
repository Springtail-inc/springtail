#include <string>
#include <string_view>
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
#include <sys_tbl_mgr/client_factory.hh>

namespace springtail::sys_tbl_mgr {
    /* static initialization must happen outside of class */
    Client* Client::_instance {nullptr};
    std::mutex Client::_instance_mutex;

    Client *
    Client::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new Client();
        }

        return _instance;
    }

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
        int max_connections;
        int port;
        std::string server;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55051);

        if (!Json::get_to<std::string>(client_json, "server", server)) {
            throw Error("Host not found in sys_tbl_mgr.server settings");
        }

        // construct the thrift client pool.
        // First argument is a factory object that constructs a thrift clients
        // using the host and port from above
        _thrift_client_pool = std::make_shared<ObjectPool<ServiceClient>>(
            std::make_shared<ObjectFactory>(server, port),
            max_connections/2,
            max_connections
        );
    }

    void
    Client::shutdown()
    {
         std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    // exposed client service interface below

    void
    Client::ping()
    {
        ThriftClient c = _get_client();
        Status result;

        c.client->ping(result);

        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    TableRequest
    _gen_table_request(uint64_t db_id,
                       const XidLsn &xid,
                       const PgMsgTable &msg)
    {
        TableRequest request;
        request.db_id = db_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;
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

    std::string
    Client::create_table(uint64_t db_id,
                         const XidLsn &xid,
                         const PgMsgTable &msg)
    {
        ThriftClient c = _get_client();
        DDLStatement result;

        auto &&request = _gen_table_request(db_id, xid, msg);
        c.client->create_table(result, request);

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
        ThriftClient c = _get_client();
        DDLStatement result;

        auto &&request = _gen_table_request(db_id, xid, msg);
        c.client->alter_table(result, request);

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
        ThriftClient c = _get_client();
        DDLStatement result;

        DropTableRequest request;
        request.db_id = db_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;
        request.table_id = msg.oid;
        request.schema = msg.schema;
        request.name = msg.table;

        c.client->drop_table(result, request);

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
        ThriftClient c = _get_client();
        Status result;

        UpdateRootsRequest request;
        request.db_id = db_id;
        request.xid = xid;
        request.table_id = table_id;
        request.roots.insert(request.roots.end(), metadata.roots.begin(), metadata.roots.end());
        request.stats.row_count = metadata.stats.row_count;
        request.snapshot_xid = metadata.snapshot_xid;

        c.client->update_roots(result, request);

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }

    void
    Client::finalize(uint64_t db_id,
                     uint64_t xid)
    {
        ThriftClient c = _get_client();
        Status result;

        FinalizeRequest request;
        request.db_id = db_id;
        request.xid = xid;

        c.client->finalize(result, request);

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }

    TableMetadata
    Client::get_roots(uint64_t db_id,
                      uint64_t table_id,
                      uint64_t xid)
    {
        ThriftClient c = _get_client();
        GetRootsResponse result;

        GetRootsRequest request;
        request.db_id = db_id;
        request.xid = xid;
        request.table_id = table_id;

        c.client->get_roots(result, request);

        TableMetadata metadata;
        metadata.roots.insert(metadata.roots.end(),
                              result.roots.begin(), result.roots.end());
        metadata.stats.row_count = result.stats.row_count;
        metadata.snapshot_xid = result.snapshot_xid;

        return metadata;
    }

    SchemaMetadata
    Client::get_schema(uint64_t db_id,
                       uint64_t table_id,
                       const XidLsn &xid)
    {
        ThriftClient c = _get_client();
        GetSchemaResponse result;

        GetSchemaRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;

        c.client->get_schema(result, request);

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

    SchemaMetadata
    Client::get_target_schema(uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn &access_xid,
                              const XidLsn &target_xid)
    {
        ThriftClient c = _get_client();
        GetSchemaResponse result;

        GetTargetSchemaRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.access_xid = access_xid.xid;
        request.access_lsn = access_xid.lsn;
        request.target_xid = target_xid.xid;
        request.target_lsn = target_xid.lsn;

        c.client->get_target_schema(result, request);

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
        ThriftClient c = _get_client();

        ExistsRequest request;
        request.db_id = db_id;
        request.table_id = table_id;
        request.xid = xid.xid;
        request.lsn = xid.lsn;

        return c.client->exists(request);
    }
    

} // namespace
