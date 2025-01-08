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
#include <sys_tbl_mgr/client_factory.hh>
#include <vector>

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
        uint64_t cache_size;

        // fetch properties for the sys tbl mgr
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("Sys tbl mgr client settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Sys tbl mgr server settings not found");
        }

        if (!Json::get_to<uint64_t>(json, "cache_size", cache_size)) {
            throw Error("Sys tbl mgr cache size settings not found");
        }

        // create the caches
        //_schema_cache = std::make_shared<Cache<SchemaKey, SchemaValue>>(cache_size);
        //_roots_cache = std::make_shared<Cache<MetadataKey, MetadataValue>>(cache_size);
        _schema_cache = std::make_shared<SchemaCache>(cache_size, cache_size * 8);

        // init channel pool
        int max_connections;
        int port;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55051);

        std::string server = Properties::get_sys_tbl_mgr_hostname();

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


    std::string 
    Client::create_index(uint64_t db_id, const XidLsn &xid, const PgMsgIndex &msg, sys_tbl::IndexNames::State state)
    {
        ThriftClient c = _get_client();
        DDLStatement result;

        auto &&request = _gen_index_request(db_id, xid, msg);
        request.index.state=static_cast<int8_t>(state);

        c.client->create_index(result, request);

        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    void
    Client::set_index_state(uint64_t db_id, const XidLsn &xid, uint64_t table_id, uint64_t index_id, sys_tbl::IndexNames::State state)
    {
        ThriftClient c = _get_client();
        Status result;

        SetIndexStateRequest request;
        _set_request_common(request, db_id, xid);

        request.table_id = table_id;
        request.index_id = index_id;
        request.state = static_cast<uint8_t>(state);

        c.client->set_index_state(result, request);

        if (result.status != StatusCode::SUCCESS) {
            throw SysTblMgrError(result.message);
        }
    }


    IndexInfo 
    Client::get_index_info(uint64_t db_id, uint64_t index_id, const XidLsn &xid)
    {
        ThriftClient c = _get_client();
        IndexInfo result;

        GetIndexInfoRequest request;
        _set_request_common(request, db_id, xid);
        request.index_id = index_id;

        c.client->get_index_info(result, request);

        return result;
    }


    std::string 
    Client::drop_index(uint64_t db_id, const XidLsn &xid, const PgMsgDropIndex &msg)
    {
        ThriftClient c = _get_client();
        DDLStatement result;

        DropIndexRequest request;
        _set_request_common(request, db_id, xid);

        request.index_id = msg.oid;
        request.schema = msg.schema;

        c.client->drop_index(result, request);

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
        for (auto const& [index_id, extent_id]: metadata.roots) {
            sys_tbl_mgr::RootInfo ri;
            ri.index_id = index_id;
            ri.extent_id = extent_id;
            request.roots.push_back(ri);
        }

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

    TableMetadataPtr
    Client::get_roots(uint64_t db_id,
                      uint64_t table_id,
                      uint64_t xid)
    {
        return nullptr;
#if 0
        MetadataKey key{db_id, table_id, xid};

        auto entry = _roots_cache->get(key, [this](const MetadataKey &key) {
            GetRootsRequest request;
            request.db_id = key.db;
            request.table_id = key.tid;
            request.xid = key.xid;

            GetRootsResponse result;

            ThriftClient c = _get_client();
            c.client->get_roots(result, request);

            auto metadata = std::make_shared<TableMetadata>();
            for (const auto &root : result.roots) {
                metadata->roots.push_back([](const RootInfo &root) {
                    return TableRoot(root.index_id, root.extent_id);
                }(root));
            }
            metadata->stats.row_count = result.stats.row_count;
            metadata->snapshot_xid = result.snapshot_xid;

            return std::make_shared<MetadataValue>(result.snapshot_xid, metadata);
        });

        return entry->metadata;
#endif
    }

    SchemaMetadataPtr
    Client::get_schema(uint64_t db_id,
                       uint64_t table_id,
                       const XidLsn &xid)
    {
        SchemaCache::Key key{db_id, table_id, xid};

        auto metadata = _schema_cache->get(key, [this](const SchemaCache::Key &key) {
            ThriftClient c = _get_client();
            GetSchemaResponse result;

            GetSchemaRequest request;
            request.db_id = key.db;
            request.table_id = key.tid;
            request.xid = key.xid.xid;
            request.lsn = key.xid.lsn;

            c.client->get_schema(result, request);

            auto metadata = std::make_shared<SchemaMetadata>();
            for (const auto &col_entry : result.columns) {
                const auto &column = col_entry.second;
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

                metadata->columns.push_back(value);
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

                metadata->history.push_back(value);
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
                metadata->indexes.push_back(std::move(info));
            }

            XidLsn access_start(static_cast<uint64_t>(result.access_xid_start),
                                static_cast<uint64_t>(result.access_lsn_start));
            XidLsn access_end(static_cast<uint64_t>(result.access_xid_end),
                              static_cast<uint64_t>(result.access_lsn_end));
            XidLsn target_start(static_cast<uint64_t>(result.target_xid_start),
                                static_cast<uint64_t>(result.target_lsn_start));
            XidLsn target_end(static_cast<uint64_t>(result.target_xid_end),
                              static_cast<uint64_t>(result.target_lsn_end));

            metadata->access_range = XidRange(access_start, access_end);
            metadata->target_range = XidRange(access_start, access_end);

            return metadata;
        });

        return metadata;
    }

    SchemaMetadataPtr
    Client::get_target_schema(uint64_t db_id,
                              uint64_t table_id,
                              const XidLsn &access_xid,
                              const XidLsn &target_xid)
    {
        SchemaCache::Key access_key{db_id, table_id, access_xid};
        SchemaCache::Key target_key{db_id, table_id, target_xid};

        auto populate = [this](const SchemaCache::Key &access_key,
                               const SchemaCache::Key &target_key) {
            ThriftClient c = _get_client();
            GetSchemaResponse result;

            GetTargetSchemaRequest request;
            request.db_id = access_key.db;
            request.table_id = access_key.tid;
            request.access_xid = access_key.xid.xid;
            request.access_lsn = access_key.xid.lsn;
            request.target_xid = target_key.xid.xid;
            request.target_lsn = target_key.xid.lsn;

            c.client->get_target_schema(result, request);

            auto metadata = std::make_shared<SchemaMetadata>();
            for (const auto &col_entry : result.columns) {
                const auto &column = col_entry.second;
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

                metadata->columns.push_back(value);
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

                metadata->history.push_back(value);
            }

            XidLsn access_start(static_cast<uint64_t>(result.access_xid_start),
                                static_cast<uint64_t>(result.access_lsn_start));
            XidLsn access_end(static_cast<uint64_t>(result.access_xid_end),
                              static_cast<uint64_t>(result.access_lsn_end));
            XidLsn target_start(static_cast<uint64_t>(result.target_xid_start),
                                static_cast<uint64_t>(result.target_lsn_start));
            XidLsn target_end(static_cast<uint64_t>(result.target_xid_end),
                              static_cast<uint64_t>(result.target_lsn_end));

            metadata->access_range = XidRange(access_start, access_end);
            metadata->target_range = XidRange(access_start, access_end);

            return metadata;
        };

        return _schema_cache->get_range(access_key, target_key, populate);
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

    std::string
    Client::swap_sync_table(const TableRequest &create,
                            const UpdateRootsRequest &roots)
    {
        ThriftClient c = _get_client();
        DDLStatement result;

        c.client->swap_sync_table(result, create, roots);
        if (result.statement.empty()) {
            throw SysTblMgrError();
        }

        return result.statement;
    }

    void
    Client::invalidate_schema_cache(uint64_t db_id,
                                    uint64_t table_id,
                                    const XidLsn &xid)
    {
        _schema_cache->reinsert(SchemaCache::Key{ db_id, table_id, xid });
    }

} // namespace
