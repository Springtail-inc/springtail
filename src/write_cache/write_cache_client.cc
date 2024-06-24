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

#include <thrift/write_cache/ThriftWriteCache.h>

#include <write_cache/write_cache_client.hh>
#include <write_cache/write_cache_client_factory.hh>

namespace springtail {
    /* static initialization must happen outside of class */
    WriteCacheClient* WriteCacheClient::_instance {nullptr};
    std::mutex WriteCacheClient::_instance_mutex;

    WriteCacheClient *
    WriteCacheClient::get_instance()
    {
        std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance == nullptr) {
            _instance = new WriteCacheClient();
        }

        return _instance;
    }

    WriteCacheClient::WriteCacheClient()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json client_json;
        nlohmann::json server_json;

        // fetch properties for the write cache client
        if (!Json::get_to(json, "client", client_json)) {
            throw Error("Write cache client settings not found");
        }

        if (!Json::get_to(json, "server", server_json)) {
            throw Error("Write cache server settings not found");
        }

        // init channel pool
        int max_connections;
        int port;
        std::string server;
        Json::get_to<int>(client_json, "connections", max_connections, 8);
        Json::get_to<int>(server_json, "port", port, 55051);

        if (!Json::get_to<std::string>(client_json, "server", server)) {
            throw Error("Host not found in write_cache.server settings");
        }

        // construct the thrift client pool.
        // First argument is a factory object that constructs a thrift clients
        // using the host and port from above
        _thrift_client_pool = std::make_shared<ObjectPool<thrift::write_cache::ThriftWriteCacheClient>>(
            std::make_shared<WriteCacheThriftObjectFactory>(server, port),
            max_connections/2,
            max_connections
        );
    }

    void
    WriteCacheClient::shutdown()
    {
         std::scoped_lock<std::mutex> lock(_instance_mutex);

        if (_instance != nullptr) {
            delete _instance;
            _instance = nullptr;
        }
    }

    // exposed client service interface below

    void
    WriteCacheClient::ping()
    {
        ThriftClient c = _get_client();
        thrift::write_cache::Status result;

        c.client->ping(result);

        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    void
    WriteCacheClient::add_table_change(uint64_t tid, TableChange &change)
    {
        ThriftClient c = _get_client();
        thrift::write_cache::Status result;

        thrift::write_cache::TableChange request;
        request.table_id = tid;
        request.xid = change.xid;
        request.xid_seq = change.xid_seq;
        if (change.op == TableOp::TRUNCATE) {
            request.op = thrift::write_cache::TableChangeOpType::TRUNCATE_TABLE;
        } else if (change.op == TableOp::SCHEMA_CHANGE) {
            request.op = thrift::write_cache::TableChangeOpType::SCHEMA_CHANGE;
        }

        c.client->add_table_change(result, request);

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::add_rows(uint64_t tid, uint64_t eid, std::vector<RowData> &&rows)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::AddRowsRequest request;
        thrift::write_cache::Status result;

        request.table_id = tid;
        request.extent_id = eid;

        // marshall up rows
        for (auto &&r: rows) {
            thrift::write_cache::Row row;
            row.xid = r.xid;
            row.xid_seq = r.xid_seq;
            row.primary_key = std::move(r.pkey);

            if (r.op != RowOp::DELETE) {
                // update or insert
                // note: setting __isset directly to avoid data copy
                row.data = std::move(r.data);
                row.__isset.data = true;

                if (r.op == RowOp::INSERT) {
                    row.op = thrift::write_cache::RowOpType::INSERT;
                } else {
                    row.op = thrift::write_cache::RowOpType::UPDATE;
                }
            } else {
                row.op = thrift::write_cache::RowOpType::DELETE;
            }

            request.rows.push_back(std::move(row));
        }

        c.client->add_rows(result, request);

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    std::vector<WriteCacheClient::TableChange>
    WriteCacheClient::fetch_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::GetTableChangeRequest request;
        thrift::write_cache::GetTableChangeResponse response;

        request.table_id = tid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;

        c.client->get_table_changes(response, request);

        assert(response.table_id == tid);

        // take response changes and move them into the TableChange vector to be returned
        std::vector<TableChange> changes;
        for (const auto &chg: response.changes) {
            TableChange change;
            change.xid = chg.xid;
            change.xid_seq = chg.xid_seq;
            if (chg.op == thrift::write_cache::TableChangeOpType::TRUNCATE_TABLE) {
                change.op = TableOp::TRUNCATE;
            } else if (chg.op == thrift::write_cache::TableChangeOpType::SCHEMA_CHANGE) {
                change.op = TableOp::SCHEMA_CHANGE;
            }
            changes.push_back(std::move(change));
        }

        return changes;
    }

    std::vector<uint64_t>
    WriteCacheClient::list_tables(uint64_t start_xid, uint64_t end_xid, uint32_t count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ListTablesRequest request;
        thrift::write_cache::ListTablesResponse response;

        request.start_xid = start_xid;
        request.end_xid = end_xid;
        request.count = count;
        request.cursor = cursor;

        c.client->list_tables(response, request);

        cursor = response.cursor;

        return std::vector<uint64_t>(response.table_ids.begin(), response.table_ids.end());
    }

    std::vector<uint64_t>
    WriteCacheClient::list_extents(uint64_t tid, uint64_t start_xid, uint64_t end_xid,
                                   uint32_t count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();
        std::vector<uint64_t> extents;

        thrift::write_cache::ListExtentsRequest request;
        thrift::write_cache::ListExtentsResponse response;

        request.start_xid = start_xid;
        request.end_xid = end_xid;
        request.count = count;
        request.cursor = cursor;

        c.client->list_extents(response, request);

        assert(response.table_id == tid);

        cursor = response.cursor;

        return std::vector<uint64_t>(response.extent_ids.begin(), response.extent_ids.end());
    }

    std::vector<WriteCacheClient::RowData>
    WriteCacheClient::fetch_rows(uint64_t tid, uint64_t eid, uint64_t start_xid,
                                 uint64_t end_xid, uint32_t count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::GetRowsRequest request;
        thrift::write_cache::GetRowsResponse response;

        request.table_id = tid;
        request.extent_id = eid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;
        request.count = count;
        request.cursor = cursor;

        c.client->get_rows(response, request);

        cursor = request.cursor;

        std::vector<RowData> rows;
        for (const auto &r: response.rows) {
            RowData row;

            row.xid = r.xid;
            row.xid_seq = r.xid_seq;
            row.pkey = std::move(r.primary_key);
            row.data = std::move(r.data);
            if (r.op == thrift::write_cache::RowOpType::UPDATE) {
                row.op = RowOp::UPDATE;
            } else if (r.op == thrift::write_cache::RowOpType::INSERT) {
                row.op = RowOp::INSERT;
            } else if (r.op == thrift::write_cache::RowOpType::DELETE) {
                row.op = RowOp::DELETE;
            }

            rows.push_back(std::move(row));
        }

        return rows;
    }

    void
    WriteCacheClient::evict_table(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::EvictTableRequest request;
        thrift::write_cache::Status result;

        request.table_id = tid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;

        c.client->evict_table(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::evict_table_changes(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::EvictTableChangesRequest request;
        thrift::write_cache::Status result;

        request.table_id = tid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;

        c.client->evict_table_changes(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::set_clean_flag(uint64_t tid, uint64_t eid, uint64_t start_xid, uint64_t end_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::SetCleanFlagRequest request;
        thrift::write_cache::Status result;

        request.table_id = tid;
        request.extent_id = eid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;

        c.client->set_clean_flag(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::reset_clean_flag(uint64_t tid, uint64_t start_xid, uint64_t end_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ResetCleanFlagRequest request;
        thrift::write_cache::Status result;

        request.table_id = tid;
        request.start_xid = start_xid;
        request.end_xid = end_xid;

        c.client->reset_clean_flag(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }
} // namespace
