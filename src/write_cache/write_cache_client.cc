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
        int max_connections = Json::get_or<int>(client_json, "connections", 8);
        int port = Json::get_or<int>(server_json, "port", 55051);

        std::string server = Properties::get_write_cache_hostname();

        // construct the thrift client pool.
        // First argument is a factory object that constructs a thrift clients
        // using the host and port from above
        _thrift_client_pool = std::make_shared<ObjectPool<thrift::write_cache::ThriftWriteCacheClient>>(
            std::make_shared<WriteCacheThriftObjectFactory>(server, port),
            max_connections/2,
            max_connections,
            ObjectPool<thrift::write_cache::ThriftWriteCacheClient>::LIFO
        );
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

    std::vector<uint64_t>
    WriteCacheClient::list_tables(uint64_t db_id, uint64_t xid, uint32_t count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ListTablesRequest request;
        thrift::write_cache::ListTablesResponse response;

        request.db_id = db_id;
        request.xid = xid;
        request.count = count;
        request.cursor = cursor;

        c.client->list_tables(response, request);

        cursor = response.cursor;

        return std::vector<uint64_t>(response.table_ids.begin(), response.table_ids.end());
    }

    std::vector<WriteCacheClient::WriteCacheExtent>
    WriteCacheClient::get_extents(uint64_t db_id, uint64_t tid, uint64_t xid,
                                  uint32_t count, uint64_t &cursor)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::GetExtentsRequest request;
        thrift::write_cache::GetExtentsResponse response;

        request.db_id = db_id;
        request.table_id = tid;
        request.xid = xid;
        request.count = count;
        request.cursor = cursor;

        c.client->get_extents(response, request);

        assert(response.table_id == tid);
        cursor = response.cursor;

        std::vector<WriteCacheExtent> extents;
        for (const auto &e: response.extents) {
            WriteCacheExtent extent;

            extent.xid = e.xid;
            extent.lsn = e.xid_seq;
            extent.data = std::move(e.data);

            extents.push_back(std::move(extent));
        }

        return extents;
    }

    void
    WriteCacheClient::evict_table(uint64_t db_id, uint64_t tid, uint64_t xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::EvictTableRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.xid = xid;

        c.client->evict_table(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::evict_xid(uint64_t db_id, uint64_t xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::EvictXidRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.xid = xid;

        c.client->evict_xid(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::add_mapping(uint64_t db_id, uint64_t tid,
                                  uint64_t target_xid,
                                  uint64_t old_eid,
                                  const std::vector<uint64_t> &new_eids)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::AddMappingRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.old_eid = old_eid;

        // note: performs a copy due to differing types
        std::copy(new_eids.begin(), new_eids.end(), std::back_inserter(request.new_eids));

        c.client->add_mapping(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

    void
    WriteCacheClient::set_lookup(uint64_t db_id, uint64_t tid,
                                 uint64_t target_xid,
                                 uint64_t extent_id)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::SetLookupRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        c.client->set_lookup(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

    std::vector<uint64_t>
    WriteCacheClient::forward_map(uint64_t db_id, uint64_t tid,
                                  uint64_t target_xid,
                                  uint64_t extent_id)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ForwardMapRequest request;
        thrift::write_cache::ExtentMapResponse result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        c.client->forward_map(result, request);

        std::vector<uint64_t> extent_ids;
        extent_ids.insert(extent_ids.end(), result.extent_ids.begin(), result.extent_ids.end());
        return extent_ids;
    }

    std::vector<uint64_t>
    WriteCacheClient::reverse_map(uint64_t db_id, uint64_t tid,
                                  uint64_t access_xid,
                                  uint64_t target_xid,
                                  uint64_t extent_id)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ReverseMapRequest request;
        thrift::write_cache::ExtentMapResponse result;

        request.db_id = db_id;
        request.table_id = tid;
        request.access_xid = access_xid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        c.client->reverse_map(result, request);

        std::vector<uint64_t> extent_ids;
        extent_ids.insert(extent_ids.end(), result.extent_ids.begin(), result.extent_ids.end());
        return extent_ids;
    }

    void
    WriteCacheClient::expire_map(uint64_t db_id, uint64_t tid,
                                 uint64_t commit_xid)
    {
        ThriftClient c = _get_client();

        thrift::write_cache::ExpireMapRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.commit_xid = commit_xid;

        c.client->expire_map(result, request);
        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

} // namespace
