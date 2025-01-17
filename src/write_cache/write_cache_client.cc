#include <string>
#include <string_view>
#include <memory>
#include <cassert>

#include <absl/log/check.h>
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

namespace springtail {
    WriteCacheClient::WriteCacheClient()
    {
        nlohmann::json json = Properties::get(Properties::WRITE_CACHE_CONFIG);
        nlohmann::json rpc_json;

        // fetch RPC properties for the write cache client
        if (!Json::get_to(json, "rpc_config", rpc_json)) {
            throw Error("Write cache RPC settings are not found");
        }

        std::string server = Properties::get_write_cache_hostname();

        init(server, rpc_json);
    }

    // exposed client service interface below

    void
    WriteCacheClient::ping()
    {
        thrift::write_cache::Status result;

        _invoke_with_retries([&result](ThriftClient &c) {
            c.client->ping(result);
        });

        std::cout << "Ping got: " << result.message << std::endl;
        return;
    }

    std::vector<uint64_t>
    WriteCacheClient::list_tables(uint64_t db_id, uint64_t xid, uint32_t count, uint64_t &cursor)
    {
        thrift::write_cache::ListTablesRequest request;
        thrift::write_cache::ListTablesResponse response;

        request.db_id = db_id;
        request.xid = xid;
        request.count = count;
        request.cursor = cursor;

        _invoke_with_retries([&response, &request](ThriftClient &c) {
            c.client->list_tables(response, request);
        });

        cursor = response.cursor;

        return std::vector<uint64_t>(response.table_ids.begin(), response.table_ids.end());
    }

    std::vector<WriteCacheClient::WriteCacheExtent>
    WriteCacheClient::get_extents(uint64_t db_id, uint64_t tid, uint64_t xid,
                                  uint32_t count, uint64_t &cursor)
    {
        thrift::write_cache::GetExtentsRequest request;
        thrift::write_cache::GetExtentsResponse response;

        request.db_id = db_id;
        request.table_id = tid;
        request.xid = xid;
        request.count = count;
        request.cursor = cursor;

        _invoke_with_retries([&response, &request](ThriftClient &c) {
            c.client->get_extents(response, request);
        });

        CHECK_EQ(response.table_id, tid);
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
        thrift::write_cache::EvictTableRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.xid = xid;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->evict_table(result, request);
        });

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }

        return;
    }

    void
    WriteCacheClient::evict_xid(uint64_t db_id, uint64_t xid)
    {
        thrift::write_cache::EvictXidRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.xid = xid;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->evict_xid(result, request);
        });

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
        thrift::write_cache::AddMappingRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.old_eid = old_eid;

        // note: performs a copy due to differing types
        std::copy(new_eids.begin(), new_eids.end(), std::back_inserter(request.new_eids));

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->add_mapping(result, request);
        });

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

    void
    WriteCacheClient::set_lookup(uint64_t db_id, uint64_t tid,
                                 uint64_t target_xid,
                                 uint64_t extent_id)
    {
        thrift::write_cache::SetLookupRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->set_lookup(result, request);
        });

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

    std::vector<uint64_t>
    WriteCacheClient::forward_map(uint64_t db_id, uint64_t tid,
                                  uint64_t target_xid,
                                  uint64_t extent_id)
    {
        thrift::write_cache::ForwardMapRequest request;
        thrift::write_cache::ExtentMapResponse result;

        request.db_id = db_id;
        request.table_id = tid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->forward_map(result, request);
        });

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
        thrift::write_cache::ReverseMapRequest request;
        thrift::write_cache::ExtentMapResponse result;

        request.db_id = db_id;
        request.table_id = tid;
        request.access_xid = access_xid;
        request.target_xid = target_xid;
        request.extent_id = extent_id;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->reverse_map(result, request);
        });

        std::vector<uint64_t> extent_ids;
        extent_ids.insert(extent_ids.end(), result.extent_ids.begin(), result.extent_ids.end());
        return extent_ids;
    }

    void
    WriteCacheClient::expire_map(uint64_t db_id, uint64_t tid,
                                 uint64_t commit_xid)
    {
        thrift::write_cache::ExpireMapRequest request;
        thrift::write_cache::Status result;

        request.db_id = db_id;
        request.table_id = tid;
        request.commit_xid = commit_xid;

        _invoke_with_retries([&result, &request](ThriftClient &c) {
            c.client->expire_map(result, request);
        });

        if (result.status != thrift::write_cache::StatusCode::SUCCESS) {
            throw Error("RPC failed");
        }
    }

} // namespace
