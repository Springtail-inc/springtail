
#include <write_cache/write_cache_service.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

#include <write_cache/extent_mapper.hh>

namespace springtail {

    void
    ThriftWriteCacheService::ping(thrift::write_cache::Status& _return)
    {
        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
        _return.__set_message("PONG");

        std::cout << "Got ping\n";
    }

    void
    ThriftWriteCacheService::get_extents(thrift::write_cache::GetExtentsResponse& _return, const thrift::write_cache::GetExtentsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        WriteCacheIndexPtr index = server->get_index(request.db_id);

        uint64_t cursor = request.cursor;

        std::vector<WriteCacheIndexExtentPtr> extents =
            index->get_extents(request.table_id, request.xid,
                               request.count, cursor);

        for (const auto &e: extents) {
            thrift::write_cache::Extent extent;
            extent.xid = e->xid;
            extent.xid_seq = e->xid_seq;

            // serialze the extent data
            extent.__set_data(e->data->serialize());

            _return.extents.push_back(std::move(extent));
        }

        _return.cursor = cursor;
        _return.table_id = request.table_id;
    }

    void
    ThriftWriteCacheService::evict_table(thrift::write_cache::Status& _return,
                                         const thrift::write_cache::EvictTableRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        WriteCacheIndexPtr index = server->get_index(request.db_id);

        index->evict_table(request.table_id, request.xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::evict_xid(thrift::write_cache::Status& _return,
                                       const thrift::write_cache::EvictXidRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        WriteCacheIndexPtr index = server->get_index(request.db_id);

        index->evict_xid(request.xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::list_tables(thrift::write_cache::ListTablesResponse& _return,
                                         const thrift::write_cache::ListTablesRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        WriteCacheIndexPtr index = server->get_index(request.db_id);

        uint64_t cursor = request.cursor;

        auto &&tids = index->get_tids(request.xid, request.count, cursor);
        for (auto tid: tids) {
            _return.table_ids.push_back(tid);
        }

        _return.cursor = cursor;
    }

    //// EXTENT MAPPER API

    void
    ThriftWriteCacheService::add_mapping(thrift::write_cache::Status &_return,
                                         const thrift::write_cache::AddMappingRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance(request.db_id);

        // note: unfortunately need to copy the data to shift to uin64_t type
        std::vector<uint64_t> new_eids(request.new_eids.begin(), request.new_eids.end());
        mapper->add_mapping(request.table_id, request.target_xid,
                            request.old_eid, new_eids);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::set_lookup(thrift::write_cache::Status &_return,
                                        const thrift::write_cache::SetLookupRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance(request.db_id);
        mapper->set_lookup(request.table_id, request.target_xid, request.extent_id);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::forward_map(thrift::write_cache::ExtentMapResponse &_return,
                                         const thrift::write_cache::ForwardMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance(request.db_id);
        auto &&response = mapper->forward_map(request.table_id,
                                              request.target_xid, request.extent_id);

        _return.extent_ids.insert(_return.extent_ids.end(), response.begin(), response.end());
    }

    void
    ThriftWriteCacheService::reverse_map(thrift::write_cache::ExtentMapResponse &_return,
                                         const thrift::write_cache::ReverseMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance(request.db_id);
        auto &&response = mapper->reverse_map(request.table_id, request.access_xid,
                                              request.target_xid, request.extent_id);

        _return.extent_ids.insert(_return.extent_ids.end(), response.begin(), response.end());
    }

    void
    ThriftWriteCacheService::expire_map(thrift::write_cache::Status &_return,
                                        const thrift::write_cache::ExpireMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance(request.db_id);
        mapper->expire(request.table_id, request.commit_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }
}
