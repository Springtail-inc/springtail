
#include <write_cache/write_cache_service.hh>
#include <write_cache/write_cache_server.hh>

namespace springtail {

    void
    ThriftWriteCacheService::ping(thrift::Status& _return)
    {
        _return.__set_status(thrift::StatusCode::SUCCESS);
        _return.__set_message("PONG");

        std::cout << "Got ping\n";
    }

    void
    ThriftWriteCacheService::add_rows(thrift::Status& _return,
                                      const thrift::AddRowRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        for (thrift::Row r: request.rows) {
            std::shared_ptr<WriteCacheIndexRow> row;
            if (r.delete_flag) {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.primary_key), r.xid, r.xid_seq, r.delete_flag);
            } else {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.data), std::move(r.primary_key), r.xid, r.xid_seq, r.delete_flag);
            }

            index->add_row(request.table_id, request.extent_id, row);
        }

        _return.status = thrift::StatusCode::SUCCESS;
    }

    void
    ThriftWriteCacheService::list_extents(thrift::ListExtentsResponse& _return,
                                          const thrift::ListExtentsRequest& request)
    {

    }

    void
    ThriftWriteCacheService::get_rows(thrift::GetRowsResponse& _return,
                                      const thrift::GetRowsRequest& request)
    {

    }

    void
    ThriftWriteCacheService::evict_extent(thrift::Status& _return,
                                          const thrift::EvictExtentRequest& request)
    {

    }

    void
    ThriftWriteCacheService::add_table_changes(thrift::Status& _return, const std::vector<thrift::TableChange> & changes)
    {

    }

    void
    ThriftWriteCacheService::get_table_changes(thrift::GetTableChangeResponse& _return, const thrift::GetTableChangeRequest& request)
    {

    }

    void
    ThriftWriteCacheService::list_tables(thrift::ListTablesResponse& _return, const thrift::ListTablesRequest& request)
    {

    }

}