
#include <write_cache/write_cache_service.hh>
#include <write_cache/write_cache_server.hh>
#include <write_cache/write_cache_index.hh>
#include <write_cache/write_cache_table_set.hh>

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
                                     const thrift::AddRowsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        std::vector<WriteCacheIndexRowPtr> rows;

        for (const thrift::Row &r: request.rows) {
            WriteCacheIndexRowPtr row;
            if (r.delete_flag) {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.primary_key), r.xid, r.xid_seq, r.delete_flag);
            } else {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.data), std::move(r.primary_key), r.xid, r.xid_seq, r.delete_flag);
            }
            rows.push_back(row);
        }

        index->add_rows(request.table_id, request.extent_id, rows);

        _return.status = thrift::StatusCode::SUCCESS;
    }

    void
    ThriftWriteCacheService::list_extents(thrift::ListExtentsResponse& _return,
                                          const thrift::ListExtentsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        uint64_t cursor = request.cursor;
        _return.extent_ids = index->get_eids(request.table_id, request.start_xid, request.end_xid,
                                             request.count, cursor);

        _return.cursor = cursor;
        _return.table_id = request.table_id;
    }

    void
    ThriftWriteCacheService::get_rows(thrift::GetRowsResponse& _return,
                                      const thrift::GetRowsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        uint64_t cursor = request.cursor;
        std::vector<WriteCacheIndexRowPtr> rows =
            index->get_rows(request.table_id, request.extent_id, request.start_xid, request.end_xid, request.count);

        _return.cursor = cursor;
        _return.extent_id = request.extent_id;
        _return.table_id = request.table_id;

        for (const auto &r: rows) {
            thrift::Row row;
            row.xid = r->xid;
            row.xid_seq = r->xid_seq;
            row.delete_flag = r->delete_flag;
            row.primary_key = r->pkey;

            if (!r->delete_flag) {
                row.__set_data(r->data);
            }

            _return.rows.push_back(row);
        }
    }

    void
    ThriftWriteCacheService::evict_table(thrift::Status& _return,
                                         const thrift::EvictTableRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->evict_table(request.table_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::add_table_change(thrift::Status& _return, const thrift::TableChange &change)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        WriteCacheIndexTableChange::TableChangeOp op;
        if (change.op == thrift::TableChangeOpType::SCHEMA_CHANGE) {
            op = WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE;
        } else if (change.op == thrift::TableChangeOpType::TRUNCATE_TABLE) {
            op = WriteCacheIndexTableChange::TableChangeOp::TRUNCATE_TABLE;
        }

        index->add_table_change(std::make_shared<WriteCacheIndexTableChange>(change.table_id, change.xid, change.xid_seq, op));
    }

    void
    ThriftWriteCacheService::get_table_changes(thrift::GetTableChangeResponse& _return, const thrift::GetTableChangeRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        std::vector<WriteCacheIndexTableChangePtr> changes = index->get_table_changes(request.table_id, request.start_xid, request.end_xid);

        _return.table_id = request.table_id;

        for (auto c: changes) {
            thrift::TableChange change;
            change.table_id = c->tid;
            change.xid = c->xid;
            change.xid_seq = c->xid_seq;

            if (c->op == WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE) {
                change.op = thrift::TableChangeOpType::SCHEMA_CHANGE;
            } else if (c->op == WriteCacheIndexTableChange::TableChangeOp::TRUNCATE_TABLE) {
                change.op = thrift::TableChangeOpType::TRUNCATE_TABLE;
            }

            _return.changes.push_back(change);
        }
    }

    void
    ThriftWriteCacheService::list_tables(thrift::ListTablesResponse& _return, const thrift::ListTablesRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        _return.table_ids = index->get_tids(request.start_xid, request.end_xid, request.count);
    }

    void
    ThriftWriteCacheService::evict_table_changes(thrift::Status& _return,
                                                 const thrift::EvictTableChangesRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->evict_table_changes(request.table_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::StatusCode::SUCCESS);
    }
}