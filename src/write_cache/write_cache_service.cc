
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
    ThriftWriteCacheService::add_rows(thrift::write_cache::Status& _return,
                                      const thrift::write_cache::AddRowsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        std::vector<WriteCacheIndexRowPtr> rows;

        for (const thrift::write_cache::Row &r: request.rows) {
            WriteCacheIndexRowPtr row;
            if (r.op == thrift::write_cache::RowOpType::DELETE) {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.primary_key), request.extent_id, r.xid, r.xid_seq);
            } else {
                row = std::make_shared<WriteCacheIndexRow>(std::move(r.data), std::move(r.primary_key), request.extent_id, r.xid, r.xid_seq,
                    ((r.op == thrift::write_cache::RowOpType::UPDATE)
                     ? WriteCacheIndexRow::RowOp::UPDATE
                     : WriteCacheIndexRow::RowOp::INSERT));
            }
            rows.push_back(row);
        }

        index->add_rows(request.table_id, request.extent_id, rows);

        _return.status = thrift::write_cache::StatusCode::SUCCESS;
    }

    void
    ThriftWriteCacheService::list_extents(thrift::write_cache::ListExtentsResponse& _return,
                                          const thrift::write_cache::ListExtentsRequest& request)
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
    ThriftWriteCacheService::get_rows(thrift::write_cache::GetRowsResponse& _return,
                                      const thrift::write_cache::GetRowsRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        uint64_t cursor = request.cursor;
        std::vector<WriteCacheIndexRowPtr> rows =
            index->get_rows(request.table_id, request.extent_id, request.start_xid, request.end_xid,
                            request.count, cursor);

        _return.cursor = cursor;
        _return.extent_id = request.extent_id;
        _return.table_id = request.table_id;

        for (const auto &r: rows) {
            thrift::write_cache::Row row;
            row.xid = r->xid;
            row.xid_seq = r->xid_seq;
            row.primary_key = r->pkey;

            if (r->op == WriteCacheIndexRow::RowOp::UPDATE) {
                row.op = thrift::write_cache::RowOpType::UPDATE;
                row.__set_data(r->data);
            } else if (r->op == WriteCacheIndexRow::RowOp::INSERT) {
                row.op = thrift::write_cache::RowOpType::INSERT;
                row.__set_data(r->data);
            } else if (r->op == WriteCacheIndexRow::RowOp::DELETE) {
                row.op = thrift::write_cache::RowOpType::DELETE;
            }

            _return.rows.push_back(row);
        }
    }

    void
    ThriftWriteCacheService::evict_table(thrift::write_cache::Status& _return,
                                         const thrift::write_cache::EvictTableRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->evict_table(request.table_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::add_table_change(thrift::write_cache::Status& _return, const thrift::write_cache::TableChange &change)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        WriteCacheIndexTableChange::TableChangeOp op;
        if (change.op == thrift::write_cache::TableChangeOpType::SCHEMA_CHANGE) {
            op = WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE;
        } else if (change.op == thrift::write_cache::TableChangeOpType::TRUNCATE_TABLE) {
            op = WriteCacheIndexTableChange::TableChangeOp::TRUNCATE_TABLE;
        }

        index->add_table_change(std::make_shared<WriteCacheIndexTableChange>(change.table_id, change.xid, change.xid_seq, op));
    }

    void
    ThriftWriteCacheService::get_table_changes(thrift::write_cache::GetTableChangeResponse& _return, const thrift::write_cache::GetTableChangeRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        std::vector<WriteCacheIndexTableChangePtr> changes = index->get_table_changes(request.table_id, request.start_xid, request.end_xid);

        _return.table_id = request.table_id;

        for (auto c: changes) {
            thrift::write_cache::TableChange change;
            change.table_id = c->tid;
            change.xid = c->xid;
            change.xid_seq = c->xid_seq;

            if (c->op == WriteCacheIndexTableChange::TableChangeOp::SCHEMA_CHANGE) {
                change.op = thrift::write_cache::TableChangeOpType::SCHEMA_CHANGE;
            } else if (c->op == WriteCacheIndexTableChange::TableChangeOp::TRUNCATE_TABLE) {
                change.op = thrift::write_cache::TableChangeOpType::TRUNCATE_TABLE;
            }

            _return.changes.push_back(change);
        }
    }

    void
    ThriftWriteCacheService::list_tables(thrift::write_cache::ListTablesResponse& _return,
                                         const thrift::write_cache::ListTablesRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        uint64_t cursor = request.cursor;

        _return.table_ids = index->get_tids(request.start_xid, request.end_xid, request.count, cursor);
        _return.cursor = cursor;
    }

    void
    ThriftWriteCacheService::evict_table_changes(thrift::write_cache::Status& _return,
                                                 const thrift::write_cache::EvictTableChangesRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->evict_table_changes(request.table_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::set_clean_flag(thrift::write_cache::Status& _return,
                                           const thrift::write_cache::SetCleanFlagRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->set_clean_flag(request.table_id, request.extent_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::reset_clean_flag(thrift::write_cache::Status& _return,
                                              const thrift::write_cache::ResetCleanFlagRequest& request)
    {
        WriteCacheServer *server = WriteCacheServer::get_instance();
        std::shared_ptr<WriteCacheIndex> index = server->get_index();

        index->reset_clean_flag(request.table_id, request.start_xid, request.end_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::add_mapping(thrift::write_cache::Status &_return,
                                         const thrift::write_cache::AddMappingRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance();

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
        ExtentMapper *mapper = ExtentMapper::get_instance();
        mapper->set_lookup(request.table_id, request.target_xid, request.extent_id);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }

    void
    ThriftWriteCacheService::forward_map(thrift::write_cache::ExtentMapResponse &_return,
                                         const thrift::write_cache::ForwardMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance();
        auto &&response = mapper->forward_map(request.table_id,
                                              request.target_xid, request.extent_id);

        _return.extent_ids.insert(_return.extent_ids.end(), response.begin(), response.end());
    }

    void
    ThriftWriteCacheService::reverse_map(thrift::write_cache::ExtentMapResponse &_return,
                                         const thrift::write_cache::ReverseMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance();
        auto &&response = mapper->reverse_map(request.table_id, request.access_xid,
                                              request.target_xid, request.extent_id);

        _return.extent_ids.insert(_return.extent_ids.end(), response.begin(), response.end());
    }

    void
    ThriftWriteCacheService::expire_map(thrift::write_cache::Status &_return,
                                        const thrift::write_cache::ExpireMapRequest &request)
    {
        ExtentMapper *mapper = ExtentMapper::get_instance();
        mapper->expire(request.table_id, request.commit_xid);

        _return.__set_status(thrift::write_cache::StatusCode::SUCCESS);
    }
}
