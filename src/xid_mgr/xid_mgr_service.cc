#include <thrift/xid_mgr/ThriftXidMgr.h>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail {

    void
    ThriftXidMgrService::ping(thrift::xid_mgr::Status& _return)
    {
        xid_mgr::XidMgrServer::call_wrapper([&_return]() {
            _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
            _return.__set_message("PONG");

            std::cout << "Got ping\n";
        });
    }

    void
    ThriftXidMgrService::commit_xid(thrift::xid_mgr::Status& _return,
                                    const int64_t db_id,
                                    const thrift::xid_mgr::xid_t xid,
                                    bool has_schema_changes)
    {
        xid_mgr::XidMgrServer::call_wrapper([&_return, db_id, xid, has_schema_changes]() {
            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
            server->commit_xid(db_id, xid, has_schema_changes);
            _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
        });
    }

    void
    ThriftXidMgrService::record_ddl_change(thrift::xid_mgr::Status& _return,
                                           const int64_t db_id,
                                           const thrift::xid_mgr::xid_t xid)
    {
        xid_mgr::XidMgrServer::call_wrapper([&_return, db_id, xid]() {
            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
            server->record_ddl_change(db_id, xid);
            _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
        });
    }

    thrift::xid_mgr::xid_t
    ThriftXidMgrService::get_committed_xid(const int64_t db_id,
                                           const thrift::xid_mgr::xid_t schema_xid)
    {
        uint64_t xid = 0;
        xid_mgr::XidMgrServer::call_wrapper([db_id, schema_xid, &xid]() {
            xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
            xid = server->get_committed_xid(db_id, schema_xid);
        });
        return xid;
    }
}
