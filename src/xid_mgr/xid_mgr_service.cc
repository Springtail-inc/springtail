
#include <thrift/xid_mgr/ThriftXidMgr.h>

#include <xid_mgr/xid_mgr_service.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail {

    ThriftXidMgrService *ThriftXidMgrService::_instance = nullptr;
    std::once_flag ThriftXidMgrService::_init_flag;
    std::once_flag ThriftXidMgrService::_shutdown_flag;

    ThriftXidMgrService *
    ThriftXidMgrService::_init() {
        if (!_instance) {
            _instance = new ThriftXidMgrService();
        }
        return _instance;
    }

    void
    ThriftXidMgrService::_shutdown() {
        if (_instance) {
            delete _instance;
            _instance = nullptr;
        }
    }

    void
    ThriftXidMgrService::ping(thrift::xid_mgr::Status& _return)
    {
        _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
        _return.__set_message("PONG");

        std::cout << "Got ping\n";
    }

    void
    ThriftXidMgrService::commit_xid(thrift::xid_mgr::Status& _return,
                                    const int64_t db_id,
                                    const thrift::xid_mgr::xid_t xid,
                                    bool has_schema_changes)
    {
        xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
        server->commit_xid(db_id, xid, has_schema_changes);
        _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
    }

    void
    ThriftXidMgrService::record_ddl_change(thrift::xid_mgr::Status& _return,
                                           const int64_t db_id,
                                           const thrift::xid_mgr::xid_t xid)
    {
        xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
        server->record_ddl_change(db_id, xid);
        _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
    }

    thrift::xid_mgr::xid_t
    ThriftXidMgrService::get_committed_xid(const int64_t db_id,
                                           const thrift::xid_mgr::xid_t schema_xid)
    {
        xid_mgr::XidMgrServer *server = xid_mgr::XidMgrServer::get_instance();
        uint64_t xid = server->get_committed_xid(db_id, schema_xid);
        return xid;
    }
}
