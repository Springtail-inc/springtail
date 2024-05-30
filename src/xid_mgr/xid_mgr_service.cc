
#include <thrift/xid_mgr/ThriftXidMgr.h>

#include <xid_mgr/xid_mgr_service.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail {

    void
    ThriftXidMgrService::ping(thrift::xid_mgr::Status& _return)
    {
        _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
        _return.__set_message("PONG");

        std::cout << "Got ping\n";
    }

    void
    ThriftXidMgrService::commit_xid(thrift::xid_mgr::Status& _return, const thrift::xid_mgr::xid_t request)
    {
        XidMgrServer *server = XidMgrServer::get_instance();
        server->commit_xid(request);
        _return.__set_status(thrift::xid_mgr::StatusCode::SUCCESS);
    }

    thrift::xid_mgr::xid_t
    ThriftXidMgrService::get_committed_xid()
    {
        XidMgrServer *server = XidMgrServer::get_instance();
        uint64_t xid = server->get_committed_xid();
        return xid;
    }
}
