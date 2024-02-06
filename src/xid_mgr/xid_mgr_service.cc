
#include "ThriftXidMgr.h"

#include <xid_mgr/xid_mgr_service.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail {

    void
    ThriftXidMgrService::ping(thrift::Status& _return)
    {
        _return.__set_status(thrift::StatusCode::SUCCESS);
        _return.__set_message("PONG");

        std::cout << "Got ping\n";
    }

    void
    ThriftXidMgrService::get_xid_range(thrift::XidRange& _return)
    {
        XidMgrServer *server = XidMgrServer::get_instance();
        std::pair<uint64_t, uint64_t> range = server->get_xid_range();
        _return.start_xid = range.first;
        _return.end_xid = range.second;
    }

    void
    ThriftXidMgrService::commit_xid(thrift::Status& _return, const thrift::xid_t request)
    {
        XidMgrServer *server = XidMgrServer::get_instance();
        server->commit_xid(request);
        _return.__set_status(thrift::StatusCode::SUCCESS);
    }

    thrift::xid_t
    ThriftXidMgrService::get_latest_committed_xid()
    {
        XidMgrServer *server = XidMgrServer::get_instance();
        uint64_t xid = server->get_latest_committed_xid();
        return xid;
    }
}