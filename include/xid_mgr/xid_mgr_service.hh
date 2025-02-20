#pragma once

#include <thrift/transport/TSocket.h>

#include <common/singleton.hh>
#include <common/logging.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h>

namespace springtail {

    /**
     * @brief This is the implementation of the ThriftXidMgrIf that is generated
     *        from the .thrift file.  It contains the service (handler) for actually
     *        implementing the remote procedure calls.
     */
    class ThriftXidMgrService final : public thrift::xid_mgr::ThriftXidMgrIf, public Singleton<ThriftXidMgrService>
    {
        friend class Singleton<ThriftXidMgrService>;
    public:

        void ping(thrift::xid_mgr::Status& _return) override;
        void commit_xid(thrift::xid_mgr::Status& _return, const int64_t db_id, const thrift::xid_mgr::xid_t xid, bool has_schema_changes) override;
        void record_ddl_change(thrift::xid_mgr::Status& _return, const int64_t db_id, const thrift::xid_mgr::xid_t xid) override;
        thrift::xid_mgr::xid_t get_committed_xid(const int64_t db_id, thrift::xid_mgr::xid_t schema_xid) override;

    private:
        ThriftXidMgrService() = default;
        ~ThriftXidMgrService() = default;
    };
}
