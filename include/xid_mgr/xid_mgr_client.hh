#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

// #include <common/object_pool.hh>
#include <common/singleton.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h> // generated file
#include <thrift/common/thrift_client.hh>

namespace springtail {

    class XidMgrClient :
        public thrift::Client<XidMgrClient, thrift::xid_mgr::ThriftXidMgrClient>,
        public Singleton<XidMgrClient>
    {
        friend class Singleton<XidMgrClient>;
    public:
        // RPC interfaces below

        /**
         * @brief Ping the server
         */
        void ping();

        /**
         * @brief Commit xid, mark it as latest
         * @param db_id database id
         * @param xid xid to commit
         * @param has_schema_change whether the xid has schema changes related to it or not
         */
        void commit_xid(uint64_t db_id, uint64_t xid, bool has_schema_change);

        /**
         * @brief Record DDL change.  Used for handling table sync.
         * @param db_id database id
         * @param xid xid to commit
         */
        void record_ddl_change(uint64_t db_id, uint64_t xid);

        /**
         * @brief Get the latest committed xid
         * @param db_id database id
         * @param schema_xid last known schema xid
         * @return uint64_t latest committed xid
         */
        uint64_t get_committed_xid(uint64_t db_id, uint64_t schema_xid);
    private:
        XidMgrClient();
        ~XidMgrClient() override = default;
    };

} // namespace springtail
