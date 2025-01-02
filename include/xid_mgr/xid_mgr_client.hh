#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <common/object_pool.hh>

#include <thrift/xid_mgr/ThriftXidMgr.h> // generated file

namespace springtail {

    class XidMgrClient
    {
    public:
        // delete copy constructor
        XidMgrClient(const XidMgrClient &) = delete;
        void operator=(const XidMgrClient &) = delete;
        void operator=(const XidMgrClient &&) = delete;

        static XidMgrClient *get_instance() {
            std::call_once(_init_flag, &XidMgrClient::init);
            return _instance;
        }

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

        /** Initializer for singleton, called once */
        static void init();

        /** singleton instance */
        static XidMgrClient *_instance;

        /** once flag for once initialization */
        static std::once_flag _init_flag;

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<thrift::xid_mgr::ThriftXidMgrClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<thrift::xid_mgr::ThriftXidMgrClient>> pool;
            std::shared_ptr<thrift::xid_mgr::ThriftXidMgrClient> client;
            ~ThriftClient() {
                pool->put(client);
            }
        };

        /**
         * @brief Helper function to fetch a thrift client from the object pool wrapped in
         *        a struct to ensure its proper release to the pool
         */
        inline ThriftClient _get_client()
        {
            std::shared_ptr<thrift::xid_mgr::ThriftXidMgrClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

        void _reconnect_client(ThriftClient &c);
    };
} // namespace springtail
