#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>

#include <common/object_pool.hh>
#include <pg_repl/pg_repl_msg.hh>
#include <storage/table.hh>
#include <storage/xid.hh>

#include <thrift/sys_tbl_mgr/ThriftSysTblMgr.h> // generated file

namespace springtail {

    class SysTblMgrClient
    {
    public:
        /**
         * @brief Get the singleton write cache client instance object
         * @return SysTblMgrClient *
         */
        static SysTblMgrClient *get_instance();

        /**
         * @brief Shutdown cache
         */
        static void shutdown();

        /**
         * @brief Ping the server
         */
        void ping();

        /**
         * Call create_table() on the SysTblMgr.
         */
        void create_table(const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call alter_table() on the SysTblMgr.
         */
        void alter_table(const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call drop_table() on the SysTblMgr.
         */
        void drop_table(const XidLsn &xid, const PgMsgDropTable &msg);

        /**
         * Call update_roots() on the SysTblMgr.
         */
        void update_roots(uint64_t table_id, uint64_t xid,
                          const std::vector<uint64_t> &roots, uint64_t row_count);

        /**
         * Call finalize() on the SysTblMgr.
         */
        void finalize(uint64_t xid);

        /**
         * Call get_roots() on the SysTblMgr.
         */
        TableMetadata get_roots(uint64_t table_id, uint64_t xid);

        /**
         * Call get_schema_info() on the SysTblMgr.
         */
        SchemaMetadata get_schema_info(uint64_t table_id, const XidLsn &xid);

        /**
         * Call get_schema_info_with_target() on the SysTblMgr.
         */
        SchemaMetadata get_schema_info_with_target(uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

    protected:
        /** Singleton write cache client instance */
        static SysTblMgrClient *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        /**
         * @brief Construct a new Write Cache Client object
         */
        SysTblMgrClient();

        /**
         * @brief Destroy the Write Cache Client object; shouldn't be called directly use shutdown()
         */
        ~SysTblMgrClient() {}

    private:
        // delete copy constructor
        SysTblMgrClient(const SysTblMgrClient &) = delete;
        void operator=(const SysTblMgrClient &)   = delete;

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<thrift::sys_tbl_mgr::ThriftSysTblMgrClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<thrift::sys_tbl_mgr::ThriftSysTblMgrClient>> pool;
            std::shared_ptr<thrift::sys_tbl_mgr::ThriftSysTblMgrClient> client;
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
            std::shared_ptr<thrift::sys_tbl_mgr::ThriftSysTblMgrClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

    };

} // namespace springtail
