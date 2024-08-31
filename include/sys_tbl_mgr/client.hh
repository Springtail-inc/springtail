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

#include <thrift/sys_tbl_mgr/Service.h> // generated file

namespace springtail::sys_tbl_mgr {

    class Client
    {
    public:
        /**
         * @brief Get the singleton write cache client instance object
         * @return Client *
         */
        static Client *get_instance();

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
        std::string create_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call alter_table() on the SysTblMgr.
         */
        std::string alter_table(uint64_t db_id, const XidLsn &xid, const PgMsgTable &msg);

        /**
         * Call drop_table() on the SysTblMgr.
         */
        std::string drop_table(uint64_t db_id, const XidLsn &xid, const PgMsgDropTable &msg);

        /**
         * Call update_roots() on the SysTblMgr.
         */
        void update_roots(uint64_t db_id, uint64_t table_id, uint64_t xid,
                          const std::vector<uint64_t> &roots, uint64_t row_count);

        /**
         * Call finalize() on the SysTblMgr.
         */
        void finalize(uint64_t db_id, uint64_t xid);

        /**
         * Call get_roots() on the SysTblMgr.
         */
        TableMetadata get_roots(uint64_t db_id, uint64_t table_id, uint64_t xid);

        /**
         * Call get_schema() on the SysTblMgr.
         */
        SchemaMetadata get_schema(uint64_t db_id, uint64_t table_id, const XidLsn &xid);

        /**
         * Call get_target_schema() on the SysTblMgr.
         */
        SchemaMetadata get_target_schema(uint64_t db_id, uint64_t table_id, const XidLsn &access_xid, const XidLsn &target_xid);

    protected:
        /** Singleton write cache client instance */
        static Client *_instance;

        /** Mutex protecting _instance in get_instance() */
        static std::mutex _instance_mutex;

        /**
         * @brief Construct a new Write Cache Client object
         */
        Client();

        /**
         * @brief Destroy the Write Cache Client object; shouldn't be called directly use shutdown()
         */
        ~Client() {}

    private:
        // delete copy constructor
        Client(const Client &) = delete;
        void operator=(const Client &)   = delete;

        // the following is for handling cached thrift clients from the object pool
        // we wrap the client in a struct whose deallocator will release it back to the pool

        /** Thrift client object pool */
        std::shared_ptr<ObjectPool<ServiceClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<ServiceClient>> pool;
            std::shared_ptr<ServiceClient> client;

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
            std::shared_ptr<ServiceClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }

    };

} // namespace springtail
