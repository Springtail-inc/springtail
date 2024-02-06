#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <common/object_pool.hh>

#include <ThriftXidMgr.h> // generated file

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
         * @brief Get newly allocated xid range
         * @return std::pair<uint64_t, uint64_t> first=start of range, second=end of range inclusive
         */
        std::pair<uint64_t, uint64_t> get_xid_range();

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
        std::shared_ptr<ObjectPool<thrift::ThriftXidMgrClient>> _thrift_client_pool;

        /** Struct to wrap the client pool and client object to ensure it gets release back */
        struct ThriftClient {
            std::shared_ptr<ObjectPool<thrift::ThriftXidMgrClient>> pool;
            std::shared_ptr<thrift::ThriftXidMgrClient> client;
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
            std::shared_ptr<thrift::ThriftXidMgrClient> client = _thrift_client_pool->get();
            ThriftClient c = { _thrift_client_pool, client };
            return c;
        }
    };
} // namespace springtail