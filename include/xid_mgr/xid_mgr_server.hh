#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

namespace springtail {

    class XidMgrServer
    {
    public:

        // delete copy constructor
        XidMgrServer(const XidMgrServer &)   = delete;
        void operator=(const XidMgrServer &) = delete;

        /**
         * @brief Get the singleton write cache server instance object
         * @return XidMgrServer *
         */
        static XidMgrServer *get_instance() {
            std::call_once(_init_flag, &XidMgrServer::_init);
            return _instance;
        }
        /**
         * @brief Shutdown cache
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &XidMgrServer::_shutdown);
        }

        /**
         * @brief Startup server; does not return
         */
        void startup();


        // interfaces from thrift

        /**
         * @brief Get an unallocated xid range
         * @param last_xid last allocated xid, 0 for none
         * @return std::pair<uint64_t, uint64_t> start/end offset inclusive
         */
        std::pair<uint64_t, uint64_t> get_xid_range(uint64_t last_xid);

        /**
         * @brief commit up to and including given xid
         * @param xid
         */
        void commit_xid(uint64_t xid);

        /**
         * @brief Get the latest committed xid object
         * @return uint64_t
         */
        uint64_t get_committed_xid();

    private:
        /**
         * @brief Construct a new Write Cache Server object
         */
        XidMgrServer();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~XidMgrServer() {}

        /** init from get_instance, called once */
        static XidMgrServer *_init();

        /** shutdown from shutdown(), called once */
        static void _shutdown();

        /** Singleton write cache server instance */
        static XidMgrServer *_instance;

        /** init flag */
        static std::once_flag _init_flag;
        /** shutdown flag */
        static std::once_flag _shutdown_flag;

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;
    };

} // namespace springtail