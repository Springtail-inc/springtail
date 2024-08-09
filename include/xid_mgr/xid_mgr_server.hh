#pragma once

#include <mutex>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>
#include <filesystem>

#include <thrift/server/TServer.h>

namespace springtail {

    /**
     * @class XidMgrServer
     * @brief This class represents a server for managing transaction IDs (XIDs).
     *        It provides functionality to allocate XID ranges, commit XIDs, and retrieve the latest committed XID.
     */
    class XidMgrServer
    {
    public:
        static constexpr char const XID_MGR_COMMIT_FILE[] = "xid_mgr_commit";

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
         * @brief Shutdown XID manager
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &XidMgrServer::_shutdown);
        }

        /**
         * @brief Startup server; does not return
         */
        static void startup() {
            // start the server
            auto server = get_instance();
            server->_startup();

            // after shutdown() we delete the instance
            delete _instance;
        }


        // interfaces from thrift

        /**
         * @brief commit up to and including given xid
         * @param xid
         */
        void commit_xid(uint64_t xid, bool has_schema_changes);

        /**
         * @brief Get the latest committed xid object
         * @return uint64_t
         */
        uint64_t get_committed_xid(uint64_t schema_xid);

    private:
        /**
         * @brief Construct a new XidMgr object
         */
        XidMgrServer();

        /**
         * @brief Destroy the XidMgr object; shouldn't be called directly use shutdown()
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

        /** last committed xid */
        uint64_t _committed_xid = 0;

        /** history of schema xids */
        std::vector<uint64_t> _history;

        /** base path */
        std::filesystem::path _base_path;

        /** file descriptor */
        int _fd;

        /** mutex for reading/writing xid */
        std::shared_mutex _mutex;

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** startup from startup(), called once */
        void _startup();

        /**
         * Write committed xid to file (if larger than last value)
         * @param xid new value for committed xid
         */
        void _write_committed_xid(uint64_t xid);

        /**
         * Read committed xid from file into _committed_xid
         * @return uint64_t return the committed xid
         */
        uint64_t _read_committed_xid();
    };

} // namespace springtail
