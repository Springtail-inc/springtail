#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <thrift/server/TServer.h>

namespace springtail::sys_tbl_mgr {

    class Server
    {
    public:
        /**
         * @brief Get the singleton write cache server instance object
         * @return SysTblMgrServer *
         */
        static Server *get_instance() {
            std::call_once(_init_flag, &Server::_init);
            return _instance;
        }
        /**
         * @brief Shutdown cache
         */
        static void shutdown() {
            std::call_once(_shutdown_flag, &Server::_shutdown);
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

        // delete copy constructor
        Server(const Server &) = delete;
        void operator=(const Server &)   = delete;

    private:
        /**
         * @brief Construct a new Write Cache Server object
         */
        Server();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~Server() {}

        /** init from get_instance, called once */
        static Server *_init();

        /** shutdown from shutdown(), called once */
        static void _shutdown();

        /** startup from startup(), called once */
        void _startup();

        /** Singleton write cache server instance */
        static Server *_instance;

        /** init flag */
        static std::once_flag _init_flag;
        /** shutdown flag */
        static std::once_flag _shutdown_flag;

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;
    };

} // namespace springtail
