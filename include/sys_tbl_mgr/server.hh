#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <thrift/server/TServer.h>
#include <thrift/concurrency/ThreadManager.h>

#include <common/singleton.hh>

namespace springtail::sys_tbl_mgr {

    class Server : public Singleton<Server>
    {
        friend class Singleton<Server>;
    public:
        /**
         * @brief Startup server; does not return
         */
        static void startup() {
            // start the server
            auto server = get_instance();
            server->_startup();
        }

        void stop() {
            _server->stop();
            _thread_manager->stop();
        }

    private:
        /**
         * @brief Construct a new Write Cache Server object
         */
        Server();

        /**
         * @brief Destroy the Write Cache Server object; shouldn't be called directly use shutdown()
         */
         ~Server() override = default;

        /** shutdown from shutdown(), called once */
        void _internal_shutdown();

        /** startup from startup(), called once */
        void _startup();

        /** The thrift server. */
        std::shared_ptr<apache::thrift::server::TServer> _server;

        /** thread manager that is used by the server */
        std::shared_ptr<apache::thrift::concurrency::ThreadManager> _thread_manager = {nullptr};

        /** number of worker threads */
        int _worker_thread_count;
        /** server port */
        int _port;
    };

} // namespace springtail
