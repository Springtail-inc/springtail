#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include <thrift/sys_tbl_mgr/Service.h>
#include <thrift/common/thrift_server.hh>

#include <common/singleton.hh>

#include <sys_tbl_mgr/service.hh>

namespace springtail::sys_tbl_mgr {

    class Server final:
        public springtail::thrift::Server<Server,
                                            ServiceProcessorFactory,
                                            Service,
                                            ServiceIfFactory,
                                            ServiceIf>,
        public Singleton<Server>
    {
        friend class Singleton<Server>;
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

    };

} // namespace springtail
