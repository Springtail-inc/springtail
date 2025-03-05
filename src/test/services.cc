#include <test/services.hh>

#include <common/json.hh>
#include <common/properties.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/server.hh>
#include <write_cache/write_cache_client.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::test {

    class XidMgrTestRunner : public ServiceRunner {
    public:
        XidMgrTestRunner() :
            ServiceRunner("XidMgrServer") {}

        bool start() override {
            std::filesystem::path xid_dir;

            auto json = Properties::get(Properties::XID_MGR_CONFIG);
            if (json.is_null() || !json.contains("rpc_config")) {
                throw Error("XID Mgr config incorrect");
            }
            /*
            if (json.is_null() || !json.contains("server")) {
                throw Error("XID Mgr config incorrect");
            }
            json = json["server"];
            */
            Json::get_to<std::filesystem::path>(json, "base_path", xid_dir);
            xid_dir = Properties::make_absolute_path(xid_dir);
            std::filesystem::remove_all(xid_dir);
            xid_mgr::XidMgrServer::get_instance()->startup();
            return true;
        }

        void stop() override {
            XidMgrClient::shutdown();
            xid_mgr::XidMgrServer::shutdown();
        }
    };

    class SysTblMgrTestRunner : public ServiceRunner {
    public:
        SysTblMgrTestRunner() :
            ServiceRunner("SysTblMgrServer") {}

        bool start() override {
            std::filesystem::path table_dir;

            auto json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", table_dir);
            table_dir = Properties::make_absolute_path(table_dir);
            std::filesystem::remove_all(table_dir);
            sys_tbl_mgr::Server::get_instance()->startup();
            return true;
        }

        void stop() override {
            sys_tbl_mgr::Client::shutdown();
            sys_tbl_mgr::Server::shutdown();
        }
    };

    class WriteCacheTestRunner : public WriteCacheRunner {
    public:
        WriteCacheTestRunner() : WriteCacheRunner() {}

        void stop() override {
            WriteCacheClient::shutdown();
            WriteCacheRunner::stop();
        }
    };

    std::vector<std::unique_ptr<ServiceRunner>>
    get_services(bool xid_mgr,
                bool sys_tbl_mgr,
                bool write_cache) {
        if (sys_tbl_mgr || write_cache) {
            xid_mgr = true;
        }
        std::vector<std::unique_ptr<ServiceRunner>> services;
        if (xid_mgr) {
            services.emplace_back(std::make_unique<XidMgrTestRunner>());
        }
        if (sys_tbl_mgr) {
            services.emplace_back(std::make_unique<SysTblMgrTestRunner>());
        }
        if (write_cache) {
            services.emplace_back(std::make_unique<WriteCacheTestRunner>());
        }
        return services;
    }
}
