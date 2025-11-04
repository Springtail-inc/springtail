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

    void
    start_xid_mgr()
    {
        std::filesystem::path xid_dir;

        auto json = Properties::get(Properties::LOG_MGR_CONFIG);
        if (json.is_null() || !json.contains("rpc_config")) {
            throw Error("LOG Mgr config incorrect");
        }

        Json::get_to<std::filesystem::path>(json, "transaction_log_path", xid_dir);
        xid_dir = Properties::make_absolute_path(xid_dir);
        std::filesystem::remove_all(xid_dir);
        xid_mgr::XidMgrServer::get_instance()->cleanup_on_shutdown();
    }

    void
    start_sys_tbl_mgr()
    {
        std::filesystem::path table_dir;

        auto json = Properties::get(Properties::STORAGE_CONFIG);
        Json::get_to<std::filesystem::path>(json, "table_dir", table_dir);
        table_dir = Properties::make_absolute_path(table_dir);
        std::filesystem::remove_all(table_dir);
        sys_tbl_mgr::Server::get_instance();
    }

    void
    start_services(bool xid_mgr, bool sys_tbl_mgr, bool write_cache)
    {
        if (sys_tbl_mgr) {
            xid_mgr = true;
        }

        if (xid_mgr) {
            start_xid_mgr();
        }

        if (sys_tbl_mgr) {
            start_sys_tbl_mgr();
        }

        if (write_cache) {
            WriteCacheServer::get_instance();
        }
    }
}
