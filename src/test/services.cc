#include <common/json.hh>
#include <common/properties.hh>
#include <sys_tbl_mgr/client.hh>
#include <sys_tbl_mgr/server.hh>
#include <test/services.hh>
#include <write_cache/write_cache_client.hh>
#include <write_cache/write_cache_server.hh>
#include <xid_mgr/xid_mgr_client.hh>
#include <xid_mgr/xid_mgr_server.hh>

namespace springtail::test {
    Services::Services(bool xid_mgr,
                       bool sys_tbl_mgr,
                       bool write_cache)
        : _xid_mgr(xid_mgr),
          _sys_tbl_mgr(sys_tbl_mgr),
          _write_cache(write_cache)
    { }

    void
    Services::init()
    {
        // the XID mgr is needed for other services
        if (_sys_tbl_mgr || _write_cache) {
            _xid_mgr = true;
        }

        // start the XID mgr
        if (_xid_mgr) {
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

            _threads.push_back(std::thread([] {
                xid_mgr::XidMgrServer::get_instance()->startup();
            }));
        }

        // start the SysTbl mgr
        if (_sys_tbl_mgr) {
            std::filesystem::path table_dir;

            auto json = Properties::get(Properties::STORAGE_CONFIG);
            Json::get_to<std::filesystem::path>(json, "table_dir", table_dir);
            table_dir = Properties::make_absolute_path(table_dir);
            std::filesystem::remove_all(table_dir);

            _threads.push_back(std::thread([] {
                sys_tbl_mgr::Server::get_instance()->startup();
            }));
        }

        // start the Write Cache
        if (_write_cache) {
            _threads.push_back(std::thread([] {
                WriteCacheServer::get_instance()->startup();
            }));
        }
        // give everyting a chance to startup
        sleep(1);
    }

    void
    Services::shutdown()
    {
        // shut down the write_cache
        if (_write_cache) {
            WriteCacheClient::shutdown();
            WriteCacheServer::get_instance()->stop();
            _threads.back().join();
            _threads.pop_back();
            WriteCacheServer::shutdown();
        }

        // shut down the sys_tbl_mgr
        if (_sys_tbl_mgr) {
            sys_tbl_mgr::Client::shutdown();
            sys_tbl_mgr::Server::get_instance()->stop();
            _threads.back().join();
            _threads.pop_back();
            sys_tbl_mgr::Server::shutdown();
        }

        // shut down the xid_mgr
        if (_xid_mgr) {
            XidMgrClient::shutdown();
            xid_mgr::XidMgrServer::get_instance()->stop();
            _threads.back().join();
            _threads.pop_back();
            xid_mgr::XidMgrServer::shutdown();
        }
    }
}
